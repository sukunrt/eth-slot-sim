package driver

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// NodeRunner is one node's stateful per-slot orchestrator. It owns the slot loop AND
// is the node's OnReceive sink, so it can couple block arrival to attestation emit
// while keeping Node and Validator pure. The multi-node Driver builds N; the Shadow
// binary builds 1.
type NodeRunner struct {
	num     int
	nd      *node.Node
	val     *validator.Validator
	comm    *committee.Assignment // nil ⇒ block-only (Phase 1)
	tracer  metrics.Tracer
	slotDur time.Duration
	due     time.Duration // attestation deadline, offset into the slot
	aggDue  time.Duration // aggregate emit, offset into the slot (0 ⇒ no aggregates)
	prep    time.Duration // Δ_prep before emitting on block receipt
	seed    uint64        // seeds the per-slot subnet-peer dial choice
	base    map[int]bool  // the node's long-lived peers (so we know which dials are extra)

	runCtx context.Context // set at Run; used by emit/publish
	mu     sync.Mutex
	slots  map[int]*slotState
}

// slotState is the per-(node, slot) coupling holder.
type slotState struct {
	deadline time.Time
	duties   []committee.AttestDuty
	dialed   []int // extra peers dialed this slot, dropped at slot end
	timer    *time.Timer
	emitOnce sync.Once

	seen       bool
	seenAt     time.Time
	seenOrigin int

	aggSubnets  []int // subnets this node aggregates this slot (publishes M aggregates on each)
	aggTimer    *time.Timer
	aggEmitOnce sync.Once
}

// NewRunner builds a runner for one node. comm may be nil (block-only). aggDue 0 disables
// the aggregate phase. basePeers is the node's long-lived peer set (so per-slot subnet dials
// it adds on top can be dropped).
func NewRunner(num int, nd *node.Node, val *validator.Validator, comm *committee.Assignment,
	tracer metrics.Tracer, slotDur, due, aggDue, prep time.Duration, seed uint64, basePeers []int) *NodeRunner {
	base := make(map[int]bool, len(basePeers))
	for _, p := range basePeers {
		base[p] = true
	}
	return &NodeRunner{
		num: num, nd: nd, val: val, comm: comm, tracer: tracer,
		slotDur: slotDur, due: due, aggDue: aggDue, prep: prep, seed: seed, base: base,
		slots: make(map[int]*slotState),
	}
}

// Attach wires the runner as the node's receive sink. Call before JoinTopics.
func (r *NodeRunner) Attach() { r.nd.OnReceive = r.onReceive }

// Prepare subscribes the node's own subnets (its long-lived meshes). Call during bring-up,
// before the settle, so the meshes form before slot 0.
func (r *NodeRunner) Prepare() {
	if r.comm == nil {
		return
	}
	if r.aggDue > 0 { // every node joins the global aggregate mesh (it downloads all aggregates)
		if err := r.nd.Subscribe(validator.AggregateTopic); err != nil {
			slog.Error("subscribe aggregate topic failed", "node", r.num, "err", err)
		}
	}
	for _, s := range r.comm.Node(r.num).SubscribedSubnets() {
		if err := r.nd.Subscribe(validator.AttestationTopic(s)); err != nil {
			slog.Error("subscribe subnet failed", "node", r.num, "subnet", s, "err", err)
		}
	}
}

// Run executes numSlots slots from runStart: per slot publish block duties, dial this
// slot's subnet peers, arm the attestation deadline, and clean up at slot end.
func (r *NodeRunner) Run(ctx context.Context, runStart time.Time, numSlots int) {
	r.runCtx = ctx
	for slot := range numSlots {
		slotStart := runStart.Add(time.Duration(slot) * r.slotDur)
		ss := r.beginSlot(slot, slotStart)
		time.Sleep(time.Until(slotStart.Add(r.slotDur)))
		r.endSlot(slot, ss)
	}
}

// beginSlot dials 2 subscribers of each subnet this node attests but doesn't subscribe
// (so a fan-out publish has somewhere to land), arms the deadline timer, then publishes
// the block — all before the block publish, so a proposer that also attests can self-vote.
func (r *NodeRunner) beginSlot(slot int, slotStart time.Time) *slotState {
	var ss *slotState
	if r.comm != nil {
		view := r.comm.Node(r.num)
		ss = &slotState{deadline: slotStart.Add(r.due), duties: view.AttestDuties(slot)}

		subscribed := map[int]bool{}
		for _, s := range view.SubscribedSubnets() {
			subscribed[s] = true
		}
		needDial := map[int]bool{}
		for _, d := range ss.duties {
			if !subscribed[d.Subnet] {
				needDial[d.Subnet] = true
			}
		}
		picked := map[int]bool{}
		for subnet := range needDial {
			if err := r.nd.Join(validator.AttestationTopic(subnet)); err != nil {
				slog.Error("join duty subnet failed", "node", r.num, "subnet", subnet, "err", err)
			}
			n := 0
			for _, peer := range r.shuffledSubscribers(slot, subnet) {
				if n >= 2 {
					break
				}
				if peer == r.num || r.base[peer] || picked[peer] {
					continue
				}
				picked[peer] = true
				ss.dialed = append(ss.dialed, peer)
				n++
			}
		}
		r.nd.Dial(ss.dialed) // synchronous: connections are up before the slot proceeds

		r.mu.Lock()
		r.slots[slot] = ss
		r.mu.Unlock()
		ss.timer = time.AfterFunc(time.Until(ss.deadline), func() { r.onDeadline(slot, ss) })

		// Aggregate phase: if this node aggregates any committee this slot, arm a timer to
		// publish its M aggregates at the aggregate deadline (fixed offset, no coupling).
		if r.aggDue > 0 {
			ss.aggSubnets = view.AggregateSubnets(slot)
			if len(ss.aggSubnets) > 0 {
				ss.aggTimer = time.AfterFunc(time.Until(slotStart.Add(r.aggDue)), func() {
					r.emitAggregate(slot, ss)
				})
			}
		}
	}
	for _, duty := range r.val.Duties(slot) {
		go r.publishBlock(slotStart.Add(duty.At), duty.Msg)
	}
	return ss
}

// shuffledSubscribers returns subnet's subscribers in a seeded order (so the 2 dialed are
// reproducible across runs/backends), keyed by (seed, slot, subnet).
func (r *NodeRunner) shuffledSubscribers(slot, subnet int) []int {
	subs := r.comm.Subscribers(subnet)
	order := make([]int, len(subs))
	copy(order, subs)
	rng := rand.New(rand.NewPCG(r.seed, uint64(slot)*1_000_003+uint64(subnet)))
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}

// endSlot stops the deadline timer, disconnects this slot's extra dials, and prunes the
// slot state so connections and state don't leak across a multi-slot run.
func (r *NodeRunner) endSlot(slot int, ss *slotState) {
	if ss == nil {
		return
	}
	ss.timer.Stop()
	if ss.aggTimer != nil {
		ss.aggTimer.Stop()
	}
	r.nd.Disconnect(ss.dialed)
	r.mu.Lock()
	delete(r.slots, slot)
	r.mu.Unlock()
}

// publishBlock waits until when, records the publish, then publishes the block. A
// proposer that also attests this slot votes for its own block (block_processed = now);
// loopback is not routed through the tracer, so this is the only place self-block-seen
// is set.
func (r *NodeRunner) publishBlock(when time.Time, msg validator.Message) {
	time.Sleep(time.Until(when))
	now := time.Now()
	r.tracer.OnPublish(metrics.BlockID(msg.Slot, r.num), false, now)
	if err := r.nd.Publish(r.runCtx, msg.Topic, msg.Payload); err != nil {
		slog.Error("publish block failed", "node", r.num, "slot", msg.Slot, "err", err)
	}
	r.onBlockProcessed(msg.Slot, r.num, now)
}

// onReceive routes a decoded receipt: record block/attestation arrivals (skipping the
// node's own loopback), and feed the block into the coupling.
func (r *NodeRunner) onReceive(rec node.Received) {
	switch rec.Kind {
	case node.KindBlock:
		blk := rec.Obj.(*pb.Block)
		if int(blk.Origin) == r.num {
			return
		}
		r.tracer.OnReceive(r.num, metrics.BlockID(int(blk.Slot), int(blk.Origin)), rec.At)
		r.onBlockProcessed(int(blk.Slot), int(blk.Origin), rec.At)
	case node.KindAttestation:
		att := rec.Obj.(*pb.Attestation)
		if int(att.Origin) == r.num {
			return
		}
		r.tracer.OnReceive(r.num, metrics.AttestID(int(att.Slot), int(att.Subnet), int(att.Val), int(att.Origin)), rec.At)
	case node.KindAggregate:
		agg := rec.Obj.(*pb.Aggregate)
		if int(agg.Origin) == r.num { // skip our own published aggregate (loopback)
			return
		}
		r.tracer.OnReceive(r.num, metrics.AggregateID(int(agg.Slot), int(agg.Subnet), int(agg.Origin)), rec.At)
	}
}

// onBlockProcessed is the causal edge: the slot's block was processed at `at`. It
// records block-seen once and, if processed (plus Δ_prep) by the deadline, emits this
// node's attestations early, voting for the block. A late block is left to the deadline
// timer (which votes prior). Both paths run emitDecision, so the vote is the same no
// matter which fires first; emitOnce guarantees a single emission.
func (r *NodeRunner) onBlockProcessed(slot, origin int, at time.Time) {
	r.mu.Lock()
	ss, ok := r.slots[slot]
	if !ok { // no attest duties this slot (or already pruned)
		r.mu.Unlock()
		return
	}
	if !ss.seen {
		ss.seen, ss.seenAt, ss.seenOrigin = true, at, origin
	}
	seenAt, seenOrigin, deadline := ss.seenAt, ss.seenOrigin, ss.deadline
	r.mu.Unlock()

	emitAt, voteBlock := emitDecision(true, seenAt, deadline, r.prep)
	if !voteBlock {
		return
	}
	if d := time.Until(emitAt); d > 0 { // honor Δ_prep before emitting
		time.AfterFunc(d, func() { r.emit(slot, ss, seenOrigin) })
	} else {
		r.emit(slot, ss, seenOrigin)
	}
}

// onDeadline emits this slot's attestations if not already emitted, re-deriving the
// vote from block-seen state (so a block recorded exactly at the deadline still votes
// block); otherwise it votes prior head.
func (r *NodeRunner) onDeadline(slot int, ss *slotState) {
	r.mu.Lock()
	seen, seenAt, origin := ss.seen, ss.seenAt, ss.seenOrigin
	r.mu.Unlock()
	_, voteBlock := emitDecision(seen, seenAt, ss.deadline, r.prep)
	votedOrigin := -1
	if voteBlock {
		votedOrigin = origin
	}
	r.emit(slot, ss, votedOrigin)
}

// emit publishes one attestation per duty, at most once per slot (the emit-once guard).
// votedOrigin is the voted block's origin (>=0) or -1 for the prior head.
func (r *NodeRunner) emit(slot int, ss *slotState, votedOrigin int) {
	ss.emitOnce.Do(func() {
		at := time.Now()
		votedBlock := votedOrigin >= 0
		for _, d := range ss.duties {
			topic := validator.AttestationTopic(d.Subnet)
			if err := r.nd.Join(topic); err != nil {
				slog.Error("join subnet failed", "node", r.num, "subnet", d.Subnet, "err", err)
				continue
			}
			msg := validator.MakeAttestation(slot, d.Subnet, d.Val, r.num, votedOrigin)
			r.tracer.OnPublish(metrics.AttestID(slot, d.Subnet, d.Val, r.num), votedBlock, at)
			if err := r.nd.Publish(r.runCtx, topic, msg.Payload); err != nil {
				slog.Error("publish attestation failed", "node", r.num, "slot", slot, "subnet", d.Subnet, "err", err)
			}
		}
	})
}

// emitAggregate publishes one aggregate for each committee this node aggregates this slot, on
// the global aggregate topic, at most once per slot. The aggregate carries this node as its
// origin (standing in for the aggregator's signature), so each aggregator's aggregate is
// distinct and gossipsub does not dedup them.
func (r *NodeRunner) emitAggregate(slot int, ss *slotState) {
	ss.aggEmitOnce.Do(func() {
		at := time.Now()
		for _, subnet := range ss.aggSubnets {
			msg := validator.MakeAggregate(slot, subnet, r.num)
			r.tracer.OnPublish(metrics.AggregateID(slot, subnet, r.num), false, at)
			if err := r.nd.Publish(r.runCtx, msg.Topic, msg.Payload); err != nil {
				slog.Error("publish aggregate failed", "node", r.num, "slot", slot, "subnet", subnet, "err", err)
			}
		}
	})
}
