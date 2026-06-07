package driver

import (
	"context"
	"log/slog"
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
	prep    time.Duration // Δ_prep before emitting on block receipt

	runCtx context.Context // set at Run; used by emit/publish
	mu     sync.Mutex
	slots  map[int]*slotState
}

// slotState is the per-(node, slot) coupling holder.
type slotState struct {
	deadline   time.Time
	duties     []committee.AttestDuty
	aggSubnets []int
	timer      *time.Timer
	emitOnce   sync.Once

	seen       bool
	seenAt     time.Time
	seenOrigin int
}

// NewRunner builds a runner for one node. comm may be nil (block-only).
func NewRunner(num int, nd *node.Node, val *validator.Validator, comm *committee.Assignment,
	tracer metrics.Tracer, slotDur, due, prep time.Duration) *NodeRunner {
	return &NodeRunner{
		num: num, nd: nd, val: val, comm: comm, tracer: tracer,
		slotDur: slotDur, due: due, prep: prep, slots: make(map[int]*slotState),
	}
}

// Attach wires the runner as the node's receive sink. Call before JoinTopics.
func (r *NodeRunner) Attach() { r.nd.OnReceive = r.onReceive }

// Run executes numSlots slots from runStart: subscribe the backbone (whole-run), then
// per slot publish block duties, arm the attestation deadline, and clean up at slot end.
func (r *NodeRunner) Run(ctx context.Context, runStart time.Time, numSlots int) {
	r.runCtx = ctx
	if r.comm != nil {
		for _, s := range r.comm.Node(r.num).Backbone() {
			if err := r.nd.Subscribe(validator.AttestationTopic(s)); err != nil {
				slog.Error("subscribe backbone failed", "node", r.num, "subnet", s, "err", err)
			}
		}
	}
	for slot := range numSlots {
		slotStart := runStart.Add(time.Duration(slot) * r.slotDur)
		ss := r.beginSlot(slot, slotStart)
		time.Sleep(time.Until(slotStart.Add(r.slotDur)))
		r.endSlot(slot, ss)
	}
}

// beginSlot publishes this node's block duties and, if it has a committee, caches its
// attest duties, subscribes this slot's aggregator subnets, and arms the deadline timer.
func (r *NodeRunner) beginSlot(slot int, slotStart time.Time) *slotState {
	for _, duty := range r.val.Duties(slot) {
		go r.publishBlock(slotStart.Add(duty.At), duty.Msg)
	}
	if r.comm == nil {
		return nil
	}
	view := r.comm.Node(r.num)
	ss := &slotState{
		deadline:   slotStart.Add(r.due),
		duties:     view.AttestDuties(slot),
		aggSubnets: view.AggregatorSubnets(slot),
	}
	for _, s := range ss.aggSubnets {
		if err := r.nd.Subscribe(validator.AttestationTopic(s)); err != nil {
			slog.Error("subscribe aggregator failed", "node", r.num, "subnet", s, "err", err)
		}
	}
	r.mu.Lock()
	r.slots[slot] = ss
	r.mu.Unlock()
	ss.timer = time.AfterFunc(time.Until(ss.deadline), func() { r.onDeadline(slot, ss) })
	return ss
}

// endSlot stops the deadline timer, leaves this slot's aggregator subnets, and prunes
// the slot state so nothing leaks across a multi-slot run.
func (r *NodeRunner) endSlot(slot int, ss *slotState) {
	if ss == nil {
		return
	}
	ss.timer.Stop()
	for _, s := range ss.aggSubnets {
		r.nd.Unsubscribe(validator.AttestationTopic(s))
	}
	r.mu.Lock()
	delete(r.slots, slot)
	r.mu.Unlock()
}

// publishBlock waits until when, records the publish, then publishes the block.
func (r *NodeRunner) publishBlock(when time.Time, msg validator.Message) {
	time.Sleep(time.Until(when))
	r.tracer.OnPublish(metrics.BlockID(msg.Slot, r.num), false, time.Now())
	if err := r.nd.Publish(r.runCtx, msg.Topic, msg.Payload); err != nil {
		slog.Error("publish block failed", "node", r.num, "slot", msg.Slot, "err", err)
	}
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
	case node.KindAttestation:
		att := rec.Obj.(*pb.Attestation)
		if int(att.Origin) == r.num {
			return
		}
		r.tracer.OnReceive(r.num, metrics.AttestID(int(att.Slot), int(att.Subnet), int(att.Val), int(att.Origin)), rec.At)
	}
}

// onDeadline emits this slot's attestations if not already emitted. With no block seen
// it votes prior head; the coupling (block-driven early emit) is layered on in M5.
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
