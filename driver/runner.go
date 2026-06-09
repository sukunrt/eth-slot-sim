package driver

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/schedule"
	"github.com/ethp2p/slot-sim/validator"
)

// DecoupledParams turns on the decoupled-consensus phase (nil ⇒ off), mirroring how a nil
// schedule means block-only. When set, the runner emits AC votes (column-gated, on one global
// topic) in place of attestations, and — every K AC slots — finality votes + a population-scaled
// aggregate; attestation/sync emit is suppressed. The AC-vote deadline and Δ_prep reuse the
// existing due/prep. K/FCVoteOffset/FCAggFraction drive the finality chain (used by M4/M5).
type DecoupledParams struct {
	K             int           // ac_slots_per_finality_slot (a finality slot spans K AC slots)
	FCVoteOffset  time.Duration // offset into the finality slot for the per-validator FC vote burst
	FCAggFraction int           // finality_slot_aggregation_fraction (percent, 0 < f < 100)
}

// NodeRunner is one node's stateful per-slot orchestrator. It owns the slot loop AND
// is the node's OnReceive sink, so it can couple block arrival to attestation emit
// while keeping Node and Validator pure. The multi-node Driver builds N; the Shadow
// binary builds 1.
type NodeRunner struct {
	num       int
	nd        *node.Node
	val       *validator.Validator
	sched     *schedule.Assignment // nil ⇒ block-only (Phase 1)
	attest    bool                 // emit attestations (with sched set but false ⇒ columns-only)
	sync      bool                 // emit sync-committee messages + contributions (members only)
	decoupled bool                 // emit AC votes + finality votes/aggregates (replaces attest/sync)
	tracer    metrics.Tracer
	slotDur   time.Duration
	due       time.Duration // attestation / AC-vote deadline, offset into the slot
	aggDue    time.Duration // aggregate emit, offset into the slot (0 ⇒ no aggregates)
	prep      time.Duration // Δ_prep before emitting on block receipt
	seed      uint64        // seeds the per-slot subnet-peer dial choice
	base      map[int]bool  // the node's long-lived peers (so we know which dials are extra)

	// Decoupled finality-chain knobs (set when decoupled; consumed by the FC paths, M4/M5).
	k             int           // ac_slots_per_finality_slot
	fcVoteOffset  time.Duration // offset into the finality slot for the FC vote burst
	fcAggFraction int           // % of the finality slot when FC aggregates publish

	runCtx context.Context // set at Run; used by emit/publish
	mu     sync.Mutex
	slots  map[int]*slotState
	finals map[int]*finalityState // finality-slot-keyed state (decoupled; spans k AC slots)
}

// finalityState is the per-(node, finality-slot) state. Unlike slotState (per AC slot), it spans k
// AC slots — armed at a finality boundary, drained by its one-shot vote timer (and, M5, its
// aggregate timer), and reaped at the finality slot's last AC slot. It is keyed in r.finals by the
// finality slot index, so the per-AC-slot endSlot pruning of r.slots leaves it untouched.
type finalityState struct {
	subnet    int         // this node's stable finality subnet
	duties    []int       // validators this node hosts (one FinalityVote each)
	voteTimer *time.Timer // fires at finalitySlotStart(n) + fcVoteOffset
	voteOnce  sync.Once   // one FC vote burst per finality slot
}

// slotState is the per-(node, slot) coupling holder.
type slotState struct {
	deadline time.Time
	duties   []schedule.AttestDuty
	dialed   []int // extra peers dialed this slot, dropped at slot end
	timer    *time.Timer
	emitOnce sync.Once

	seen       bool
	seenAt     time.Time
	seenOrigin int

	// Sync-committee membership this slot (set at beginSlot from view.SyncSubnet). A member
	// emits one message on syncSubnet at min(block_seen, deadline); a non-member emits none.
	syncSubnet   int
	syncMember   bool
	syncEmitOnce sync.Once

	// Column custody gate (active when custody is non-empty). columnsComplete starts true
	// when there's nothing to wait for (columns off ⇒ empty custody), so the gate is a no-op.
	custody           []int        // this node's custody columns this slot
	haveColumn        map[int]bool // custody columns received (post-verify); nil when none
	columnsComplete   bool         // all custody columns in (or none to wait for)
	columnsCompleteAt time.Time    // when custody completed (the gate's emit input)

	aggSubnets  []int // subnets this node aggregates this slot (publishes M aggregates on each)
	aggTimer    *time.Timer
	aggEmitOnce sync.Once

	// Sync contribution phase (fixed deadline, no coupling). syncAggSubnets = the sync subnets
	// this node aggregates this slot; it publishes one contribution on each (global topic).
	syncAggSubnets  []int
	syncAggTimer    *time.Timer
	syncAggEmitOnce sync.Once
}

// NewRunner builds a runner for one node. sched may be nil (block-only). attest gates
// attestation emission: sched set with attest false is a columns-only run (disseminate +
// measure columns, no vote). sync gates sync-committee emission (messages + contributions), for
// member nodes only; it arms the shared deadline timer alongside attest. aggDue 0 disables the
// aggregate phase. dc (nil ⇒ off) turns on the decoupled-consensus phase, which suppresses
// attestation/sync emit (the AC vote replaces attestations). basePeers is the node's long-lived
// peer set (so per-slot subnet dials it adds on top can be dropped).
func NewRunner(num int, nd *node.Node, val *validator.Validator, sched *schedule.Assignment, attest, sync bool,
	tracer metrics.Tracer, slotDur, due, aggDue, prep time.Duration, seed uint64, basePeers []int,
	dc *DecoupledParams) *NodeRunner {
	base := make(map[int]bool, len(basePeers))
	for _, p := range basePeers {
		base[p] = true
	}
	r := &NodeRunner{
		num: num, nd: nd, val: val, sched: sched, attest: attest, sync: sync, tracer: tracer,
		slotDur: slotDur, due: due, aggDue: aggDue, prep: prep, seed: seed, base: base,
		slots: make(map[int]*slotState),
	}
	if dc != nil {
		r.decoupled = true
		r.k, r.fcVoteOffset, r.fcAggFraction = dc.K, dc.FCVoteOffset, dc.FCAggFraction
		r.finals = make(map[int]*finalityState)
	}
	return r
}

// Attach wires the runner as the node's receive sink. Call before JoinTopics.
func (r *NodeRunner) Attach() { r.nd.OnReceive = r.onReceive }

// Prepare subscribes the node's own subnets (its long-lived meshes). Call during bring-up,
// before the settle, so the meshes form before slot 0.
func (r *NodeRunner) Prepare() {
	if r.sched == nil {
		return
	}
	if r.attest {
		if r.aggDue > 0 { // every node joins the global aggregate mesh (it downloads all aggregates)
			if err := r.nd.Subscribe(validator.AggregateTopic); err != nil {
				slog.Error("subscribe aggregate topic failed", "node", r.num, "err", err)
			}
		}
		for _, s := range r.sched.Node(r.num).SubscribedSubnets() {
			if err := r.nd.Subscribe(validator.AttestationTopic(s)); err != nil {
				slog.Error("subscribe subnet failed", "node", r.num, "subnet", s, "err", err)
			}
		}
	}
	if r.sched.NumColumns > 0 { // the node's custody column meshes (the DA dissemination phase)
		for _, c := range r.sched.Node(r.num).CustodyColumns() {
			if err := r.nd.Subscribe(validator.ColumnTopic(c)); err != nil {
				slog.Error("subscribe column failed", "node", r.num, "column", c, "err", err)
			}
		}
	}
	if r.sync {
		// Every node downloads all contributions (the global topic, like aggregates); a member
		// also meshes its one sync subnet (no backbone, no per-slot dial — see sync-committee-spec).
		if err := r.nd.Subscribe(validator.SyncContributionTopic); err != nil {
			slog.Error("subscribe sync contribution topic failed", "node", r.num, "err", err)
		}
		if subnet, member := r.sched.Node(r.num).SyncSubnet(); member {
			if err := r.nd.Subscribe(validator.SyncMessageTopic(subnet)); err != nil {
				slog.Error("subscribe sync subnet failed", "node", r.num, "subnet", subnet, "err", err)
			}
		}
	}
	if r.decoupled {
		// Every node downloads all AC votes and FC aggregates (global topics, like aggregates), and
		// persistently meshes its one finality subnet (it votes there every finality slot). Columns
		// (subscribed above) gate the AC vote. Attestation/sync topics are not joined.
		if err := r.nd.Subscribe(validator.AvailabilityVoteTopic); err != nil {
			slog.Error("subscribe availability vote topic failed", "node", r.num, "err", err)
		}
		if err := r.nd.Subscribe(validator.FinalityAggregateTopic); err != nil {
			slog.Error("subscribe finality aggregate topic failed", "node", r.num, "err", err)
		}
		if subnet, member := r.sched.Node(r.num).FinalitySubnet(); member {
			if err := r.nd.Subscribe(validator.FinalityVoteTopic(subnet)); err != nil {
				slog.Error("subscribe finality subnet failed", "node", r.num, "subnet", subnet, "err", err)
			}
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

// beginSlot sets up this slot's coupling state when the node attests and/or syncs: it dials 2
// subscribers of each attest-duty subnet it doesn't subscribe (so a fan-out publish has somewhere
// to land), records sync membership, and arms the shared deadline timer — all before the block
// publish, so a proposer that also votes can self-vote. The block publish itself runs unconditionally.
func (r *NodeRunner) beginSlot(slot int, slotStart time.Time) *slotState {
	var ss *slotState
	if r.sched != nil && (r.attest || r.sync || r.decoupled) {
		view := r.sched.Node(r.num)
		ss = &slotState{deadline: slotStart.Add(r.due)}
		if r.attest {
			ss.duties = view.AttestDuties(slot)
			if r.sched.NumColumns > 0 {
				ss.custody = view.CustodyColumns()
			}

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
		} else if r.decoupled {
			// The AC vote is the column-gated attestation, retargeted: duties come from the per-slot
			// VRF draw, the vote rides the global topic (subscribed in Prepare — no per-subnet dial),
			// and columns gate it exactly as for attestations.
			ss.duties = view.ACVoteDuties(slot)
			ss.custody = view.CustodyColumns()
		}
		// The column gate only blocks the attestation vote; with no custody (columns off, or a
		// sync-only run that doesn't attest) it's trivially complete.
		ss.columnsComplete = len(ss.custody) == 0
		if !ss.columnsComplete {
			ss.haveColumn = make(map[int]bool, len(ss.custody))
		}
		if r.sync { // this node's stable sync membership (a member emits one message on its subnet)
			ss.syncSubnet, ss.syncMember = view.SyncSubnet()
		}

		r.mu.Lock()
		r.slots[slot] = ss
		r.mu.Unlock()
		ss.timer = time.AfterFunc(time.Until(ss.deadline), func() { r.onDeadline(slot, ss) })

		// Aggregate phase: if this node aggregates any committee this slot, arm a timer to
		// publish its M aggregates at the aggregate deadline (fixed offset, no coupling).
		if r.attest && r.aggDue > 0 {
			ss.aggSubnets = view.AggregateSubnets(slot)
			if len(ss.aggSubnets) > 0 {
				ss.aggTimer = time.AfterFunc(time.Until(slotStart.Add(r.aggDue)), func() {
					r.emitAggregate(slot, ss)
				})
			}
		}
		// Sync contribution phase: if this node aggregates any sync subnet this slot, arm a timer
		// to publish its contributions at the contribution deadline (reuses aggDue; fixed offset).
		if r.sync && r.aggDue > 0 {
			ss.syncAggSubnets = view.SyncAggregateSubnets(slot)
			if len(ss.syncAggSubnets) > 0 {
				ss.syncAggTimer = time.AfterFunc(time.Until(slotStart.Add(r.aggDue)), func() {
					r.emitSyncContribution(slot, ss)
				})
			}
		}
		// Finality chain: at a finality boundary, arm this node's fixed-time per-validator vote burst
		// (its aggregate timer + pre-join, if an aggregator, land in M5). FC state lives in r.finals,
		// not ss, so it survives the intervening per-AC-slot endSlot pruning.
		if r.decoupled {
			r.armFinalityVote(slot, slotStart, view)
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
	subs := r.sched.Subscribers(subnet)
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
	if ss.syncAggTimer != nil {
		ss.syncAggTimer.Stop()
	}
	r.nd.Disconnect(ss.dialed)
	r.mu.Lock()
	delete(r.slots, slot)
	r.mu.Unlock()
	if r.decoupled {
		r.reapFinality(slot)
	}
}

// armFinalityVote, at a finality boundary (slot % k == 0), arms this node's fixed-time per-validator
// finality-vote burst for finality slot n = slot/k. The boundary slot's slotStart IS
// finalitySlotStart(n), so the vote fires at slotStart + fcVoteOffset. The state is created
// get-or-create (M5's aggregator pre-join may have created it a slot earlier) and stored in r.finals.
func (r *NodeRunner) armFinalityVote(slot int, slotStart time.Time, view schedule.View) {
	if slot%r.k != 0 {
		return
	}
	n := slot / r.k
	subnet, member := view.FinalitySubnet()
	if !member { // every node is a member, but guard a phase-off / partial assignment
		return
	}
	r.mu.Lock()
	fs := r.finals[n]
	if fs == nil {
		fs = &finalityState{subnet: subnet}
		r.finals[n] = fs
	}
	fs.duties = view.FinalityVoteDuties()
	r.mu.Unlock()
	fs.voteTimer = time.AfterFunc(time.Until(slotStart.Add(r.fcVoteOffset)), func() {
		r.emitFinalityVotes(n, fs)
	})
}

// emitFinalityVotes publishes one finality vote per validator this node hosts, on its subnet, at
// most once per finality slot. The vote is fixed-time and dissemination-only this cut (un-gated, no
// fork-choice outcome), so OnPublish records no vote. The subnet is already subscribed in Prepare.
func (r *NodeRunner) emitFinalityVotes(n int, fs *finalityState) {
	fs.voteOnce.Do(func() {
		at := time.Now()
		topic := validator.FinalityVoteTopic(fs.subnet)
		for _, val := range fs.duties {
			msg := validator.MakeFinalityVote(n, fs.subnet, val, r.num)
			r.tracer.OnPublish(metrics.FinalityVoteID(n, fs.subnet, val, r.num), false, at)
			if err := r.nd.Publish(r.runCtx, topic, msg.Payload); err != nil {
				slog.Error("publish finality vote failed", "node", r.num, "finality_slot", n, "val", val, "err", err)
			}
		}
	})
}

// reapFinality prunes finality state at the finality slot's last AC slot ((n+1)*k − 1, i.e. slot
// where (slot+1)%k == 0). By then the vote timer has long fired (fcVoteOffset ≪ k slots), so the
// stop is defensive. This is the only place r.finals is pruned — it deliberately outlives the
// per-AC-slot endSlot pruning of r.slots that runs every intervening slot.
func (r *NodeRunner) reapFinality(slot int) {
	if (slot+1)%r.k != 0 {
		return
	}
	n := slot / r.k
	r.mu.Lock()
	if fs := r.finals[n]; fs != nil {
		if fs.voteTimer != nil {
			fs.voteTimer.Stop()
		}
		delete(r.finals, n)
	}
	r.mu.Unlock()
}

// publishBlock waits until when, records the publish, then publishes the block and (when the
// column phase is on) bursts one DataColumnSidecar on each column subnet — back-to-back, at
// the block's instant, on meshes the proposer (a full-custody node) already joined at
// bring-up. A proposer that also attests this slot votes for its own block (block_processed =
// now); loopback is not routed through the tracer, so this is the only place self-block-seen
// is set.
func (r *NodeRunner) publishBlock(when time.Time, msg validator.Message) {
	time.Sleep(time.Until(when))
	now := time.Now()
	r.tracer.OnPublish(metrics.BlockID(msg.Slot, r.num), false, now)
	if err := r.nd.Publish(r.runCtx, msg.Topic, msg.Payload); err != nil {
		slog.Error("publish block failed", "node", r.num, "slot", msg.Slot, "err", err)
	}
	if r.sched != nil && r.sched.NumColumns > 0 {
		for col := range r.sched.NumColumns {
			cmsg := validator.MakeColumn(msg.Slot, col, r.num)
			r.tracer.OnPublish(metrics.ColumnID(msg.Slot, col, r.num), false, now)
			if err := r.nd.Publish(r.runCtx, cmsg.Topic, cmsg.Payload); err != nil {
				slog.Error("publish column failed", "node", r.num, "slot", msg.Slot, "column", col, "err", err)
			}
		}
		// The proposer holds every column it just made; mark custody complete so it can
		// self-vote block immediately (it never self-receives — loopback is skipped).
		r.markColumnsComplete(msg.Slot, now)
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
	case node.KindColumn:
		col := rec.Obj.(*pb.Column)
		if int(col.Origin) == r.num { // skip our own published column (loopback)
			return
		}
		r.tracer.OnReceive(r.num, metrics.ColumnID(int(col.Slot), int(col.Column), int(col.Origin)), rec.At)
		r.onColumnProcessed(int(col.Slot), int(col.Column), rec.At)
	case node.KindSyncMessage:
		sm := rec.Obj.(*pb.SyncMessage)
		if int(sm.Origin) == r.num { // skip our own published sync message (loopback)
			return
		}
		r.tracer.OnReceive(r.num, metrics.SyncMessageID(int(sm.Slot), int(sm.Subnet), int(sm.Origin)), rec.At)
	case node.KindSyncContribution:
		sc := rec.Obj.(*pb.SyncContribution)
		if int(sc.Origin) == r.num { // skip our own published contribution (loopback)
			return
		}
		r.tracer.OnReceive(r.num, metrics.SyncContributionID(int(sc.Slot), int(sc.Subnet), int(sc.Origin)), rec.At)
	case node.KindACVote:
		v := rec.Obj.(*pb.ACVote)
		if int(v.Origin) == r.num { // skip our own published AC vote (loopback)
			return
		}
		r.tracer.OnReceive(r.num, metrics.ACVoteID(int(v.Slot), int(v.Val), int(v.Origin)), rec.At)
	case node.KindFinalityVote:
		fv := rec.Obj.(*pb.FinalityVote)
		if int(fv.Origin) == r.num { // skip our own published finality vote (loopback)
			return
		}
		r.tracer.OnReceive(r.num,
			metrics.FinalityVoteID(int(fv.FinalitySlot), int(fv.Subnet), int(fv.Val), int(fv.Origin)), rec.At)
	}
}

// onBlockProcessed is the causal edge: the slot's block was processed at `at`. It records
// block-seen once, then attempts the early vote (which the gate also conditions on all custody
// columns; see tryEarlyEmit). A late block/column is left to the deadline timer (which votes
// prior). emitOnce guarantees a single emission no matter which path fires first.
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
	r.mu.Unlock()
	r.tryEarlyEmit(slot, ss)
	r.trySyncEmit(slot, ss) // sync votes head on block-seen alone (no column gate)
}

// onColumnProcessed records a custody column's arrival (post-verify, so consistent with
// block-seen). When it completes the node's custody set it attempts the early vote — a
// late-completing column can unblock a block vote that was waiting on custody. A no-op once
// custody is already complete (covers the columns-off and proposer cases).
func (r *NodeRunner) onColumnProcessed(slot, col int, at time.Time) {
	r.mu.Lock()
	ss, ok := r.slots[slot]
	if !ok || ss.columnsComplete {
		r.mu.Unlock()
		return
	}
	if !ss.haveColumn[col] {
		ss.haveColumn[col] = true
		if len(ss.haveColumn) == len(ss.custody) {
			ss.columnsComplete, ss.columnsCompleteAt = true, at
		}
	}
	complete := ss.columnsComplete
	r.mu.Unlock()
	if complete {
		r.tryEarlyEmit(slot, ss)
	}
}

// markColumnsComplete records custody completion at `at` without a per-column count — used by
// the proposer, which holds every column it just published (it never self-receives).
func (r *NodeRunner) markColumnsComplete(slot int, at time.Time) {
	r.mu.Lock()
	if ss, ok := r.slots[slot]; ok && !ss.columnsComplete {
		ss.columnsComplete, ss.columnsCompleteAt = true, at
	}
	r.mu.Unlock()
}

// tryEarlyEmit attempts this node's early vote-block attestation. The gate: vote block only
// once the block is processed AND all custody columns are in, emitted at
// max(block, columns_complete)+Δ_prep and only if that lands by the deadline (else the
// deadline timer votes prior head). Reuses the pure emitDecision by feeding it the gated
// readiness and the later of the two times; emitOnce collapses the racing block/column/
// deadline attempts into one emission.
func (r *NodeRunner) tryEarlyEmit(slot int, ss *slotState) {
	if !r.attest && !r.decoupled { // a sync-only run has a slotState but no column-gated vote
		return
	}
	r.mu.Lock()
	ready := ss.seen && ss.columnsComplete
	processed := laterOf(ss.seenAt, ss.columnsCompleteAt)
	seenOrigin, deadline := ss.seenOrigin, ss.deadline
	r.mu.Unlock()

	emitAt, voteBlock := emitDecision(ready, processed, deadline, r.prep)
	if !voteBlock {
		return
	}
	if d := time.Until(emitAt); d > 0 { // honor Δ_prep before emitting
		time.AfterFunc(d, func() { r.emitGated(slot, ss, seenOrigin) })
	} else {
		r.emitGated(slot, ss, seenOrigin)
	}
}

// emitGated dispatches the column-gated vote to the active phase's emitter: the AC vote (one
// global topic) under decoupled, else the per-subnet attestation. Both share ss.emitOnce, so a
// slot emits exactly one (attest and decoupled are mutually exclusive).
func (r *NodeRunner) emitGated(slot int, ss *slotState, votedOrigin int) {
	if r.decoupled {
		r.emitACVote(slot, ss, votedOrigin)
		return
	}
	r.emit(slot, ss, votedOrigin)
}

// trySyncEmit attempts this member's sync-committee message — the head vote, emitted at
// min(block_processed+Δ_prep, deadline) and only if that lands by the deadline (else the deadline
// timer votes prior head). Unlike the attestation vote it is UN-GATED by data availability: it
// feeds emitDecision the raw block-seen state, not the column-gated readiness — so a node with the
// block but a missing custody column votes head on sync yet prior on its attestation, isolating the
// DA gate's effect. A no-op for a non-member or when sync is off; syncEmitOnce collapses the racing
// block/deadline attempts into one emission.
func (r *NodeRunner) trySyncEmit(slot int, ss *slotState) {
	if !r.sync || !ss.syncMember {
		return
	}
	r.mu.Lock()
	seen, seenAt, origin, deadline := ss.seen, ss.seenAt, ss.seenOrigin, ss.deadline
	r.mu.Unlock()
	emitAt, voteHead := emitDecision(seen, seenAt, deadline, r.prep)
	if !voteHead {
		return
	}
	if d := time.Until(emitAt); d > 0 { // honor Δ_prep before emitting
		time.AfterFunc(d, func() { r.emitSyncMessage(slot, ss, origin) })
	} else {
		r.emitSyncMessage(slot, ss, origin)
	}
}

// onDeadline emits this slot's attestations if not already emitted, re-deriving the vote from
// block-seen + custody-complete state (so a block/column recorded exactly at the deadline
// still votes block); otherwise it votes prior head — a node with the block but a missing
// custody column votes prior, the column phase's measurable effect on the slot.
func (r *NodeRunner) onDeadline(slot int, ss *slotState) {
	r.mu.Lock()
	ready := ss.seen && ss.columnsComplete
	processed := laterOf(ss.seenAt, ss.columnsCompleteAt)
	origin, seen, seenAt := ss.seenOrigin, ss.seen, ss.seenAt
	r.mu.Unlock()
	if r.attest || r.decoupled {
		_, voteBlock := emitDecision(ready, processed, ss.deadline, r.prep)
		votedOrigin := -1
		if voteBlock {
			votedOrigin = origin
		}
		r.emitGated(slot, ss, votedOrigin)
	}
	if r.sync && ss.syncMember { // sync votes head on block-seen alone (un-gated by columns)
		_, voteHead := emitDecision(seen, seenAt, ss.deadline, r.prep)
		votedOrigin := -1
		if voteHead {
			votedOrigin = origin
		}
		r.emitSyncMessage(slot, ss, votedOrigin)
	}
}

// laterOf returns the later of two times (the gate emits at max(block, columns_complete)).
func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
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

// emitACVote publishes one AC vote per duty on the single global availability-vote topic, at most
// once per slot — the no-aggregation, no-subnet twin of emit (the AC vote is the column-gated
// attestation, retargeted). votedOrigin is the voted block's origin (>=0) or -1 for the prior head.
func (r *NodeRunner) emitACVote(slot int, ss *slotState, votedOrigin int) {
	ss.emitOnce.Do(func() {
		at := time.Now()
		votedBlock := votedOrigin >= 0
		for _, d := range ss.duties {
			msg := validator.MakeACVote(slot, d.Val, r.num, votedOrigin)
			r.tracer.OnPublish(metrics.ACVoteID(slot, d.Val, r.num), votedBlock, at)
			if err := r.nd.Publish(r.runCtx, validator.AvailabilityVoteTopic, msg.Payload); err != nil {
				slog.Error("publish AC vote failed", "node", r.num, "slot", slot, "val", d.Val, "err", err)
			}
		}
	})
}

// emitSyncMessage publishes this member's one sync-committee message on its subnet, at most once
// per slot (the sync-emit-once guard collapses the racing block-receipt and deadline attempts).
// votedOrigin is the voted block's origin (>=0) or -1 for the prior head. A no-op for a non-member.
func (r *NodeRunner) emitSyncMessage(slot int, ss *slotState, votedOrigin int) {
	if !ss.syncMember {
		return
	}
	ss.syncEmitOnce.Do(func() {
		at := time.Now()
		voteHead := votedOrigin >= 0
		topic := validator.SyncMessageTopic(ss.syncSubnet)
		if err := r.nd.Join(topic); err != nil { // already subscribed at bring-up; idempotent
			slog.Error("join sync subnet failed", "node", r.num, "subnet", ss.syncSubnet, "err", err)
			return
		}
		msg := validator.MakeSyncMessage(slot, ss.syncSubnet, r.num, votedOrigin)
		r.tracer.OnPublish(metrics.SyncMessageID(slot, ss.syncSubnet, r.num), voteHead, at)
		if err := r.nd.Publish(r.runCtx, topic, msg.Payload); err != nil {
			slog.Error("publish sync message failed", "node", r.num, "slot", slot, "subnet", ss.syncSubnet, "err", err)
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

// emitSyncContribution publishes one contribution for each sync subnet this node aggregates this
// slot, on the global contribution topic, at most once per slot — the sync twin of emitAggregate.
// The contribution carries this node as its origin (the aggregator's signature stand-in), so each
// aggregator's contribution is distinct and gossipsub does not dedup them.
func (r *NodeRunner) emitSyncContribution(slot int, ss *slotState) {
	ss.syncAggEmitOnce.Do(func() {
		at := time.Now()
		for _, subnet := range ss.syncAggSubnets {
			msg := validator.MakeSyncContribution(slot, subnet, r.num)
			r.tracer.OnPublish(metrics.SyncContributionID(slot, subnet, r.num), false, at)
			if err := r.nd.Publish(r.runCtx, msg.Topic, msg.Payload); err != nil {
				slog.Error("publish sync contribution failed", "node", r.num, "slot", slot, "subnet", subnet, "err", err)
			}
		}
	})
}
