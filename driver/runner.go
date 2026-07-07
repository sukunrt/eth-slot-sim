package driver

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
	"github.com/ethp2p/slot-sim/validator"
)

// DecoupledParams turns on the decoupled-consensus phase (nil ⇒ off), mirroring how a nil
// schedule means block-only. When set, the runner emits AC votes (column-gated, on one global
// topic) in place of attestations, and — every K AC slots — finality votes + a population-scaled
// aggregate; attestation/sync emit is suppressed. The AC-vote deadline and Δ_prep reuse the
// existing due/prep. K/FCVoteOffset/FCAggFraction drive the finality chain (used by M4/M5).
// Segregated switches the FC paths to per-AC-slot rounds (validator-segregation-spec.md): round
// s % K's validators vote in slot s (FCVoteOffset now offsets into the AC slot) and per-slot
// aggregators publish cell-scaled aggregates at RoundAggFraction% of the slot (FCAggFraction is
// ignored). It must agree with the schedule's shape (Assignment.Segregated) — the binaries
// assert that at startup.
type DecoupledParams struct {
	K                int           // ac_slots_per_finality_slot (a finality slot spans K AC slots)
	FCVoteOffset     time.Duration // offset into the finality slot (AC slot when segregated) for the FC vote burst
	FCAggFraction    int           // finality_slot_aggregation_fraction (percent, 0 < f < 100; base mode)
	Segregated       bool          // per-AC-slot finality rounds (replaces the per-fslot FC path)
	RoundAggFraction int           // round_aggregation_fraction (% of the AC slot; segregated mode)
}

// NodeRunner is one node's stateful per-slot orchestrator. It owns the slot loop AND
// is the node's OnReceive sink, so it can couple block arrival to attestation emit
// while keeping Node pure. The multi-node Driver builds N; the Shadow binary builds 1.
type NodeRunner struct {
	num         int
	nd          *node.Node
	view        schedule.View // this node's slice of the plan; zero ⇒ block-only (Phase 1)
	numNodes    int           // fleet size (the cyclic proposer fallback when view is zero)
	blockSize   int           // proposed-block payload bytes
	offset      time.Duration // block publish offset into the slot
	jitter      time.Duration // block publish lands in [offset, offset+jitter)
	attest      bool          // emit attestations (with a view set but false ⇒ columns-only)
	sync        bool          // emit sync-committee messages + contributions (members only)
	decoupled   bool          // emit AC votes + finality votes/aggregates (replaces attest/sync)
	tracer      metrics.Tracer
	slotDur     time.Duration
	blockDue    time.Duration // attestation / AC-vote deadline, offset into the slot
	aggDue      time.Duration // the aggregation deadline (~2/3 slot): aggregates + sync contributions publish here
	prep        time.Duration // Δ_prep before emitting on block receipt
	seed        uint64        // seeds the per-slot subnet-peer dial choice
	stablePeers map[int]bool  // the node's long-lived peers (so we know which dials are extra)

	// Decoupled finality-chain knobs (set when decoupled; consumed by the FC paths, M4/M5).
	finalityRoundSize int           // ac_slots_per_finality_slot (k: AC slots per finality slot)
	fcVoteOffset      time.Duration // offset into the finality slot (AC slot when segregated) for the FC vote burst
	fcAggFraction     int           // % of the finality slot when FC aggregates publish (base mode)
	segregated        bool          // per-AC-slot finality rounds (validator segregation)
	roundAggFraction  int           // % of the AC slot when round aggregates publish (segregated)

	// Partial transport (partial-attestation-spec.md; off ⇒ classic). The emit paths swap
	// Publish for manager calls; everything else (timing, coupling, warmup, OnPublish) stays.
	partial     bool
	attDataSize int // marshaled attestation_data bytes per bucket
	sigSize     int // signature filler bytes per vote

	runCtx context.Context // set at Run; used by emit/publish
	// Round 0's finality warm-up runs off the slot path (round0Warmup): round0Once makes the first
	// caller — Prepare's bring-up goroutine, or slot 0's own setup if it wins the race — SPAWN the
	// pre-join, and round0Ready (closed when its dials are up) is the wait every caller then parks
	// on, so the vote timer is never armed before round 0's joins exist. round0Ready is a channel
	// rather than sync.Once's own wait deliberately: the pre-join ends in a Dial (a time-blocking
	// handshake), and synctest advances its fake clock while a goroutine waits on a channel but NOT
	// while it waits on sync.Once's internal mutex — so blocking a second caller in Once.Do across
	// the dial would wedge the in-process tests behind that dial. Created only when decoupled.
	round0Once  sync.Once
	round0Ready chan struct{}
	mu          sync.Mutex
	slots       map[int]*slotState
	// finals holds the decoupled finality state: keyed by finality slot in base mode (spans k
	// AC slots), by AC slot under segregation (a round spans 2: pre-join at s−1, the rest in s).
	finals map[int]*finalityState
}

// finalityState is the per-(node, round) state of the finality chain. In base mode a round IS a
// finality slot: unlike slotState (per AC slot), it spans k AC slots — set up at the pre-join
// slot n·k−1 (the boundary itself for n=0), drained by its one-shot vote and aggregation
// timers, and reaped at the finality slot's last AC slot — keyed in r.finals by the finality
// slot index, so the per-AC-slot endSlot pruning of r.slots leaves it untouched. Under
// segregation every AC slot is a round: set up at the previous slot, drained inside its own,
// reaped at its endSlot, keyed by the AC slot.
type finalityState struct {
	duties     []schedule.AttestDuty // hosted validators × their drawn subnets (one vote each)
	ownSubnet  int                   // this node's stable finality subnet (the partial member/fanout split)
	voteDialed []int                 // fan-out dials into non-member duty subnets (at the pre-join)
	voteTimer  *time.Timer           // fires at finalitySlotStart(n) + fcVoteOffset
	voteOnce   sync.Once             // one FC vote burst per finality slot

	// Aggregation state (set at the pre-join). aggregateSubnets = the subnets this node
	// aggregates (validator-sampled from the ENTIRE set, so generally foreign);
	// aggregateSubscribed = the topics it Subscribe'd for the duty (the foreign ones);
	// aggregateDialed = the subnet peers dialed so those meshes have somewhere to graft. All of
	// it is torn down at the aggregation deadline.
	aggregateSubnets    []int
	aggregateSubscribed []string
	aggregateDialed     []int
	aggregateTimer      *time.Timer // publishes the aggregates at the deadline (aggregators only)
	aggregateOnce       sync.Once   // one aggregate burst per finality slot
	dropTimer           *time.Timer // tears the pre-join state down at the deadline (its own timer)
	dropOnce            sync.Once   // teardown: at the drop deadline, or reap as the fallback
	sealTimer           *time.Timer // partial transport: seals the round's buckets at the deadline
}

// slotState is the per-(node, slot) coupling holder.
type slotState struct {
	deadline time.Time
	// attestationDuties: this slot's attest duties (attest path); acVoteDuties: the AC-vote duties
	// (decoupled path) — mutually exclusive. attestationSubnetsSubscribed: the node's stable member
	// subnets (the partial member/fanout split).
	attestationDuties            []schedule.AttestDuty
	acVoteDuties                 []schedule.ACVoteDuty
	attestationSubnetsSubscribed map[int]bool

	dialed       []int         // extra peers dialed this slot (off-path), dropped towards slot end
	dialReady    chan struct{} // closed when the async dial finishes; emit waits before fan-out
	dialTimer    *time.Timer   // drops the dials towards slot end (off endSlot's path)
	dropDialOnce sync.Once     // the dialTimer and endSlot share one disconnect
	timer        *time.Timer

	blockSeen       bool
	blockSeenAt     time.Time
	blockSeenOrigin int

	// The slot's single attestation/AC vote: claimed atomically (under r.mu) by whichever path
	// fires first — the early block vote on readiness, or the deadline's prior-head fallback.
	// alreadyVoted is the claim guard; votedForSlotBlock records whether it was for the slot's
	// block (else the prior head), which the emit reads to set votedOrigin.
	alreadyVoted      bool
	votedForSlotBlock bool

	// Sync-committee membership this slot (set in setupSlot from view.SyncSubnet). A member
	// emits one message on syncSubnet at min(block_seen, deadline); a non-member emits none.
	// syncAlreadyVoted / syncVotedForSlotBlock are the sync vote's claim + decision (the sync
	// twin of alreadyVoted; sync votes head un-gated by columns).
	syncSubnet            int
	syncMember            bool
	syncAlreadyVoted      bool
	syncVotedForSlotBlock bool

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

// RunnerConfig is the run-wide half of a runner's construction — identical for every node in a
// run, so the Driver builds one and hands it to all N runners (the per-node half — num, node,
// proposer, basePeers — stays positional on NewRunner). Field names mirror driver.Config; the
// zero value is a block-only runner.
type RunnerConfig struct {
	Attest bool // emit attestations; a view set with Attest false ⇒ columns-only
	Sync   bool // emit sync-committee messages + contributions (members only)

	// Block proposal: the view's per-slot proposer (cyclic slot%NumNodes when the view is
	// zero) publishes a BlockSize-byte block at Offset + rand(0, Jitter) into the slot.
	NumNodes  int
	BlockSize int
	Offset    time.Duration
	Jitter    time.Duration

	SlotDuration   time.Duration
	AttestationDue time.Duration // attestation / AC-vote deadline, offset into the slot

	// AggregateDue is the aggregation deadline (~2/3 slot), shared by attestation aggregates
	// and sync contributions (mainnet has both due at the same instant). Whether anyone
	// aggregates is the plan's business (the view's aggregator duties), not a knob's.
	AggregateDue time.Duration

	Prep time.Duration // Δ_prep before emitting on block receipt
	Seed uint64        // seeds the per-slot subnet-peer dial choice + the block jitter

	// Decoupled (nil ⇒ off) turns on the decoupled-consensus phase, which suppresses
	// attestation/sync emit (the AC vote replaces attestations). Partial (nil ⇒ classic)
	// switches the attestation-class floods to the partial transport — a transport, not a
	// phase, so it needs Attest or Decoupled on, and the node's Partial options set.
	Decoupled *DecoupledParams
	Partial   *PartialParams
}

// NewRunner builds a runner for one node: nd identifies it, view is its slice of the plan
// (injected, so the runner can only ever ask about its own duties; zero ⇒ block-only),
// basePeers is its long-lived peer set (so per-slot subnet dials added on top can be
// dropped), and cfg carries the run-wide knobs shared by every runner in the run.
func NewRunner(nd *node.Node, view schedule.View, basePeers []int,
	tracer metrics.Tracer, cfg RunnerConfig) *NodeRunner {
	base := make(map[int]bool, len(basePeers))
	for _, p := range basePeers {
		base[p] = true
	}
	r := &NodeRunner{
		num:         nd.Num,
		nd:          nd,
		view:        view,
		numNodes:    cfg.NumNodes,
		blockSize:   cfg.BlockSize,
		offset:      cfg.Offset,
		jitter:      cfg.Jitter,
		attest:      cfg.Attest,
		sync:        cfg.Sync,
		tracer:      tracer,
		slotDur:     cfg.SlotDuration,
		blockDue:    cfg.AttestationDue,
		aggDue:      cfg.AggregateDue,
		prep:        cfg.Prep,
		seed:        cfg.Seed,
		stablePeers: base,
		slots:       make(map[int]*slotState),
	}
	if dc := cfg.Decoupled; dc != nil {
		r.decoupled = true
		r.finalityRoundSize, r.fcVoteOffset, r.fcAggFraction = dc.K, dc.FCVoteOffset, dc.FCAggFraction
		r.segregated, r.roundAggFraction = dc.Segregated, dc.RoundAggFraction
		r.finals = make(map[int]*finalityState)
		r.round0Ready = make(chan struct{})
	}
	// The plan decides who aggregates; the deadline only says when. A plan that draws
	// aggregators with no deadline set would silently never publish them — refuse instead.
	if cfg.AggregateDue <= 0 {
		if cfg.Attest && view.HasAggregators {
			panic("driver: the plan draws aggregators but AggregateDue is unset")
		}
		if cfg.Sync && view.HasSyncAggregators {
			panic("driver: the plan draws sync aggregators but AggregateDue is unset")
		}
	}
	// Same rule for the finality chain: the plan decides who aggregates, the fraction only
	// says when. Aggregators drawn with no valid fraction would publish at slot start and tear
	// the pre-join down before the vote burst — refuse instead.
	if dc := cfg.Decoupled; dc != nil && view.HasFinalityAggregators {
		frac := dc.FCAggFraction
		if dc.Segregated {
			frac = dc.RoundAggFraction
		}
		if frac <= 0 || frac >= 100 {
			panic("driver: the plan draws finality aggregators but the aggregation fraction is not in (0,100)")
		}
	}
	if pp := cfg.Partial; pp != nil {
		if !cfg.Attest && cfg.Decoupled == nil {
			panic("driver: the partial transport requires the attestation phase or decoupled consensus")
		}
		if nd.Partial == nil {
			panic("driver: partial transport on but Node.Partial unset")
		}
		r.partial, r.attDataSize, r.sigSize = true, pp.dataSize(), pp.sigSize()
	}
	return r
}

// Attach wires the runner as the node's receive sink. Call before JoinTopics.
func (r *NodeRunner) Attach() { r.nd.OnReceive = r.onReceive }

// Prepare subscribes the node's own subnets (its long-lived meshes). Call during bring-up,
// before the settle, so the meshes form before slot 0.
func (r *NodeRunner) Prepare() {
	if r.attest {
		if r.view.HasAggregators { // every node joins the global aggregate mesh (it downloads all aggregates)
			if err := r.nd.Subscribe(validator.AggregateTopic); err != nil {
				slog.Error("subscribe aggregate topic failed", "node", r.num, "err", err)
			}
		}
		for _, s := range r.view.SubscribedSubnets {
			if err := r.nd.Subscribe(validator.AttestationTopic(s)); err != nil {
				slog.Error("subscribe subnet failed", "node", r.num, "subnet", s, "err", err)
			}
		}
	}
	if r.view.NumColumns > 0 { // the node's custody column meshes (the DA dissemination phase)
		for _, c := range r.view.CustodyColumns {
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
		if r.view.SyncMember {
			if err := r.nd.Subscribe(validator.SyncMessageTopic(r.view.SyncSubnet)); err != nil {
				slog.Error("subscribe sync subnet failed", "node", r.num, "subnet", r.view.SyncSubnet, "err", err)
			}
		}
	}
	if r.decoupled {
		// Every node downloads all AC votes and FC aggregates (global topics, like aggregates), and
		// persistently meshes its one finality subnet (the stable receiver core; its validators'
		// votes ride their own drawn subnets, fanning out where the node isn't a member). Columns
		// (subscribed above) gate the AC vote. Attestation/sync topics are not joined.
		if err := r.nd.Subscribe(validator.AvailabilityVoteTopic); err != nil {
			slog.Error("subscribe availability vote topic failed", "node", r.num, "err", err)
		}
		if err := r.nd.Subscribe(validator.FinalityAggregateTopic); err != nil {
			slog.Error("subscribe finality aggregate topic failed", "node", r.num, "err", err)
		}
		if r.view.FinalityMember {
			if err := r.nd.Subscribe(validator.FinalityVoteTopic(r.view.FinalitySubnet)); err != nil {
				slog.Error("subscribe finality subnet failed", "node", r.num, "subnet", r.view.FinalitySubnet, "err", err)
			}
		}
		// Round 0's finality warm-up belongs to bring-up: kicked here it runs during the settle
		// window (in Shadow each host reaches Prepare at its own mesh-join stagger, so the fleet's
		// dial barriers spread out over a quiet ~3 minutes), and slot 0's setup finds it long done.
		// Kicking it from Run instead would fire it AT runStart — Run is only invoked at slot 0 —
		// recreating the very race the warm-up exists to avoid.
		go r.round0Warmup()
	}
}

// Run paces one setup per slot start from runStart. beginSlot is non-blocking — every slot's work
// (dials, emits, the finality pre-join, cleanup) runs in goroutines and timers — so the loop never
// drifts off the slot clock. Round 0's finality warm-up already ran off Prepare (bring-up); slot
// 0's setup joins it through round0Warmup. It returns at the last slot's end; the Driver then
// drains in-flight receives.
func (r *NodeRunner) Run(ctx context.Context, runStart time.Time, numSlots int) {
	r.runCtx = ctx
	for slot := range numSlots {
		slotStart := runStart.Add(time.Duration(slot) * r.slotDur)
		time.Sleep(time.Until(slotStart))
		r.beginSlot(slot, slotStart)
	}
	time.Sleep(time.Until(runStart.Add(time.Duration(numSlots) * r.slotDur)))
}

// beginSlot kicks this slot's setup OFF the Run loop and returns at once: it MUST NOT block. All of
// setupSlot — schedule reads, the fan-out dials, timer arming, the finality pre-join, the block
// publishes — runs in its own goroutine (and the timers it arms), so a slow handshake or a heavy
// schedule read never stalls the slot clock or the next slot's start.
func (r *NodeRunner) beginSlot(slot int, slotStart time.Time) {
	go r.setupSlot(slot, slotStart)
}

// setupSlot runs every slot's lifecycle: build the slot state, fill the active phases' duties
// (kicking the fan-out dials off-path — emit awaits them), register it, arm the timers — all
// before the block publishes so a proposer that also votes can self-vote — and finally arm the
// slot-end cleanup. Every slot gets the same lifecycle; a block-only slot just carries no duties.
func (r *NodeRunner) setupSlot(slot int, slotStart time.Time) {
	ss := &slotState{deadline: slotStart.Add(r.blockDue)}
	if r.attest {
		ss.attestationDuties = r.view.AttestDuties[slot]
		subscribed := map[int]bool{}
		for _, s := range r.view.SubscribedSubnets {
			subscribed[s] = true
		}
		ss.attestationSubnetsSubscribed = subscribed

		needsDial := false
		for _, d := range ss.attestationDuties {
			if !subscribed[d.Subnet] {
				needsDial = true
				break
			}
		}

		if needsDial {
			ss.dialReady = make(chan struct{})
			go r.dialDuties(slot, ss)
			ss.dialTimer = time.AfterFunc(time.Until(slotStart.Add(r.slotDur)),
				func() { r.dropDials(ss) })
		}

		// Aggregate phase: if this node aggregates any committee this slot, arm a timer to
		// publish its M aggregates at the aggregation deadline (fixed offset, no coupling).
		ss.aggSubnets = r.view.AggregateSubnets[slot]
		if len(ss.aggSubnets) > 0 {
			ss.aggTimer = time.AfterFunc(time.Until(slotStart.Add(r.aggDue)), func() {
				r.emitAggregate(slot, ss)
			})
		}
	} else if r.decoupled {
		// The AC vote is the column-gated attestation, retargeted: duties come from the per-slot
		// VRF draw, the vote rides the global topic (subscribed in Prepare — no per-subnet dial),
		// and columns gate it exactly as for attestations.
		ss.acVoteDuties = r.view.ACVoteDuties[slot]

		// Finality chain: the aggregator pre-join (slot before a boundary) and, at a boundary,
		// this node's fixed-time per-validator vote burst + (if an aggregator) its aggregate. FC
		// state lives in r.finals, not ss, so it survives the intervening per-AC-slot endSlot
		// pruning. The two FC shapes are wholly separate paths: per-AC-slot rounds when
		// segregated, per-finality-slot when base.
		if r.segregated {
			r.armFinalitySubRound(slot, slotStart)
		} else {
			r.armFinality(slot, slotStart)
		}
	}

	// The DA gate: this node's custody columns (from the column phase; empty ⇒ gate
	// trivially complete). Only the vote paths wait on it — the attestation and the AC
	// vote emit block only once all custody is in (tryEarlyEmit); sync votes head
	// un-gated by design, isolating the DA gate's effect.
	ss.custody = r.view.CustodyColumns
	ss.columnsComplete = len(ss.custody) == 0
	ss.haveColumn = make(map[int]bool, len(ss.custody))

	if r.sync {
		// This node's stable sync membership (a member emits one message on its subnet).
		ss.syncSubnet, ss.syncMember = r.view.SyncSubnet, r.view.SyncMember

		// Sync contribution phase: if this node aggregates any sync subnet this slot, arm a
		// timer to publish its contributions at the aggregation deadline (shared with
		// aggregates).
		ss.syncAggSubnets = r.view.SyncAggregateSubnets[slot]
		if len(ss.syncAggSubnets) > 0 {
			ss.syncAggTimer = time.AfterFunc(time.Until(slotStart.Add(r.aggDue)), func() {
				r.emitSyncContribution(slot, ss)
			})
		}
	}

	r.mu.Lock()
	r.slots[slot] = ss
	r.mu.Unlock()
	// The deadline dispatches per phase inside onBlockDeadline; with no vote phase on it no-ops.
	ss.timer = time.AfterFunc(time.Until(ss.deadline), func() { r.onBlockDeadline(slot, ss) })
	if r.proposes(slot) {
		msg := validator.MakeBlock(slot, r.num, r.blockSize)
		go r.publishBlock(slotStart.Add(r.blockPublishAt(slot)), msg)
	}
	// Slot-end cleanup runs off a timer, not the Run loop (which has moved on).
	time.AfterFunc(time.Until(slotStart.Add(r.slotDur)), func() { r.endSlot(slot, ss) })
}

// proposes reports whether this node publishes slot's block: the plan's per-slot proposer
// when the plan names proposers, else the cyclic slot%N rule (block-only runs).
func (r *NodeRunner) proposes(slot int) bool {
	if len(r.view.Proposes) > 0 {
		return r.view.Proposes[slot]
	}
	return slot%r.numNodes == r.num
}

// Stream keys for the runner's per-purpose PCG draws: PCG takes one uint64 stream, so the
// purpose salt keeps the three families of draws disjoint and the shift packs the two (small)
// identifiers injectively. Every draw is a pure function of (run seed, purpose, identity) —
// independent of goroutine interleaving and identical across both backends.
const (
	jitterStream       = 1 << 60 // block publish jitter, |num<<32|slot
	dialStream         = 2 << 60 // attestation fan-out dial order, |slot<<32|subnet
	finalityDialStream = 3 << 60 // finality pre-join dial order, |round<<32|subnet
)

// blockPublishAt draws the block's publish instant, offset + rand(0, jitter) into the slot.
func (r *NodeRunner) blockPublishAt(slot int) time.Duration {
	at := r.offset
	if r.jitter > 0 {
		rng := rand.New(rand.NewPCG(r.seed, jitterStream|uint64(r.num)<<32|uint64(slot)))
		at += time.Duration(rng.Int64N(int64(r.jitter)))
	}
	return at
}

// shuffledSubscribers returns subnet's subscribers in a seeded order (so the 2 dialed are
// reproducible across runs/backends), keyed by (seed, slot, subnet).
func (r *NodeRunner) shuffledSubscribers(slot, subnet int) []int {
	order := slices.Clone(r.view.Subscribers[subnet])
	rng := rand.New(rand.NewPCG(r.seed, dialStream|uint64(slot)<<32|uint64(subnet)))
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}

// dialDuties joins each non-member duty subnet and dials 2 of its subscribers, so a fan-out
// attestation publish has somewhere to land. It runs off the slot path (r.nd.Dial is a synchronous
// handshake barrier); it records the dialed peers under the lock — before dialing, so a racing
// disconnect still sees the full set — then closes ss.dialReady, which emit's fan-out waits on.
// Subnets are walked in duty order (the seeded peer pick stays reproducible across runs/backends).
func (r *NodeRunner) dialDuties(slot int, ss *slotState) {
	defer close(ss.dialReady)
	var dialed []int
	picked, seen := map[int]bool{}, map[int]bool{}
	for _, d := range ss.attestationDuties {
		if ss.attestationSubnetsSubscribed[d.Subnet] || seen[d.Subnet] {
			continue
		}
		seen[d.Subnet] = true
		if err := r.nd.Join(validator.AttestationTopic(d.Subnet)); err != nil {
			slog.Error("join duty subnet failed", "node", r.num, "subnet", d.Subnet, "err", err)
		}
		n := 0
		for _, peer := range r.shuffledSubscribers(slot, d.Subnet) {
			if n >= 2 {
				break
			}
			if peer == r.num || r.stablePeers[peer] || picked[peer] {
				continue
			}
			picked[peer] = true
			dialed = append(dialed, peer)
			n++
		}
	}
	r.mu.Lock()
	ss.dialed = dialed
	r.mu.Unlock()
	r.nd.Dial(dialed) // synchronous handshakes, but off the slot path
}

// dropDials disconnects this slot's fan-out dials, once. The dialTimer fires it towards slot end;
// endSlot calls it as the backstop. Once-guarded so the two never double-close, reading ss.dialed
// under the lock so it sees whatever dialDuties recorded.
func (r *NodeRunner) dropDials(ss *slotState) {
	ss.dropDialOnce.Do(func() {
		r.mu.Lock()
		dialed := ss.dialed
		r.mu.Unlock()
		r.nd.Disconnect(dialed)
	})
}

// awaitDials blocks until this slot's fan-out dials are up (dialDuties closed ss.dialReady), so a
// fan-out publish lands on a formed mesh. A nil channel (no dials needed this slot) returns at once.
// Callers must run off the receive loop — emit reaches it only via a goroutine or a timer.
func (r *NodeRunner) awaitDials(ss *slotState) {
	if ss.dialReady != nil {
		<-ss.dialReady
	}
}

// endSlot stops the deadline timer, disconnects this slot's extra dials, and prunes the
// slot state so connections and state don't leak across a multi-slot run.
func (r *NodeRunner) endSlot(slot int, ss *slotState) {
	ss.timer.Stop()
	if ss.dialTimer != nil {
		ss.dialTimer.Stop()
	}
	if ss.aggTimer != nil {
		ss.aggTimer.Stop()
	}
	if ss.syncAggTimer != nil {
		ss.syncAggTimer.Stop()
	}
	r.dropDials(ss) // backstop: drop the fan-out dials if the dialTimer hasn't already
	r.mu.Lock()
	delete(r.slots, slot)
	r.mu.Unlock()
	// Partial-bucket GC, one slot behind (the grace slot keeps late stragglers countable).
	if r.partial && r.attest && slot > 0 {
		r.nd.PrunePartial(node.KindAttestation, slot-1)
	}
	if r.decoupled {
		r.reapFinality(slot)
	}
}

// armFinality drives the BASE finality chain at each AC slot (the segregated shape is armRound, a
// wholly separate path the caller picks): the pre-join at the slot before a boundary (vote fan-out
// warm-up + aggregator subscribe), and — at a boundary (slot % k == 0) — this node's per-validator
// vote burst plus the aggregation deadline. The boundary slot's slotStart IS finalitySlotStart(n),
// so the FC timers are armed off it directly. State lives in r.finals (created by the pre-join;
// round 0 has no previous slot, so it is warmed up off the slot path via round0Warmup rather than
// inline here) and spans k AC slots.
func (r *NodeRunner) armFinality(slot int, slotStart time.Time) {
	if (slot+1)%r.finalityRoundSize == 0 { // the AC slot before a boundary: warm up finality slot (slot+1)/k
		go r.prejoinFinality((slot + 1) / r.finalityRoundSize) // off the slot path (see armRound)
	}
	if slot%r.finalityRoundSize != 0 {
		return
	}
	n := slot / r.finalityRoundSize
	if n == 0 {
		// Join round 0's off-path warm-up: if Run's early goroutine is still dialing this WAITS on
		// it, if it finished this returns at once, and if slot 0 got here first this kicks it —
		// either way the vote timer below is not armed before round 0's joins exist.
		r.round0Warmup()
	}
	r.mu.Lock()
	fs := r.finals[n]
	r.mu.Unlock()
	if fs == nil { // every node votes, but guard a phase-off / partial assignment
		return
	}
	fs.voteTimer = time.AfterFunc(time.Until(slotStart.Add(r.fcVoteOffset)), func() {
		r.emitFinalityVotes(n, fs)
	})
	// Two independent one-shot timers at the aggregation deadline (fcAggFraction% · k ·
	// slotDur, multiply before divide — k/2 AC slots out, firing independently of the per-AC-slot
	// Run sleep, surviving the intervening endSlots because they live in r.finals): an aggregator
	// publishes its aggregates; every node that pre-joined network state drops it at the flood's
	// semantic end. With no aggregation phase (fraction 0) the drop falls back to reapFinality.
	aggregationDueAt := slotStart.Add(
		time.Duration(r.fcAggFraction) * time.Duration(r.finalityRoundSize) * r.slotDur / 100)
	if len(fs.aggregateSubnets) > 0 {
		fs.aggregateTimer = time.AfterFunc(time.Until(aggregationDueAt), func() {
			r.emitFinalityAggregate(n, fs)
		})
	}
	if r.view.HasFinalityAggregators && fs.hasPrejoinState() {
		fs.dropTimer = time.AfterFunc(time.Until(aggregationDueAt), func() {
			r.dropFinality(fs)
		})
	}
	// Partial transport: the deadline is the vote flood's semantic end — seal the round's buckets
	// on EVERY node then, so the next round's pre-joined aggregators (fresh mesh members) get no
	// backlog push classic would never deliver. Stragglers still count until the reap prunes.
	if r.partial && r.view.HasFinalityAggregators {
		fs.sealTimer = time.AfterFunc(time.Until(aggregationDueAt), func() {
			r.nd.SealPartial(node.KindFinalityVote, n)
		})
	}
}

// armFinalitySubRound drives the per-AC-slot finality round (validator segregation): every slot pre-joins
// the NEXT slot's round, then arms this round's vote burst at slotStart + fcVoteOffset and its
// aggregation deadline at roundAggFraction% of the slot — both inside this AC slot, so the timers
// fire before this slot's endSlot reaps the state. The per-slot pre-join overlaps the previous
// round's teardown (its deadline) every slot; dropFinality's sparing covers that, exactly as it
// covers the base k=2-at-50% coincidence. Round 0 has no previous slot: it is warmed up off the
// slot path via round0Warmup rather than inline here.
func (r *NodeRunner) armFinalitySubRound(slot int, slotStart time.Time) {
	if slot == 0 {
		// Join round 0's off-path warm-up: if Run's early goroutine is still dialing this WAITS on
		// it, if it finished this returns at once, and if slot 0 got here first this kicks it —
		// either way the vote timer below is not armed before round 0's joins exist.
		r.round0Warmup()
	}
	// The next round's warm-up runs OFF the slot path: prejoinFinality ends in a synchronous
	// Dial barrier (waves of handshakes — seconds on hosts with duties across many subnets),
	// and anything sequenced after it here slips past its offset: this round's 1s vote fired
	// at p50 2.7s at n4000. The goroutine has the full slot to finish.
	go r.prejoinFinality(slot + 1) // a no-op past the run's last round (it bounds itself)
	r.mu.Lock()
	fs := r.finals[slot]
	r.mu.Unlock()
	if fs == nil { // every node votes, but guard a phase-off / partial assignment
		return
	}
	fs.voteTimer = time.AfterFunc(time.Until(slotStart.Add(r.fcVoteOffset)), func() {
		r.emitFinalityVotes(slot, fs)
	})
	// Publish and teardown are independent one-shot timers at the deadline (see armFinality): an
	// aggregator publishes; anyone who pre-joined network state drops it. reapFinality is the
	// teardown fallback when the agg phase is off.
	aggregationDueAt := slotStart.Add(time.Duration(r.roundAggFraction) * r.slotDur / 100)
	if len(fs.aggregateSubnets) > 0 {
		fs.aggregateTimer = time.AfterFunc(time.Until(aggregationDueAt), func() {
			r.emitFinalityAggregate(slot, fs)
		})
	}
	if r.view.HasFinalityAggregators && fs.hasPrejoinState() {
		fs.dropTimer = time.AfterFunc(time.Until(aggregationDueAt), func() {
			r.dropFinality(fs)
		})
	}
	// Partial transport: seal the round's buckets at the per-slot deadline (see armFinality).
	if r.partial && r.view.HasFinalityAggregators {
		fs.sealTimer = time.AfterFunc(time.Until(aggregationDueAt.Add(200*time.Millisecond)), func() {
			r.nd.SealPartial(node.KindFinalityVote, slot)
		})
	}
}

// round0Warmup runs round 0's finality pre-join exactly once, OFF every slot path, and blocks the
// caller until its dials are up. round0Once makes the first caller (Prepare's bring-up goroutine,
// or slot 0's setup if it arrives first) SPAWN the pre-join and return at once — the dial never runs
// under the Once's lock — and round0Ready, closed when the pre-join finishes, is the wait each
// caller then parks on (a channel, so synctest still advances the fake clock while a caller waits;
// see the round0Ready field). A no-op receive once closed, so repeat callers pass straight through.
func (r *NodeRunner) round0Warmup() {
	r.round0Once.Do(func() {
		go func() { r.prejoinFinality(0); close(r.round0Ready) }()
	})
	<-r.round0Ready
}

// prejoinFinality warms up round n at the previous AC slot, so connections exist when the slot
// arrives and the vote burst pushes straight through. n is the finality slot in base mode (run at
// AC slot n·k−1) and the AC slot itself under segregation (run every slot for slot+1) —
// FinalityVoteDuties/FinalityAggregations interpret the key per mode. Round 0 has no previous slot,
// so round0Warmup runs it off the slot path (kicked at bring-up from Prepare, or by slot 0's
// setup if that wins the race). The warm-up:
//
//   - vote fan-out: for each duty subnet the node is NOT a member of (its validators' draws), it
//     Joins the topic and dials 2 of the subnet's stable members — the attestation publish path,
//     on the finality clock;
//   - aggregation: for each subnet it aggregates (a validator sampled from the ENTIRE set lives
//     here, so the node generally has no connection to the subnet at all), it dials 2 members and
//     then Subscribes — a real mesh join, because collecting the subnet's votes is the duty. A
//     stable member of the subnet skips the subscribe (Prepare already did it).
//
// Everything set up here is dropped at the aggregation deadline (or reap). A no-op when the node
// is not a finality member (phase off / partial assignment), or when n is past the run's last
// round (the base-mode boundary pre-join at the final AC slot warms a round that never comes).
func (r *NodeRunner) prejoinFinality(n int) {
	if !r.view.FinalityMember || n >= len(r.view.FinalityVoteDuties) {
		return
	}
	own := r.view.FinalitySubnet
	duties := r.view.FinalityVoteDuties[n]
	aggSubnets := r.view.FinalityAggregations[n]

	// dialTwo returns 2 fresh members of subnet to dial — extra peers only (not self, not the
	// long-lived set, not already picked this pre-join), so the deadline drop is safe.
	picked := map[int]bool{}
	dialTwo := func(subnet int) []int {
		var dialed []int
		for _, peer := range r.shuffledFinalitySubscribers(n, subnet) {
			if len(dialed) >= 2 {
				break
			}
			if peer == r.num || r.stablePeers[peer] || picked[peer] {
				continue
			}
			picked[peer] = true
			dialed = append(dialed, peer)
		}
		return dialed
	}
	var dutySubnets []int
	for _, d := range duties {
		if d.Subnet != own && !slices.Contains(dutySubnets, d.Subnet) {
			dutySubnets = append(dutySubnets, d.Subnet)
		}
	}
	var voteDialed, aggregateDialed []int
	for _, subnet := range dutySubnets {
		voteDialed = append(voteDialed, dialTwo(subnet)...)
	}
	var aggregateSubscribed []string
	for _, subnet := range aggSubnets {
		aggregateDialed = append(aggregateDialed, dialTwo(subnet)...)
		if subnet != own { // a stable member already subscribes (Prepare); it must not drop that
			aggregateSubscribed = append(aggregateSubscribed, validator.FinalityVoteTopic(subnet))
		}
	}

	// Register the state BEFORE the network ops: the previous finality slot's teardown (its
	// aggregation deadline) can land on this very instant (k=2 at 50% puts it on the pre-join
	// slot), and dropFinality spares whatever a live state lists — in either firing order the
	// idempotent Join/Subscribe below then leaves this slot warmed up.
	fs := &finalityState{duties: duties, ownSubnet: own, voteDialed: voteDialed,
		aggregateSubnets: aggSubnets, aggregateSubscribed: aggregateSubscribed,
		aggregateDialed: aggregateDialed}
	r.mu.Lock()
	r.finals[n] = fs
	r.mu.Unlock()

	for _, subnet := range dutySubnets {
		if err := r.nd.Join(validator.FinalityVoteTopic(subnet)); err != nil {
			slog.Error("join finality duty subnet failed", "node", r.num, "subnet", subnet, "err", err)
		}
	}
	r.nd.Dial(slices.Concat(voteDialed, aggregateDialed)) // synchronous: up before the boundary
	for _, topic := range aggregateSubscribed {
		if err := r.nd.Subscribe(topic); err != nil {
			slog.Error("subscribe aggregation subnet failed", "node", r.num, "topic", topic, "err", err)
		}
	}
}

// shuffledFinalitySubscribers returns finality subnet's members in a seeded order (so the pre-join
// dials are reproducible across runs/backends), keyed by (seed, round key, subnet) — the round
// key is the finality slot in base mode, the AC slot under segregation.
func (r *NodeRunner) shuffledFinalitySubscribers(n, subnet int) []int {
	order := slices.Clone(r.view.FinalitySubscribers[subnet])
	rng := rand.New(rand.NewPCG(r.seed, finalityDialStream|uint64(n)<<32|uint64(subnet)))
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}

// validatorsPerCell is the voting population behind one aggregate, carried in schedule.json —
// it sizes the population-scaled aggregate. Base mode: the subnet's whole per-validator draw
// count (n, the finality slot, is unused). Segregated: the (round, subnet) cell of the two
// independent draws, round = n % k (n is the AC slot). 0 if out of range.
func (r *NodeRunner) validatorsPerCell(n, subnet int) int {
	counts := r.view.ValidatorsPerSubnet
	if r.segregated {
		round := n % r.finalityRoundSize
		perRound := r.view.ValidatorsPerRoundSubnet
		if round < 0 || round >= len(perRound) {
			return 0
		}
		counts = perRound[round]
	}
	if subnet < 0 || subnet >= len(counts) {
		return 0
	}
	return counts[subnet]
}

// emitFinalityVotes publishes one finality vote per validator this node hosts, each on ITS
// validator's drawn subnet, at most once per finality slot. The vote is fixed-time and
// dissemination-only this cut (un-gated, no fork-choice outcome), so OnPublish records no vote.
// The node's own subnet is subscribed in Prepare; foreign duty subnets were Joined + dialed at
// the pre-join slot, so the non-member publish lands via gossipsub fan-out.
func (r *NodeRunner) emitFinalityVotes(n int, fs *finalityState) {
	fs.voteOnce.Do(func() {
		at := time.Now()
		if r.partial {
			r.emitFinalityVotesPartial(n, fs, at)
			return
		}
		for _, d := range fs.duties {
			msg := validator.MakeFinalityVote(n, d.Subnet, d.Val, r.num)
			r.tracer.OnPublish(metrics.FinalityVoteID(n, d.Subnet, d.Val, r.num), false, at)
			err := r.nd.Publish(r.runCtx, validator.FinalityVoteTopic(d.Subnet), msg.Payload)
			if err != nil {
				slog.Error("publish finality vote failed",
					"node", r.num, "finality_slot", n, "val", d.Val, "err", err)
			}
		}
	})
}

// emitFinalityVotesPartial is the FC burst's partial twin: the own-subnet duties publishLocal
// (the stable membership Prepare subscribed); foreign duty subnets — pre-joined + dialed by
// prejoinFinality — get one eager fanout batch each. Finality votes are vote-free this cut, so
// each (subnet, group) has exactly one bucket (voted_origin 0). The group key is n, exactly
// what FinalityVoteID rides — fslot in base mode, the AC slot under segregation.
func (r *NodeRunner) emitFinalityVotesPartial(n int, fs *finalityState, at time.Time) {
	sig := validator.MakePartialSignature(r.sigSize)
	fanout := map[int][]int{} // foreign duty subnet → positions
	for _, d := range fs.duties {
		r.tracer.OnPublish(metrics.FinalityVoteID(n, d.Subnet, d.Val, r.num), false, at)
		if d.Subnet == fs.ownSubnet {
			data := validator.MakePartialAttData(n, d.Subnet, 0, r.attDataSize)
			r.nd.PublishLocalPartial(validator.FinalityVoteTopic(d.Subnet), n, d.Position, sig, data)
			continue
		}
		fanout[d.Subnet] = append(fanout[d.Subnet], d.Position)
	}
	for subnet, positions := range fanout {
		data := validator.MakePartialAttData(n, subnet, 0, r.attDataSize)
		sigs := make([][]byte, len(positions))
		for i := range sigs {
			sigs[i] = sig
		}
		if err := r.nd.FanoutPartial(validator.FinalityVoteTopic(subnet), n, positions, sigs, data); err != nil {
			slog.Error("partial finality fanout failed",
				"node", r.num, "finality_slot", n, "subnet", subnet, "err", err)
		}
	}
}

// emitFinalityAggregate publishes one population-scaled aggregate per aggregated subnet on the
// global topic, at most once per round. The aggregate's size derives from the schedule's
// population counts (the subnet draw in base mode, the (round, subnet) cell under segregation),
// not from collected votes — this cut is dissemination-only (the votes' fork-choice content is a
// planned extension), so it is fixed-time. n is the round key: the finality slot in base mode, the
// AC slot under segregation. Teardown of the pre-join state is the separate dropTimer, not here.
func (r *NodeRunner) emitFinalityAggregate(n int, fs *finalityState) {
	fs.aggregateOnce.Do(func() {
		at := time.Now()
		for _, subnet := range fs.aggregateSubnets {
			msg := validator.MakeFinalityAggregate(n, subnet, r.num, r.validatorsPerCell(n, subnet))
			r.tracer.OnPublish(metrics.FinalityAggregateID(n, subnet, r.num), false, at)
			if err := r.nd.Publish(r.runCtx, validator.FinalityAggregateTopic, msg.Payload); err != nil {
				slog.Error("publish finality aggregate failed",
					"node", r.num, "finality_slot", n, "subnet", subnet, "err", err)
			}
		}
	})
}

// hasPrejoinState reports whether prejoinFinality set up any network state for this round (fan-out
// dials, aggregation dials, or foreign aggregation subscribes) that dropFinality must release.
func (fs *finalityState) hasPrejoinState() bool {
	return len(fs.voteDialed) > 0 || len(fs.aggregateDialed) > 0 || len(fs.aggregateSubscribed) > 0
}

// dropFinality tears down a finality slot's pre-join state: unsubscribes the aggregation-only
// (foreign) subnets and disconnects both dial sets — sparing whatever another LIVE finality
// state still lists. The sparing matters because the deadline can coincide with the next
// finality slot's pre-join (k=2 at 50% lands exactly on it), and a node aggregating the same
// subnet — or dialing the same peers — twice in a row must not lose what it just set up. Fired by
// the dropTimer at the aggregation deadline; reapFinality is the fallback when no dropTimer was
// armed (agg phase off).
func (r *NodeRunner) dropFinality(fs *finalityState) {
	fs.dropOnce.Do(func() {
		keepTopic, keepPeer := map[string]bool{}, map[int]bool{}
		r.mu.Lock()
		for _, live := range r.finals {
			if live == fs {
				continue
			}
			for _, topic := range live.aggregateSubscribed {
				keepTopic[topic] = true
			}
			for _, p := range slices.Concat(live.voteDialed, live.aggregateDialed) {
				keepPeer[p] = true
			}
		}
		r.mu.Unlock()
		for _, topic := range fs.aggregateSubscribed {
			if !keepTopic[topic] {
				r.nd.Unsubscribe(topic)
			}
		}
		var drop []int
		for _, p := range slices.Concat(fs.voteDialed, fs.aggregateDialed) {
			if !keepPeer[p] {
				drop = append(drop, p)
			}
		}
		r.nd.Disconnect(drop)
	})
}

// reapFinality prunes finality state at the round's last AC slot: in base mode the finality
// slot's ((n+1)*k − 1, i.e. slot where (slot+1)%k == 0), under segregation every slot (a round
// lives in its own slot; finals[slot+1], pre-joined this slot, survives — only finals[slot] is
// deleted). By then both the vote timer (fcVoteOffset in) and the aggregation deadline (< the
// round's end in both modes) have fired, so the stops and the teardown are defensive (the
// teardown is load-bearing only when no deadline timer was armed). This is the only place
// r.finals is pruned — in base mode it deliberately outlives the per-AC-slot endSlot pruning of
// r.slots.
func (r *NodeRunner) reapFinality(slot int) {
	key := slot
	if !r.segregated {
		if (slot+1)%r.finalityRoundSize != 0 {
			return
		}
		key = slot / r.finalityRoundSize
	}
	r.mu.Lock()
	fs := r.finals[key]
	delete(r.finals, key)
	r.mu.Unlock()
	if r.partial { // the reaped round's partial buckets go with it (vote burst long settled)
		r.nd.PrunePartial(node.KindFinalityVote, key)
	}
	if fs == nil {
		return
	}
	if fs.voteTimer != nil {
		fs.voteTimer.Stop()
	}
	if fs.aggregateTimer != nil {
		fs.aggregateTimer.Stop()
	}
	if fs.dropTimer != nil {
		fs.dropTimer.Stop()
	}
	if fs.sealTimer != nil {
		fs.sealTimer.Stop()
	}
	r.dropFinality(fs)
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
	if r.view.NumColumns > 0 {
		for col := range r.view.NumColumns {
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

// onReceive routes a decoded receipt: skip the node's own loopback (Received.Origin is the
// publisher for every kind, even where the identity drops it), record the arrival, and feed
// blocks and columns into the coupling.
func (r *NodeRunner) onReceive(rec node.Received) {
	if rec.Origin == r.num {
		return
	}
	r.tracer.OnReceive(r.num, metrics.MsgID{Kind: rec.Kind, Identity: rec.ID}, rec.At)
	switch rec.Kind {
	case node.KindBlock:
		r.onBlockProcessed(rec.ID.Slot, rec.ID.Origin, rec.At)
	case node.KindColumn:
		r.onColumnProcessed(rec.ID.Slot, rec.ID.Subnet, rec.At) // the column index rides Subnet
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
	if !ss.blockSeen {
		ss.blockSeen, ss.blockSeenAt, ss.blockSeenOrigin = true, at, origin
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

// tryEarlyEmit attempts this node's early block vote. The gate: vote block only once the block is
// processed AND all custody columns are in, emitted at max(block, columns_complete)+Δ_prep and only
// if that lands by the deadline. If so it claims the slot's vote here, atomically — so the deadline
// just votes prior head when nothing was claimed. Called on every block/column event; the first one
// that finds the node ready-in-time wins the claim, the rest no-op.
func (r *NodeRunner) tryEarlyEmit(slot int, ss *slotState) {
	if !r.attest && !r.decoupled { // a sync-only run has a slotState but no column-gated vote
		return
	}
	r.mu.Lock()
	if ss.alreadyVoted { // claimed by an earlier event or the deadline
		r.mu.Unlock()
		return
	}
	ready := ss.blockSeen && ss.columnsComplete
	processed := laterOf(ss.blockSeenAt, ss.columnsCompleteAt)
	emitAt, voteBlock := emitDecision(ready, processed, ss.deadline, r.prep)
	if !voteBlock { // not ready in time — leave the vote for the deadline (prior head)
		r.mu.Unlock()
		return
	}
	ss.alreadyVoted, ss.votedForSlotBlock = true, true
	r.mu.Unlock()

	if d := time.Until(emitAt); d > 0 { // honor Δ_prep before emitting
		time.AfterFunc(d, func() { r.emitGated(slot, ss) })
	} else {
		go r.emitGated(slot, ss) // off the receive loop: emit may await the dials
	}
}

// emitGated dispatches the claimed vote to the active phase's emitter: the AC vote (one global
// topic) under decoupled, else the per-subnet attestation. The decision (block vs prior head) was
// recorded in ss by the claim, so the emitter reads it; attest and decoupled are exclusive.
func (r *NodeRunner) emitGated(slot int, ss *slotState) {
	if r.decoupled {
		r.emitACVote(slot, ss)
		return
	}
	r.emit(slot, ss)
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
	if ss.syncAlreadyVoted {
		r.mu.Unlock()
		return
	}
	emitAt, voteHead := emitDecision(ss.blockSeen, ss.blockSeenAt, ss.deadline, r.prep)
	if !voteHead { // not in time — leave the sync vote for the deadline (prior head)
		r.mu.Unlock()
		return
	}
	ss.syncAlreadyVoted, ss.syncVotedForSlotBlock = true, true
	r.mu.Unlock()

	if d := time.Until(emitAt); d > 0 { // honor Δ_prep before emitting
		time.AfterFunc(d, func() { r.emitSyncMessage(slot, ss) })
	} else {
		r.emitSyncMessage(slot, ss)
	}
}

// onBlockDeadline casts this slot's vote for the prior head if the early path never claimed a block
// vote — the column phase's measurable effect: a node with the block but a missing custody column
// reaches here unclaimed and votes prior. If the early path already claimed (block), this no-ops.
// The claim is atomic (under r.mu), so the deadline and the early path never both emit.
func (r *NodeRunner) onBlockDeadline(slot int, ss *slotState) {
	if r.attest || r.decoupled {
		r.mu.Lock()
		voteParent := !ss.alreadyVoted
		if voteParent { // claim the prior-head vote; leave an early block claim untouched
			ss.alreadyVoted, ss.votedForSlotBlock = true, false
		}
		r.mu.Unlock()
		if voteParent {
			r.emitGated(slot, ss)
		}
	}
	if r.sync && ss.syncMember { // sync votes head on block-seen alone (un-gated by columns)
		r.mu.Lock()
		voteParent := !ss.syncAlreadyVoted
		if voteParent {
			ss.syncAlreadyVoted, ss.syncVotedForSlotBlock = true, false
		}
		r.mu.Unlock()
		if voteParent {
			r.emitSyncMessage(slot, ss)
		}
	}
}

// laterOf returns the later of two times (the gate emits at max(block, columns_complete)).
func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// emit publishes one attestation per duty. The claim (tryEarlyEmit / onBlockDeadline) already
// guaranteed it runs at most once, so there's no emit-once guard here — it reads the recorded
// decision: votedForSlotBlock ⇒ the block's origin, else -1 for the prior head.
func (r *NodeRunner) emit(slot int, ss *slotState) {
	r.awaitDials(ss) // the fan-out needs the warm-up dials up (see setupSlot/dialDuties)
	at := time.Now()
	votedBlock := ss.votedForSlotBlock
	votedOrigin := r.votedOrigin(ss)
	if r.partial {
		r.emitPartial(slot, ss, votedOrigin, votedBlock, at)
		return
	}
	for _, d := range ss.attestationDuties {
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
}

// votedOrigin maps the recorded attestation/AC vote decision to a wire origin: the block's origin
// when the slot voted block, else -1 (the prior head).
func (r *NodeRunner) votedOrigin(ss *slotState) int {
	if ss.votedForSlotBlock {
		return ss.blockSeenOrigin
	}
	return -1
}

// emitPartial is emit's partial-transport twin: identical instants and OnPublish records, but
// the votes enter the partial manager — publishLocal on subscribed duty subnets (the tick loop
// pushes them to the mesh) and ONE eager fanout batch per non-member duty subnet (dialDuties'
// Join + dial-2 warmup already gave the fanout somewhere to land). All of one emit's duties
// share votedOrigin, so each subnet has exactly one vote bucket here; the fork against other
// nodes' differing votes lives in the attestation_data bytes.
func (r *NodeRunner) emitPartial(slot int, ss *slotState, votedOrigin int, votedBlock bool, at time.Time) {
	sig := validator.MakePartialSignature(r.sigSize)
	fanout := map[int][]int{} // non-member duty subnet → positions
	for _, d := range ss.attestationDuties {
		r.tracer.OnPublish(metrics.AttestID(slot, d.Subnet, d.Val, r.num), votedBlock, at)
		if ss.attestationSubnetsSubscribed[d.Subnet] {
			data := validator.MakePartialAttData(slot, d.Subnet, votedOrigin, r.attDataSize)
			r.nd.PublishLocalPartial(validator.AttestationTopic(d.Subnet), slot, d.Position, sig, data)
			continue
		}
		fanout[d.Subnet] = append(fanout[d.Subnet], d.Position)
	}
	for subnet, positions := range fanout {
		data := validator.MakePartialAttData(slot, subnet, votedOrigin, r.attDataSize)
		sigs := make([][]byte, len(positions))
		for i := range sigs {
			sigs[i] = sig
		}
		if err := r.nd.FanoutPartial(validator.AttestationTopic(subnet), slot, positions, sigs, data); err != nil {
			slog.Error("partial fanout failed", "node", r.num, "slot", slot, "subnet", subnet, "err", err)
		}
	}
}

// emitACVote publishes one AC vote per duty on the single global availability-vote topic — the
// no-aggregation, no-subnet twin of emit (the AC vote is the column-gated attestation, retargeted).
// The claim guaranteed a single emission; it reads the recorded decision (see votedOrigin).
func (r *NodeRunner) emitACVote(slot int, ss *slotState) {
	at := time.Now()
	votedBlock := ss.votedForSlotBlock
	votedOrigin := r.votedOrigin(ss)
	for _, d := range ss.acVoteDuties {
		msg := validator.MakeACVote(slot, d.Val, r.num, votedOrigin)
		r.tracer.OnPublish(metrics.ACVoteID(slot, d.Val, r.num), votedBlock, at)
		if err := r.nd.Publish(r.runCtx, validator.AvailabilityVoteTopic, msg.Payload); err != nil {
			slog.Error("publish AC vote failed", "node", r.num, "slot", slot, "val", d.Val, "err", err)
		}
	}
}

// emitSyncMessage publishes this member's one sync-committee message on its subnet. The claim
// (trySyncEmit / onBlockDeadline) already guaranteed a single emission, so there's no guard here;
// it reads syncVotedForSlotBlock — head ⇒ the block's origin, else -1 for the prior head.
func (r *NodeRunner) emitSyncMessage(slot int, ss *slotState) {
	voteHead := ss.syncVotedForSlotBlock
	votedOrigin := -1
	if voteHead {
		votedOrigin = ss.blockSeenOrigin
	}
	at := time.Now()
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
