// Package driver is the orchestration half of the simulator: it owns the slot
// clock, builds the passive Nodes over a Fabric, and runs one stateful NodeRunner
// per node. The runner consults the schedule for its duties (proposing, attesting,
// custody, ...), issues publishes, and is the node's OnReceive sink — so it sees
// both the block arriving and the duty firing, the seam the block→attestation
// coupling needs. Node stays pure. The single-node Shadow binary builds one
// NodeRunner directly, without the multi-node Driver.
package driver

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// Fabric is the per-node network substrate the Driver builds on: it resolves
// peer addresses (node.Network) and exposes each node's host and placeholder
// peer list. *netsim.Netsim implements it; the Shadow binary instead drives a
// single real host directly through a NodeRunner.
type Fabric interface {
	node.Network
	Len() int
	Host(i int) host.Host
	Peers(i int) []int
}

// Config parameterizes the nodes and validators a Driver builds.
type Config struct {
	BlockSize    int
	SlotDuration time.Duration
	Offset       time.Duration
	Jitter       time.Duration
	VerifyDelay  func() time.Duration
	D, Dlo, Dhi  int
	Seed         uint64 // per-node validator rng seed

	// Attestation knobs (optional). Schedule nil ⇒ block-only (Phase 1). Attest false
	// with a Schedule set ⇒ block-only too, but the Schedule's proposer schedule still
	// applies (so block dissemination is measured on the same network, sans attestations).
	Schedule          *schedule.Assignment
	Attest            bool          // emit attestations (requires Schedule)
	Sync              bool          // emit sync-committee messages + contributions (requires Schedule)
	AttestationDue    time.Duration // emit deadline as an offset into the slot
	Prep              time.Duration // Δ_prep before emitting on block receipt
	AttestVerifyDelay func() time.Duration
	AttestPerItem     time.Duration
	AttestBatchWindow time.Duration
	AttestBatchMax    int // max attestations per verify batch; 0 = uncapped

	// The aggregation deadline (≈8s, 2/3 of the slot): committee aggregators publish their
	// aggregates and sync aggregators their contributions here. Whether anyone aggregates is
	// the schedule's business (its aggregator draws), not this knob's.
	AggregateDue time.Duration

	// Decoupled-consensus phase (optional; nil ⇒ off). When set, the runner emits AC votes +
	// finality votes/aggregates and attestation/sync emit is forced off (the AC vote replaces
	// attestations). The AC-vote deadline reuses AttestationDue; columns gate it.
	Decoupled *DecoupledParams

	// Partial transport (optional; nil ⇒ classic). Switches the attestation-class floods
	// (attestations, and finality votes when Decoupled) to the gossipsub partial-messages
	// extension on the same schedule — a transport, not a phase: it requires Attest or
	// Decoupled. See partial-attestation-spec.md.
	Partial *PartialParams

	// ePBS two-phase block send (optional; nil ⇒ the legacy single Block). Composes with
	// every phase; see EPBSParams.
	EPBS *EPBSParams

	// Data-columns phase (optional; active when Schedule.NumColumns > 0). The proposer bursts
	// one DataColumnSidecar per column subnet at t=0; each node verifies columns through a
	// width-P semaphore, P sized from its full-custody role.
	ColVerifyService          func() time.Duration // per-column validation-as-sleep
	ColVerifyParallelismSuper int                  // P for a full-custody node (16)
	ColVerifyParallelismReg   int                  // P for an ordinary node (4)
}

// Driver builds and orchestrates the nodes on a Fabric.
type Driver struct {
	nodes   []*node.Node
	runners []*NodeRunner
	tracer  metrics.Tracer
	nw      Fabric
	slotDur time.Duration
}

// New builds one passive Node + one NodeRunner per host on nw, wiring each
// runner as its node's OnReceive sink.
func New(nw Fabric, cfg Config, tracer metrics.Tracer) *Driver {
	n := nw.Len()
	d := &Driver{
		nodes:   make([]*node.Node, n),
		runners: make([]*NodeRunner, n),
		tracer:  tracer,
		nw:      nw,
		slotDur: cfg.SlotDuration,
	}
	// One read-only resolver serves the whole fleet (NewRunner enforces the phase requirement).
	var resolver node.PartialResolver
	if cfg.Partial != nil {
		resolver = NewPartialResolver(cfg.Schedule)
	}
	// The committee drives attestations (when cfg.Attest) and/or columns (NumColumns > 0); the
	// runner's attest flag gates attestation emission, so a committee can disseminate columns
	// without emitting attestations. Block-only runs pass a nil Schedule.
	rcfg := RunnerConfig{
		Attest: cfg.Attest, Sync: cfg.Sync,
		NumNodes: n, BlockSize: cfg.BlockSize, Offset: cfg.Offset, Jitter: cfg.Jitter,
		SlotDuration: cfg.SlotDuration, AttestationDue: cfg.AttestationDue,
		AggregateDue: cfg.AggregateDue, Prep: cfg.Prep, Seed: cfg.Seed,
		Decoupled: cfg.Decoupled, Partial: cfg.Partial, EPBS: cfg.EPBS,
	}
	// Decoupled consensus replaces attestations + sync, so force both emit flags off when it's on
	// (the runner's mutual-exclusion invariant: a slot emits an AC vote OR an attestation, never both).
	if cfg.Decoupled != nil {
		rcfg.Attest, rcfg.Sync = false, false
	}
	for i := range n {
		nd := &node.Node{
			Num: i, Host: nw.Host(i), Network: nw,
			VerifyDelay:       cfg.VerifyDelay,
			AttestVerifyDelay: cfg.AttestVerifyDelay,
			AttestPerItem:     cfg.AttestPerItem,
			AttestBatchWindow: cfg.AttestBatchWindow,
			AttestBatchMax:    cfg.AttestBatchMax,
			D:                 cfg.D, Dlo: cfg.Dlo, Dhi: cfg.Dhi,
		}
		var view schedule.View // zero ⇒ block-only
		if cfg.Schedule != nil {
			view = cfg.Schedule.Node(i)
		}
		// Size the column verifier from the node's full-custody role (custody applies even when
		// attestations are off, so it's gated on the plan's columns, not cfg.Attest).
		if view.NumColumns > 0 {
			nd.ColVerifyService = cfg.ColVerifyService
			if view.FullCustody {
				nd.ColVerifyParallelism = cfg.ColVerifyParallelismSuper
			} else {
				nd.ColVerifyParallelism = cfg.ColVerifyParallelismReg
			}
		}
		if cfg.Partial != nil {
			nd.Partial = cfg.Partial.NodeOpts(cfg.Seed, resolver)
		}
		r := NewRunner(nd, view, nw.Peers(i), tracer, rcfg)
		r.Attach()
		d.nodes[i] = nd
		d.runners[i] = r
	}
	return d
}

// BringUp runs the start-up cadence: Start every node, settle, dial each node's base
// peers, join topics (which starts the receive loops), subscribe each node's own subnets,
// settle again so the meshes form before the first publish. The settle sleeps are
// load-bearing — publishing before the mesh forms flakes.
func (d *Driver) BringUp(ctx context.Context) error {
	for _, nd := range d.nodes {
		if err := nd.Start(ctx); err != nil {
			return err
		}
	}
	time.Sleep(time.Second)
	for _, nd := range d.nodes {
		nd.ConnectToPeers(d.nw.Peers(nd.Num))
	}
	for _, nd := range d.nodes {
		if err := nd.JoinTopics(ctx); err != nil {
			return err
		}
	}
	for _, r := range d.runners {
		r.Prepare()
	}
	time.Sleep(time.Second) // let the block + subnet meshes form
	return nil
}

// Run executes numSlots slots from runStart (shared by all nodes so arrival times
// share an origin) by running each node's NodeRunner concurrently. It returns once
// the run plus a drain window completes, then stops the receive loops.
func (d *Driver) Run(ctx context.Context, runStart time.Time, numSlots int) {
	var wg sync.WaitGroup
	for _, r := range d.runners {
		wg.Go(func() { r.Run(ctx, runStart, numSlots) })
	}
	wg.Wait()
	time.Sleep(d.slotDur) // drain in-flight receives
	for _, nd := range d.nodes {
		nd.Close()
	}
}
