// Package driver is the orchestration half of the simulator: it owns the slot
// clock, builds the passive Nodes over a Fabric, and runs one stateful NodeRunner
// per node. The runner consults the node's Validator for block duties and its
// committee slice for attest duties, issues publishes, and is the node's OnReceive
// sink — so it sees both the block arriving and the duty firing, the seam the
// block→attestation coupling needs. Node and Validator stay pure. The single-node
// Shadow binary builds one NodeRunner directly, without the multi-node Driver.
package driver

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/validator"
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

	// Attestation knobs (optional). Committee nil ⇒ block-only (Phase 1).
	Committee         *committee.Assignment
	AttestationDue    time.Duration // emit deadline as an offset into the slot
	Prep              time.Duration // Δ_prep before emitting on block receipt
	AttestVerifyDelay func() time.Duration
	AttestPerItem     time.Duration
	AttestBatchWindow time.Duration
}

// Driver builds and orchestrates the nodes on a Fabric.
type Driver struct {
	nodes   []*node.Node
	runners []*NodeRunner
	tracer  metrics.Tracer
	nw      Fabric
	slotDur time.Duration
}

// New builds one passive Node + one Validator + one NodeRunner per host on nw,
// wiring each runner as its node's OnReceive sink.
func New(nw Fabric, cfg Config, tracer metrics.Tracer) *Driver {
	n := nw.Len()
	d := &Driver{
		nodes:   make([]*node.Node, n),
		runners: make([]*NodeRunner, n),
		tracer:  tracer,
		nw:      nw,
		slotDur: cfg.SlotDuration,
	}
	for i := range n {
		val := validator.New(i, n, cfg.BlockSize, cfg.Offset, cfg.Jitter,
			rand.New(rand.NewPCG(cfg.Seed, uint64(i))))
		nd := &node.Node{
			Num: i, Host: nw.Host(i), Network: nw,
			VerifyDelay:       cfg.VerifyDelay,
			AttestVerifyDelay: cfg.AttestVerifyDelay,
			AttestPerItem:     cfg.AttestPerItem,
			AttestBatchWindow: cfg.AttestBatchWindow,
			D:                 cfg.D, Dlo: cfg.Dlo, Dhi: cfg.Dhi,
		}
		r := NewRunner(i, nd, val, cfg.Committee, tracer, cfg.SlotDuration, cfg.AttestationDue, cfg.Prep)
		r.Attach()
		d.nodes[i] = nd
		d.runners[i] = r
	}
	return d
}

// BringUp runs the start-up cadence: Start every node, settle, dial each node's
// peers, join topics (which starts the receive loops), prepare attestation membership
// (backbone meshes + duty-subnet joins for numSlots), settle again so meshes form
// before the first publish. The settle sleeps are load-bearing — publishing before the
// mesh forms flakes.
func (d *Driver) BringUp(ctx context.Context, numSlots int) error {
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
		r.Prepare(numSlots)
	}
	time.Sleep(time.Second) // let block + backbone meshes form
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
