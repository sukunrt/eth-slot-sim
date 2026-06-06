// Package driver is the orchestration half of the simulator: it owns the slot
// clock, builds the passive Nodes over a Fabric, consults each node's Validator
// for duties, issues publish requests at their offsets, and routes received
// messages into the metrics Tracer. The Node and Validator know nothing about
// timing or metrics — the Driver wires them together. The per-node steady-state
// logic (SlotLoop, RouteReceived) is exported so the single-node Shadow binary
// reuses it without the multi-node Driver.
package driver

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// Fabric is the per-node network substrate the Driver builds on: it resolves
// peer addresses (node.Network) and exposes each node's host and placeholder
// peer list. *netsim.Netsim implements it; the Shadow binary instead drives a
// single real host directly through SlotLoop/RouteReceived.
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
}

// Driver builds and orchestrates the nodes on a Fabric.
type Driver struct {
	nodes   []*node.Node
	vals    []*validator.Validator
	tracer  metrics.Tracer
	nw      Fabric
	slotDur time.Duration
}

// New builds one passive Node and one Validator per host on nw, wiring each
// node's receipts back into the tracer.
func New(nw Fabric, cfg Config, tracer metrics.Tracer) *Driver {
	n := nw.Len()
	d := &Driver{
		nodes:   make([]*node.Node, n),
		vals:    make([]*validator.Validator, n),
		tracer:  tracer,
		nw:      nw,
		slotDur: cfg.SlotDuration,
	}
	for i := range n {
		d.vals[i] = validator.New(i, n, cfg.BlockSize, cfg.Offset, cfg.Jitter,
			rand.New(rand.NewPCG(cfg.Seed, uint64(i))))
		d.nodes[i] = &node.Node{
			Num: i, Host: nw.Host(i), Network: nw,
			VerifyDelay: cfg.VerifyDelay,
			D:           cfg.D, Dlo: cfg.Dlo, Dhi: cfg.Dhi,
		}
	}
	for i, nd := range d.nodes {
		nd.OnReceive = func(r node.Received) { RouteReceived(i, r, d.tracer) }
	}
	return d
}

// BringUp runs the start-up cadence: Start every node, settle, dial each node's
// peers, join topics (which starts the receive loops), settle again so meshes
// form before the first publish. The settle sleeps are load-bearing — publishing
// before the mesh forms flakes.
func (d *Driver) BringUp(ctx context.Context) error {
	for _, nd := range d.nodes {
		if err := nd.Start(ctx); err != nil {
			return fmt.Errorf("start %d: %w", nd.Num, err)
		}
	}
	time.Sleep(time.Second)
	for _, nd := range d.nodes {
		nd.ConnectToPeers(d.nw.Peers(nd.Num))
	}
	for _, nd := range d.nodes {
		if err := nd.JoinTopics(ctx); err != nil {
			return fmt.Errorf("join %d: %w", nd.Num, err)
		}
	}
	time.Sleep(time.Second) // let meshes form
	return nil
}

// Run executes numSlots slots from runStart (shared by all nodes so arrival
// times share an origin) by running each node's SlotLoop concurrently. It
// returns once the run plus a drain window completes, then stops the receive
// loops.
func (d *Driver) Run(ctx context.Context, runStart time.Time, numSlots int) {
	var wg sync.WaitGroup
	for i := range d.nodes {
		nd, v := d.nodes[i], d.vals[i]
		wg.Go(func() { SlotLoop(ctx, nd, v, d.tracer, runStart, numSlots, d.slotDur) })
	}
	wg.Wait()
	time.Sleep(d.slotDur) // drain in-flight receives
	for _, nd := range d.nodes {
		nd.Close()
	}
}

// SlotLoop runs numSlots slots for a single node from runStart: each slot it
// publishes the node's own duties at their offsets, recording each publish time
// via the tracer. Shared by the simnet Driver (one goroutine per node) and the
// single-node Shadow binary.
func SlotLoop(ctx context.Context, nd *node.Node, v *validator.Validator, t metrics.Tracer,
	runStart time.Time, numSlots int, slotDur time.Duration) {
	for slot := range numSlots {
		slotStart := runStart.Add(time.Duration(slot) * slotDur)
		for _, duty := range v.Duties(slot) {
			go publishAt(ctx, nd, t, slotStart.Add(duty.At), duty.Msg)
		}
		time.Sleep(time.Until(slotStart.Add(slotDur)))
	}
}

// publishAt waits until when, records the publish time (synchronously, before
// the send), then asks the node to publish the payload.
func publishAt(ctx context.Context, nd *node.Node, t metrics.Tracer, when time.Time, msg validator.Message) {
	time.Sleep(time.Until(when))
	t.OnPublish(msg.Slot, nd.Num, time.Now())
	if err := nd.Publish(ctx, msg.Topic, msg.Payload); err != nil {
		slog.Error("publish failed", "node", nd.Num, "slot", msg.Slot, "err", err)
	}
}

// RouteReceived routes a node's decoded receipt into the tracer, skipping the
// node's own loopback publish (origin == num) — gossipsub has no clean origin,
// so we compare the app-level Block.Origin.
func RouteReceived(num int, r node.Received, t metrics.Tracer) {
	switch r.Kind {
	case node.KindBlock:
		blk := r.Obj.(*pb.Block)
		if int(blk.Origin) == num {
			return
		}
		t.OnReceive(num, int(blk.Slot), int(blk.Origin), r.At)
	}
}
