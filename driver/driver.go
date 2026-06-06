// Package driver is the orchestration half of the simulator: it owns the slot
// clock, builds the passive Nodes over a netsim, consults each node's Validator
// for duties, issues publish requests at their offsets, and routes received
// messages into the metrics Tracer. The Node and Validator know nothing about
// timing or metrics — the Driver wires them together.
package driver

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

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

// Driver builds and orchestrates the nodes on a netsim.
type Driver struct {
	nodes   []*node.Node
	vals    []*validator.Validator
	tracer  metrics.Tracer
	nw      *netsim.Netsim
	slotDur time.Duration
}

// New builds one passive Node and one Validator per host on nw, wiring each
// node's receipts back into the Driver.
func New(nw *netsim.Netsim, cfg Config, tracer metrics.Tracer) *Driver {
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
		nd.OnReceive = func(r node.Received) { d.onReceive(i, r) }
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
// times share an origin), publishing each slot's duties at their offset. It
// returns once the run plus a drain window completes, then stops the receive
// loops.
func (d *Driver) Run(ctx context.Context, runStart time.Time, numSlots int) {
	for slot := range numSlots {
		slotStart := runStart.Add(time.Duration(slot) * d.slotDur)
		for i, v := range d.vals {
			for _, duty := range v.Duties(slot) {
				go d.publish(ctx, slotStart.Add(duty.At), i, duty.Msg)
			}
		}
		time.Sleep(time.Until(slotStart.Add(d.slotDur)))
	}
	time.Sleep(d.slotDur) // drain in-flight receives
	for _, nd := range d.nodes {
		nd.Close()
	}
}

// publish records the publish time (synchronously, before the send) then asks
// node origin to publish the payload at time when.
func (d *Driver) publish(ctx context.Context, when time.Time, origin int, msg validator.Message) {
	time.Sleep(time.Until(when))
	d.tracer.OnPublish(msg.Slot, origin, time.Now())
	if err := d.nodes[origin].Publish(ctx, msg.Topic, msg.Payload); err != nil {
		slog.Error("publish failed", "node", origin, "slot", msg.Slot, "err", err)
	}
}

// onReceive routes a node's decoded receipt into the Tracer, skipping the node's
// own loopback publish (origin == self) — there is no clean origin in gossipsub,
// so we compare the app-level Block.Origin.
func (d *Driver) onReceive(num int, r node.Received) {
	switch r.Kind {
	case node.KindBlock:
		blk := r.Obj.(*pb.Block)
		if int(blk.Origin) == num {
			return
		}
		d.tracer.OnReceive(num, int(blk.Slot), int(blk.Origin), r.At)
	}
}
