package node_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/validator"
)

// buildNodes wires count Node objects to a netsim, each running one Validator.
func buildNodes(nw *netsim.Netsim, n int, slotDur time.Duration, rec metrics.Tracer) []*node.Node {
	nodes := make([]*node.Node, n)
	for i := range n {
		v := validator.New(i, n, 128*1024, 0, time.Second, rand.New(rand.NewPCG(uint64(i), 99)))
		nodes[i] = &node.Node{
			Num:          i,
			Host:         nw.Host(i),
			Network:      nw,
			Validator:    v,
			Tracer:       rec,
			SlotDuration: slotDur,
			VerifyDelay:  func() time.Duration { return 10 * time.Millisecond },
			D:            8, Dlo: 6, Dhi: 12,
		}
	}
	return nodes
}

// bringUp runs the proven cadence: Start all -> settle -> connect -> join -> settle.
func bringUp(t *testing.T, ctx context.Context, nodes []*node.Node, nw *netsim.Netsim) {
	t.Helper()
	for _, n := range nodes {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("start %d: %v", n.Num, err)
		}
	}
	time.Sleep(time.Second)
	for _, n := range nodes {
		n.ConnectToPeers(nw.Peers(n.Num))
	}
	for _, n := range nodes {
		if err := n.JoinTopics(); err != nil {
			t.Fatalf("join %d: %v", n.Num, err)
		}
	}
	time.Sleep(time.Second)
}

// Milestone 1: 2 nodes, one block A->B, arrival recorded exactly once.
func TestTwoNodesOneBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const slotDur = time.Second
		nw, err := netsim.New(netsim.Config{N: 2, P: 1, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		rec := metrics.NewRecorder()
		nodes := buildNodes(nw, 2, slotDur, rec)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		bringUp(t, ctx, nodes, nw)

		// Run one slot: only node 0 proposes (slot 0 % 2 == 0).
		runStart := time.Now()
		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() { n.Run(ctx, runStart, 1) })
		}
		wg.Wait()

		arr := rec.Arrivals()
		if len(arr) != 1 {
			t.Fatalf("got %d arrivals, want exactly 1 (node1 <- node0): %+v", len(arr), arr)
		}
		a := arr[0]
		if a.Node != 1 || a.Origin != 0 || a.Slot != 0 {
			t.Fatalf("arrival = %+v, want node1 origin0 slot0", a)
		}
		if a.Delay <= 0 {
			t.Fatalf("delay = %v, want > 0", a.Delay)
		}
	})
}
