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
func buildNodes(nw *netsim.Netsim, n, blockSize int, slotDur time.Duration, rec metrics.Tracer) []*node.Node {
	nodes := make([]*node.Node, n)
	for i := range n {
		v := validator.New(i, n, blockSize, 0, time.Second, rand.New(rand.NewPCG(uint64(i), 99)))
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
		nodes := buildNodes(nw, 2, 128*1024, slotDur, rec)
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

// Milestone 3: N nodes, cyclic proposer, X=N*S run. Every non-proposer receives
// each block exactly once, and the arrival spread is multi-hop.
func TestNNodesCyclicDissemination(t *testing.T) {
	if testing.Short() {
		t.Skip("100-node full-slot dissemination is CPU-heavy; -short skips it")
	}
	synctest.Test(t, func(t *testing.T) {
		const (
			n        = 100
			slotDur  = 12 * time.Second
			numSlots = n // every node proposes once
		)
		nw, err := netsim.New(netsim.Config{
			N: n, P: 20, SuperFrac: 0.20, Seed: 1,
			MinLatency: 10 * time.Millisecond, MaxLatency: 150 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		rec := metrics.NewRecorder()
		nodes := buildNodes(nw, n, 16*1024, slotDur, rec)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		bringUp(t, ctx, nodes, nw)

		runStart := time.Now()
		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() { n.Run(ctx, runStart, numSlots) })
		}
		wg.Wait()

		arr := rec.Arrivals()

		// (a) Every non-proposer receives each block exactly once: full coverage
		// (numSlots*(n-1) arrivals) with no duplicate (node,slot,origin).
		type key struct{ node, slot, origin int }
		seen := make(map[key]bool, len(arr))
		for _, a := range arr {
			k := key{a.Node, a.Slot, a.Origin}
			if seen[k] {
				t.Fatalf("duplicate arrival %+v", a)
			}
			seen[k] = true
			if a.Origin != a.Slot%n {
				t.Fatalf("arrival %+v: origin != slot%%n", a)
			}
		}
		if want := numSlots * (n - 1); len(arr) != want {
			t.Fatalf("got %d arrivals, want %d (every non-proposer once per block)", len(arr), want)
		}

		// (b) Multi-hop: for a block from a regular (non-super) proposer, the
		// slowest arrival is >= 2x the fastest (a 1-hop world clusters tightly).
		slot := -1
		for s := range numSlots {
			if !nw.IsSupernode(s % n) {
				slot = s
				break
			}
		}
		if slot < 0 {
			t.Fatal("no regular proposer found")
		}
		var lo, hi time.Duration
		for _, a := range arr {
			if a.Slot != slot {
				continue
			}
			if lo == 0 || a.Delay < lo {
				lo = a.Delay
			}
			if a.Delay > hi {
				hi = a.Delay
			}
		}
		if hi < 2*lo {
			t.Fatalf("slot %d not multi-hop: max=%v < 2*min=%v", slot, hi, 2*lo)
		}
		t.Logf("slot %d spread: min=%v max=%v; total arrivals=%d", slot, lo, hi, len(arr))
	})
}
