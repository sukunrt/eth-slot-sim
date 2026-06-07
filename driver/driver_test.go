package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
)

// A block larger than gossipsub's default 1 MiB max message size must still
// disseminate. A 1 MiB block plus its pubsub envelope exceeds the default, so
// without node.go's WithMaxMessageSize every receiver silently drops it. This is
// a message-size limit, not a scale effect, so it reproduces at N=5: pre-fix this
// gets 0 of 4 arrivals.
func TestLargeBlockDisseminates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			n         = 5
			slotDur   = 12 * time.Second
			numSlots  = 1
			blockSize = 1024 * 1024 // 1 MiB — over the pubsub 1 MiB default once enveloped
		)
		nw, err := netsim.New(netsim.Config{
			N: n, P: 4, SuperFrac: 0.0, Seed: 1,
			MinLatency: 10 * time.Millisecond, MaxLatency: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		rec := metrics.NewRecorder()
		d := driver.New(nw, driver.Config{
			BlockSize: blockSize, SlotDuration: slotDur, Jitter: time.Second,
			VerifyDelay: func() time.Duration { return 0 },
			D:           8, Dlo: 6, Dhi: 12, Seed: 99,
		}, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx, numSlots); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), numSlots)

		if want := numSlots * (n - 1); len(rec.Arrivals()) != want {
			t.Fatalf("1 MiB block: got %d arrivals, want %d — large block not disseminating "+
				"(gossipsub WithMaxMessageSize)", len(rec.Arrivals()), want)
		}
	})
}

// N nodes, cyclic proposer, multi-slot run driven by the Driver. Every
// non-proposer receives each block exactly once, and the arrival spread is
// multi-hop.
func TestNNodesCyclicDissemination(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this N-node synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		const (
			n        = 32
			slotDur  = 12 * time.Second
			numSlots = 5
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
		d := driver.New(nw, driver.Config{
			BlockSize: 16 * 1024, SlotDuration: slotDur, Jitter: time.Second,
			VerifyDelay: func() time.Duration { return 10 * time.Millisecond },
			D:           8, Dlo: 6, Dhi: 12, Seed: 99,
		}, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx, numSlots); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), numSlots)

		arr := rec.Arrivals()

		// (a) Every non-proposer receives each block exactly once: full coverage
		// (numSlots*(n-1) arrivals) with no duplicate (node,slot,origin).
		type key struct{ node, slot, origin int }
		seen := make(map[key]bool, len(arr))
		for _, a := range arr {
			k := key{a.Node, a.ID.Slot, a.ID.Origin}
			if seen[k] {
				t.Fatalf("duplicate arrival %+v", a)
			}
			seen[k] = true
			if a.ID.Origin != a.ID.Slot%n {
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
			if a.ID.Slot != slot {
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
