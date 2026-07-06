package driver_test

import (
	"context"
	"maps"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// The block proposer follows schedule.json's per-slot schedule (supernodes in a real run),
// not slot%n: over a multi-slot run every block is published by exactly that slot's scheduled
// proposer and by no other node. This drives the same validator/driver code the Shadow binary
// runs, so the two backends propose identically.
func TestBlockProposerFollowsSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		com := [][]schedule.AttesterRef{{
			{Node: 0, Val: 0, Subnet: 0, Position: 0},
			{Node: 1, Val: 1, Subnet: 0, Position: 1},
		}}
		// proposers 2 and 3 are neither attesters nor subscribers of subnet 0 — they only
		// publish the global block, so a pure cyclic rule (slot0->0, slot1->1) would differ.
		a := &schedule.Assignment{
			Params: schedule.Params{
				N: 4, V: 4, C: 1, Sc: 2, SubnetCount: 64,
				SubnetsPerNode: 1, SubscribeFloor: 2, NumSlots: 2,
			},
			SubnetSubscribers: [][]int{{0, 1}},
			Slots: []schedule.SlotPlan{
				{Slot: 0, Committees: com, SubnetOf: []int{0}, Proposer: 2},
				{Slot: 1, Committees: com, SubnetOf: []int{0}, Proposer: 3},
			},
		}
		tr := &timeTracer{}
		s := buildScenario(t, a, 4*time.Second, nil, tr, 3)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		runStart := s.run(t, ctx, 2)

		tr.mu.Lock()
		defer tr.mu.Unlock()
		got := map[int]int{} // slot -> origin
		for _, p := range tr.pubs {
			if p.id.Kind != node.KindBlock {
				continue
			}
			if prev, ok := got[p.id.Slot]; ok {
				t.Fatalf("slot %d published twice (origins %d and %d)", p.id.Slot, prev, p.id.Origin)
			}
			got[p.id.Slot] = p.id.Origin
			// The scenario's runners publish at offset 200ms with no jitter — exactly there.
			wantAt := runStart.Add(time.Duration(p.id.Slot)*s.slotDur + 200*time.Millisecond)
			if !p.at.Equal(wantAt) {
				t.Fatalf("slot %d block published at %v, want %v (slotStart+offset)", p.id.Slot, p.at, wantAt)
			}
		}
		if want := map[int]int{0: 2, 1: 3}; got[0] != want[0] || got[1] != want[1] || len(got) != 2 {
			t.Fatalf("block proposers = %v, want %v (schedule, not slot%%n)", got, want)
		}
	})
}

// With jitter on, every block publish lands in [slotStart+offset, slotStart+offset+jitter),
// and the draw is a pure function of (seed, node, slot): two identical drivers publish at
// identical instants (the per-slot setup goroutines must not affect it).
func TestBlockPublishJitterWindow(t *testing.T) {
	const offset, jitter = time.Second, 2 * time.Second
	run := func(t *testing.T) map[int]time.Duration { // slot -> publish offset into the slot
		t.Helper()
		var pubs map[int]time.Duration
		synctest.Test(t, func(t *testing.T) {
			nw, err := netsim.New(netsim.Config{
				N: 2, P: 1, Seed: 1,
				MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("netsim: %v", err)
			}
			t.Cleanup(nw.Close)
			tr := &timeTracer{}
			d := driver.New(nw, driver.Config{
				BlockSize: 1024, SlotDuration: 12 * time.Second, Offset: offset, Jitter: jitter,
				VerifyDelay: func() time.Duration { return 0 },
				D:           8, Dlo: 6, Dhi: 12, Seed: 1,
			}, tr)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := d.BringUp(ctx); err != nil {
				t.Fatal(err)
			}
			runStart := time.Now()
			d.Run(ctx, runStart, 4)

			tr.mu.Lock()
			defer tr.mu.Unlock()
			pubs = map[int]time.Duration{}
			for _, p := range tr.pubs {
				if p.id.Kind != node.KindBlock {
					continue
				}
				slotStart := runStart.Add(time.Duration(p.id.Slot) * 12 * time.Second)
				pubs[p.id.Slot] = p.at.Sub(slotStart)
			}
			if len(pubs) != 4 {
				t.Fatalf("got %d block publishes, want 4", len(pubs))
			}
			for slot, at := range pubs {
				if at < offset || at >= offset+jitter {
					t.Fatalf("slot %d published %v into the slot, want [%v, %v)", slot, at, offset, offset+jitter)
				}
			}
		})
		return pubs
	}
	if first, second := run(t), run(t); !maps.Equal(first, second) {
		t.Fatalf("same seed drew different jitter: %v vs %v", first, second)
	}
}
