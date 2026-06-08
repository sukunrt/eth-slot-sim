package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/node"
)

// The block proposer follows committee.json's per-slot schedule (supernodes in a real run),
// not slot%n: over a multi-slot run every block is published by exactly that slot's scheduled
// proposer and by no other node. This drives the same validator/driver code the Shadow binary
// runs, so the two backends propose identically.
func TestBlockProposerFollowsSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		com := [][]committee.AttesterRef{{
			{Node: 0, Val: 0, Subnet: 0, Position: 0},
			{Node: 1, Val: 1, Subnet: 0, Position: 1},
		}}
		// proposers 2 and 3 are neither attesters nor subscribers of subnet 0 — they only
		// publish the global block, so a pure cyclic rule (slot0->0, slot1->1) would differ.
		a := &committee.Assignment{
			Params: committee.Params{
				N: 4, V: 4, C: 1, Sc: 2, SubnetCount: 64,
				SubnetsPerNode: 1, SubscribeFloor: 2, NumSlots: 2,
			},
			SubnetSubscribers: [][]int{{0, 1}},
			Slots: []committee.SlotPlan{
				{Slot: 0, Committees: com, SubnetOf: []int{0}, Proposer: 2},
				{Slot: 1, Committees: com, SubnetOf: []int{0}, Proposer: 3},
			},
		}
		tr := &timeTracer{}
		s := buildScenario(t, a, 4*time.Second, nil, tr, 3)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 2)

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
		}
		if want := map[int]int{0: 2, 1: 3}; got[0] != want[0] || got[1] != want[1] || len(got) != 2 {
			t.Fatalf("block proposers = %v, want %v (schedule, not slot%%n)", got, want)
		}
	})
}
