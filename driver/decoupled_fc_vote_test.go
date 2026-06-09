package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// decoupledFCAssignment builds an N-node decoupled assignment focused on the finality-chain vote:
// the given finality-subnet partition and V validators (V>N ⇒ per-validator multiplicity). No
// columns/AC voters — the FC vote is un-gated and fixed-time, so this isolates it. k AC slots per
// finality slot; node `proposer` publishes the (irrelevant) block.
func decoupledFCAssignment(n, v, k int, subnets [][]int, proposer, numSlots int) *schedule.Assignment {
	slots := make([]schedule.SlotPlan, numSlots)
	for s := range slots {
		slots[s] = schedule.SlotPlan{Slot: s, Proposer: proposer}
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: v, AcSlotsPerFinalitySlot: k, FsSubnets: len(subnets), NumSlots: numSlots,
		},
		FinalitySubscribers: subnets,
		Slots:               slots,
	}
}

// fvKey identifies one published finality vote across its arrivals: its subnet and the validator
// that voted (the validator rides Attester; the publishing node is its host under uniform v%N).
type fvKey struct{ subnet, val int }

// assertFinalityVoteCoverage checks the per-subnet invariant: each validator's finality vote reaches
// exactly the OTHER member nodes of its subnet (FinalitySubscribersOf(subnet) \ {host}) — no
// missing, no dup, no leak to a non-member — and a node hosting m validators emits m distinct votes.
func assertFinalityVoteCoverage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, fslot int) {
	t.Helper()
	got := map[fvKey]map[int]bool{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindFinalityVote || ar.ID.Slot != fslot {
			continue
		}
		k := fvKey{ar.ID.Subnet, ar.ID.Attester}
		if got[k] == nil {
			got[k] = map[int]bool{}
		}
		if got[k][ar.Node] {
			t.Fatalf("duplicate finality vote subnet %d val %d at node %d", k.subnet, k.val, ar.Node)
		}
		got[k][ar.Node] = true
	}
	for subnet, members := range a.FinalitySubscribers {
		for _, host := range members {
			vals := a.Node(host).FinalityVoteDuties()
			if len(vals) == 0 {
				t.Fatalf("subnet %d member %d hosts no validators (V<N?)", subnet, host)
			}
			for _, val := range vals { // a node with m validators emits m votes
				want := map[int]bool{}
				for _, nd := range members {
					if nd != host {
						want[nd] = true
					}
				}
				g := got[fvKey{subnet, val}]
				for rcv := range g {
					if !want[rcv] {
						t.Fatalf("finality vote subnet %d val %d leaked to non-member %d", subnet, val, rcv)
					}
				}
				if len(g) != len(want) {
					t.Fatalf("finality subnet %d val %d: got %d receivers %v, want %d %v",
						subnet, val, len(g), keys(g), len(want), keys(want))
				}
			}
		}
	}
}

// At fc_vote_offset into the finality slot, every node emits one finality vote per validator it
// hosts on its subnet; each reaches exactly the other members of that subnet, none leak to a
// non-member, and a node with 2 validators emits 2 votes. Finality slot 0 votes at AC slot 0.
func TestDecoupledFinalityVoteCoverage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 8 nodes, 2 subnets of 4; V=16 ⇒ each node hosts 2 validators. proposer=0 (also a member).
		a := decoupledFCAssignment(8, 16, 2, [][]int{{0, 2, 4, 6}, {1, 3, 5, 7}}, 0, 2)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 4, dc) // peersP=4 forces relay within a subnet

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 2) // run finality slot 0 (votes at AC slot 0) plus its second AC slot

		assertFinalityVoteCoverage(t, a, rec, 0)

		// Total arrivals = Σ over (subnet,val) of (subnet size − 1): 2 subnets · 8 vals/subnet · 3 = 48.
		want := 0
		for _, members := range a.FinalitySubscribers {
			for _, host := range members {
				want += len(a.Node(host).FinalityVoteDuties()) * (len(members) - 1)
			}
		}
		got := 0
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindFinalityVote && ar.ID.Slot == 0 {
				got++
			}
		}
		if got != want {
			t.Fatalf("finality vote arrivals = %d, want %d", got, want)
		}
	})
}
