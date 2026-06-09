package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// syncAssignment builds an N-node assignment with sync-committee membership only (no attestation
// committees): the given per-subnet member sets, and node `proposer` (outside the membership) as
// the block proposer, so members see the block and vote head. One slot.
func syncAssignment(n int, subnets [][]int, proposer int) *schedule.Assignment {
	return &schedule.Assignment{
		Params:          schedule.Params{N: n, V: n, SubnetCount: 64, NumSlots: 1},
		SyncSubscribers: subnets,
		Slots:           []schedule.SlotPlan{{Slot: 0, Proposer: proposer}},
	}
}

// syncKey identifies one published sync message across its arrivals: its subnet and the member
// who published it (the member rides the Attester field, like an aggregate's aggregator).
type syncKey struct{ subnet, member int }

// assertSyncCoverageNoLeakage checks the sync-message invariant: each member's message reaches
// exactly the other members of its subnet (SyncSubscribersOf(subnet) \ {member}) — no missing, no
// duplicate, and no leak to a non-member (a node on another subnet or no subnet at all).
func assertSyncCoverageNoLeakage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	got := map[syncKey]map[int]bool{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindSyncMessage || ar.ID.Slot != slot {
			continue
		}
		k := syncKey{ar.ID.Subnet, ar.ID.Attester}
		if got[k] == nil {
			got[k] = map[int]bool{}
		}
		if got[k][ar.Node] {
			t.Fatalf("duplicate sync message subnet %d member %d at node %d", k.subnet, k.member, ar.Node)
		}
		got[k][ar.Node] = true
	}
	for subnet, members := range a.SyncSubscribers {
		for _, member := range members {
			want := map[int]bool{}
			for _, nd := range members {
				if nd != member {
					want[nd] = true
				}
			}
			g := got[syncKey{subnet, member}]
			for rcv := range g {
				if !want[rcv] {
					t.Fatalf("sync message subnet %d member %d leaked to non-member %d", subnet, member, rcv)
				}
			}
			if len(g) != len(want) {
				t.Fatalf("sync subnet %d member %d: got %d receivers %v, want %d %v",
					subnet, member, len(g), keys(g), len(want), keys(want))
			}
			for nd := range want {
				if !g[nd] {
					t.Fatalf("sync subnet %d member %d: missing receiver %d", subnet, member, nd)
				}
			}
		}
	}
}

// Each member publishes one sync message on its subnet at the deadline; it reaches exactly the
// other members of that subnet via the per-subnet mesh (peersP < a subnet's size ⇒ multi-hop
// relay), and no non-member (the proposer or the bystanders 9,10) receives it.
func TestSyncCoverageNoLeakage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := syncAssignment(11, [][]int{{1, 2, 3, 4}, {5, 6, 7, 8}}, 0)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 4) // peersP=4 forces relay within a subnet
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)
		assertSyncCoverageNoLeakage(t, a, rec, 0)

		// Every member saw the block well before the 4s deadline, so all voted head.
		if got := rec.FractionVotedHead(0); got != 1.0 {
			t.Fatalf("FractionVotedHead = %v, want 1.0 (all members saw the block)", got)
		}
	})
}
