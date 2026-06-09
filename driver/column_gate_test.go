package driver_test

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/schedule"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
)

// oneCommitteeColumns extends oneCommittee with a full-custody proposer (node 0, outside the
// committee) that originates every column, and makes the committee's attesters custody all
// numColumns columns — so the gate (vote block iff block AND all custody columns) can be
// exercised against the attestation vote.
func oneCommitteeColumns(n int, attesters []int, numColumns int) *schedule.Assignment {
	a := oneCommittee(n, attesters)
	a.NumColumns = numColumns
	a.FullCustody = []int{0} // node 0: the full-custody proposer / relay backbone
	custodiers := append([]int{0}, attesters...)
	slices.Sort(custodiers)
	cols := make([][]int, numColumns)
	for i := range cols {
		cols[i] = slices.Clone(custodiers)
	}
	a.ColumnSubscribers = cols
	a.Slots[0].Proposer = 0
	return a
}

// dropColumnTo installs a test-only OnReceive filter on node victim that holds back column
// col (mirroring buildScenario's suppressBlock) — so the victim never completes custody.
func dropColumnTo(s *scenario, victim, col int) {
	nd := s.nodes[victim]
	orig := nd.OnReceive
	nd.OnReceive = func(rec node.Received) {
		if rec.Kind == node.KindColumn && int(rec.Obj.(*pb.Column).Column) == col {
			return
		}
		orig(rec)
	}
}

// The gate: hold back one custody column from exactly one attester. It has the block but not
// all its custody columns by the deadline ⇒ it votes prior head, while the others vote block.
// The fraction is exactly (s_c−1)/s_c — same crossing as the block-held forced flip, but
// attributable to a missing column, not a missing block.
func TestColumnGateForcedFlip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := oneCommitteeColumns(4, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 3)
		dropColumnTo(s, 1, 0) // attester 1 never gets custody column 0

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		if got, want := rec.FractionVotedBlock(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedBlock = %v, want %v (attester 1 missing a custody column)", got, want)
		}
		voted := map[int]bool{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindAttestation {
				voted[ar.ID.Attester] = ar.VotedBlock
			}
		}
		if voted[1] {
			t.Fatal("attester 1 (custody column held back) voted block, want prior head")
		}
		if !voted[2] || !voted[3] {
			t.Fatalf("attesters 2,3 voted prior, want block (voted=%v)", voted)
		}
	})
}

// With every custody column delivered, the gate is satisfied and all attesters vote block —
// the column phase doesn't spuriously suppress votes on the happy path.
func TestColumnGateAllColumnsVoteBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := oneCommitteeColumns(4, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 3)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		if got := rec.FractionVotedBlock(0); got != 1.0 {
			t.Fatalf("FractionVotedBlock = %v, want 1 (block + all custody columns in time)", got)
		}
	})
}
