package driver_test

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// decoupledACAssignment builds an N-node decoupled assignment focused on the availability-chain
// vote: a full-custody proposer (node 0, outside the voter set) originating every column, the given
// AC voters (one validator each, val == node, for clean flip arithmetic), all custodying every
// column so the DA gate is exercised. Finality membership is omitted — this fixture tests only the
// AC vote (M3).
func decoupledACAssignment(n int, voters []int, numColumns int) *schedule.Assignment {
	acVoters := make([]schedule.AttesterRef, len(voters))
	for i, nd := range voters {
		acVoters[i] = schedule.AttesterRef{Node: nd, Val: nd} // val == node: one voter per node
	}
	custodiers := append([]int{0}, voters...)
	slices.Sort(custodiers)
	cols := make([][]int, numColumns)
	for i := range cols {
		cols[i] = slices.Clone(custodiers)
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: n, AcVoteSize: len(voters), AcSlotsPerFinalitySlot: 1, NumSlots: 1,
		},
		NumColumns:        numColumns,
		FullCustody:       []int{0},
		ColumnSubscribers: cols,
		Slots:             []schedule.SlotPlan{{Slot: 0, Proposer: 0, ACVoters: acVoters}},
	}
}

// acKey identifies one published AC vote across its arrivals: the validator that voted and the node
// that published it (val rides Attester, the publisher rides Origin).
type acKey struct{ val, origin int }

// assertACVoteCoverage checks the global-topic invariant: each voter's AC vote reaches every node
// except its publisher, exactly once (N−1 coverage, no leak, no dup) — like an aggregate.
func assertACVoteCoverage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	got := map[acKey]map[int]int{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindACVote || ar.ID.Slot != slot {
			continue
		}
		k := acKey{ar.ID.Attester, ar.ID.Origin}
		if got[k] == nil {
			got[k] = map[int]int{}
		}
		got[k][ar.Node]++
	}
	for _, r := range a.Slots[slot].ACVoters {
		recvd := got[acKey{r.Val, r.Node}]
		for nd := range a.Params.N {
			want := 1
			if nd == r.Node {
				want = 0 // the publisher's own loopback is skipped
			}
			if recvd[nd] != want {
				t.Fatalf("AC vote val %d origin %d at node %d: got %d arrivals, want %d",
					r.Val, r.Node, nd, recvd[nd], want)
			}
		}
	}
}

// Every AC voter publishes one vote on the global topic; it reaches every other node exactly once,
// and with the block + all custody columns in well before the deadline, all vote block.
func TestDecoupledACVoteCoverage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := decoupledACAssignment(6, []int{1, 2, 3, 4, 5}, 4)
		rec := metrics.NewRecorder()
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 3, &driver.DecoupledParams{K: 1})

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertACVoteCoverage(t, a, rec, 0)
		if got := rec.FractionVotedACVote(0); got != 1.0 {
			t.Fatalf("FractionVotedACVote = %v, want 1.0 (all voters saw block + columns)", got)
		}
	})
}

// The DA gate, over the global AC voters: hold back one custody column from exactly one voter past
// the deadline ⇒ it votes prior head while the others vote block, so the fraction is exactly
// (voters−1)/voters — the column gate reused verbatim, now on the availability chain.
func TestDecoupledACVoteColumnGateFlip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := decoupledACAssignment(4, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 3, &driver.DecoupledParams{K: 1})
		dropColumnTo(s, 1, 0) // voter 1 never completes custody (missing column 0)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		if got, want := rec.FractionVotedACVote(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedACVote = %v, want %v (voter 1 missing a custody column)", got, want)
		}
		voted := map[int]bool{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindACVote {
				voted[ar.ID.Attester] = ar.VotedBlock
			}
		}
		if voted[1] {
			t.Fatal("voter 1 (custody column held back) voted block, want prior head")
		}
		if !voted[2] || !voted[3] {
			t.Fatalf("voters 2,3 voted prior, want block (voted=%v)", voted)
		}
	})
}
