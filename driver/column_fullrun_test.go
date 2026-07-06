package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
)

// A sized column run produces the two headline outputs: the per-column arrival CDF (over the
// recorded column arrivals) and the gate-outcome summary (custody-complete rate). With a
// sparse base graph the columns relay through the backbone; every custodier completes in time,
// so the rate is 1. Race-skipped (the burst overflows TSan's epoch, like the attestation run).
func TestColumnFullRunOutputs(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		a := genColumnAssignment(16, 16, 4, []int{0, 1, 2}, 42)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 5) // peersP=5 forces relay
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertColumnCoverageNoLeakage(t, a, rec, 0)

		// Per-column arrival CDF: every custodier minus the proposer (the origin), per column.
		var colDelays []time.Duration
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindColumn && ar.ID.Slot == 0 {
				colDelays = append(colDelays, ar.Delay)
			}
		}
		want := 0
		for col := range a.NumColumns {
			want += len(a.ColumnSubscribersOf(col)) - 1 // proposer (node 0) is in every column
		}
		cdf := metrics.Summarize(colDelays)
		if cdf.Count != want {
			t.Fatalf("column CDF count = %d, want %d", cdf.Count, want)
		}
		if cdf.P50 <= 0 || cdf.P50 > cdf.P100 {
			t.Fatalf("column CDF percentiles unsorted/zero: p50=%v p100=%v", cdf.P50, cdf.P100)
		}

		// Gate outcome: every custodier had all its columns by the deadline ⇒ rate 1.
		custody := map[int][]int{}
		for nd := range a.Params.N {
			custody[nd] = a.Node(nd).CustodyColumns
		}
		if got := rec.CustodyCompleteRate(0, custody, 4*time.Second); got != 1.0 {
			t.Fatalf("CustodyCompleteRate = %v, want 1 (all columns delivered in time)", got)
		}
	})
}

// The gate-outcome headline: holding one custody column from one attester drops the
// custody-complete rate AND the fraction-voted-block in lockstep — the rate (1 of 4 custodiers
// incomplete) attributes the prior-head vote (1 of 3 attesters) to a missing column.
func TestColumnGateOutcomeSummary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := oneCommitteeColumns(4, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 3)
		dropColumnTo(s, 1, 0)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		custody := map[int][]int{}
		for nd := range a.Params.N {
			custody[nd] = a.Node(nd).CustodyColumns
		}
		if got, want := rec.CustodyCompleteRate(0, custody, 4*time.Second), 3.0/4.0; got != want {
			t.Fatalf("CustodyCompleteRate = %v, want %v (node 1 missing a custody column)", got, want)
		}
		if got, want := rec.FractionVotedBlock(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedBlock = %v, want %v", got, want)
		}
	})
}
