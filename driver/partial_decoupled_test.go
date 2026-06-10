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

// runDecoupledTransport runs the sized decoupled (or segregated) fixture under one transport
// and returns the finality-vote arrival keys plus the post-run live partial-group counts.
// numSlots is chosen so the LAST vote round is reaped before the run ends (base k=2: votes at
// slots 0 and 2, reaps at slots 1 and 3; segregated: every slot reaps itself), making zero
// live groups the exact GC expectation. check is the variant's absolute coverage assertion.
func runDecoupledTransport(t *testing.T, a *schedule.Assignment, dc *driver.DecoupledParams,
	partial bool, numSlots int,
	check func(*testing.T, *schedule.Assignment, *metrics.Recorder)) (keys []string, liveGroups int) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		rec := metrics.NewRecorder()
		var s *scenario
		if partial {
			s = buildPartialDecoupledScenario(t, a, 4*time.Second, nil, rec, 6, dc)
		} else {
			s = buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 6, dc)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, numSlots)

		check(t, a, rec)
		keys = arrivalKeys(rec, node.KindFinalityVote)
		for _, nd := range s.nodes {
			liveGroups += nd.LivePartialGroups()
		}
	})
	return keys, liveGroups
}

// The decoupled headline parity: classic and partial finality-vote floods on one schedule are
// arrival-set-equal, the absolute per-validator coverage holds, and — every vote round reaped
// before the run ends — the partial buckets are fully GC'd (no growth across rounds).
func TestPartialDecoupledFCVoteParity(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, FCAggFraction: 50}
	check := func(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder) {
		assertFinalityVoteCoverage(t, a, rec, 1) // fslot 1, settled
	}
	var classic, partial []string
	var live int
	t.Run("classic", func(t *testing.T) {
		classic, _ = runDecoupledTransport(t, sizedDecoupledAssignment(42), dc, false, 4, check)
	})
	t.Run("partial", func(t *testing.T) {
		partial, live = runDecoupledTransport(t, sizedDecoupledAssignment(42), dc, true, 4, check)
	})
	if len(classic) == 0 || !slices.Equal(classic, partial) {
		t.Fatalf("classic (%d) and partial (%d) FC-vote arrival sets differ",
			len(classic), len(partial))
	}
	if live != 0 {
		t.Fatalf("live partial groups after the run = %d, want 0 (reaped rounds pruned)", live)
	}
}

// The segregated twin: per-AC-slot rounds, cell-sized committees (positions are the
// (subnet, round) cell ranks), every slot reaped at its own end ⇒ zero live groups.
func TestPartialSegregatedFCVoteParity(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second,
		Segregated: true, RoundAggFraction: 50}
	check := func(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder) {
		assertRoundVoteCoverage(t, a, rec, 2) // a settled round of fslot 1
	}
	var classic, partial []string
	var live int
	t.Run("classic", func(t *testing.T) {
		classic, _ = runDecoupledTransport(t, sizedSegregatedAssignment(42), dc, false, 4, check)
	})
	t.Run("partial", func(t *testing.T) {
		partial, live = runDecoupledTransport(t, sizedSegregatedAssignment(42), dc, true, 4, check)
	})
	if len(classic) == 0 || !slices.Equal(classic, partial) {
		t.Fatalf("classic (%d) and partial (%d) FC-vote arrival sets differ",
			len(classic), len(partial))
	}
	if live != 0 {
		t.Fatalf("live partial groups after the run = %d, want 0 (every round reaped)", live)
	}
}
