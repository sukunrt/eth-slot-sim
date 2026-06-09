package driver_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// sizedDecoupledAssignment builds the M6 sized decoupled assignment the way simctl/schedule.py does
// (its own RNG — the test asserts against this assignment's own sets): N=16, V=32, ac_vote_size=8,
// k=2, fs_subnets=2, fs_aggregators=2, num_columns=8. The full-custody backbone {0,1,2,3} proposes
// and originates columns; every node custodies every column (so the AC gate is satisfiable). FC
// subnets are an even partition; per-finality-slot aggregators are drawn from each subnet's members.
func sizedDecoupledAssignment(seed uint64) *schedule.Assignment {
	const n, v, acVoteSize, k, fsSubnets, fsAgg, numColumns, numSlots = 16, 32, 8, 2, 2, 2, 8, 5
	full := []int{0, 1, 2, 3}
	allNodes := make([]int, n)
	for i := range allNodes {
		allNodes[i] = i
	}
	cols := make([][]int, numColumns)
	for c := range cols {
		cols[c] = allNodes // everyone custodies every column ⇒ the gate is satisfiable for all voters
	}
	// FC subnets: even partition (node i → subnet i % fsSubnets); vps = Σ hosted validators.
	subnets := make([][]int, fsSubnets)
	vps := make([]int, fsSubnets)
	for i := range n {
		s := i % fsSubnets
		subnets[s] = append(subnets[s], i)
		vps[s] += (v-1-i)/n + 1
	}

	slots := make([]schedule.SlotPlan, numSlots)
	for s := range slots {
		sp := schedule.SlotPlan{Slot: s, Proposer: full[s%len(full)]}
		voters := rand.New(rand.NewPCG(seed, uint64(s)+100)).Perm(v)[:acVoteSize]
		for pos, val := range voters {
			sp.ACVoters = append(sp.ACVoters, schedule.AttesterRef{Node: val % n, Val: val, Position: pos})
		}
		if s%k == 0 { // finality boundary: draw fsAgg aggregators per subnet
			aggRng := rand.New(rand.NewPCG(seed, uint64(s/k)+200))
			sp.FinalityAggregators = make([][]int, fsSubnets)
			for i, members := range subnets {
				picked := make([]int, fsAgg)
				for j, p := range aggRng.Perm(len(members))[:fsAgg] {
					picked[j] = members[p]
				}
				slices.Sort(picked)
				sp.FinalityAggregators[i] = picked
			}
		}
		slots[s] = sp
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: v, AcVoteSize: acVoteSize, AcSlotsPerFinalitySlot: k,
			FsSubnets: fsSubnets, FsAggregators: fsAgg, NumSlots: numSlots, Seed: seed,
		},
		NumColumns:          numColumns,
		FullCustody:         full,
		ColumnSubscribers:   cols,
		FinalitySubscribers: subnets,
		ValidatorsPerSubnet: vps,
		Slots:               slots,
	}
}

// A sized decoupled run exercises all three floods together — the column-gated AC vote (every AC
// slot), and, on the finality clock, the per-subnet finality votes and the global population-scaled
// aggregates. It produces every headline CDF with full coverage, and a settled finality slot's votes
// reach the aggregators by the aggregation deadline. Race-skipped (the flood overflows TSan's epoch).
func TestDecoupledFullRunOutputs(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		a := sizedDecoupledAssignment(42)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, FCAggFraction: 50}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 6, dc)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 5)

		// AC: each slot's votes flood the global topic (N−1 each), and with block + all custody
		// columns in time, all vote block.
		assertACVoteCoverage(t, a, rec, 1)
		if got := rec.FractionVotedACVote(1); got != 1.0 {
			t.Fatalf("FractionVotedACVote(1) = %v, want 1.0 (block + columns in time)", got)
		}

		// FC: finality slot 1 (settled — its boundary is AC slot 2, aggregate at AC slot 3).
		assertFinalityVoteCoverage(t, a, rec, 1)
		assertFinalityAggregateCoverage(t, a, rec, 1)

		// Coverage at the aggregation deadline: every subnet's votes reach its aggregators well
		// within (aggregation_fraction − fc_vote_offset) = 12s − 1s of the vote.
		for subnet, aggs := range a.Slots[2].FinalityAggregators {
			if got := rec.FinalityCoverageAtDeadline(1, subnet, aggs, 11*time.Second); got != 1.0 {
				t.Fatalf("FinalityCoverageAtDeadline(fslot1, subnet %d) = %v, want 1.0", subnet, got)
			}
		}

		// Every headline CDF is populated and well-formed.
		for _, c := range []struct {
			name string
			kind node.Kind
			slot int
		}{
			{"AC block", node.KindBlock, 1}, {"AC vote", node.KindACVote, 1},
			{"FC vote", node.KindFinalityVote, 1}, {"FC aggregate", node.KindFinalityAggregate, 1},
		} {
			var delays []time.Duration
			for _, ar := range rec.Arrivals() {
				if ar.ID.Kind == c.kind && ar.ID.Slot == c.slot {
					delays = append(delays, ar.Delay)
				}
			}
			cdf := metrics.Summarize(delays)
			if cdf.Count == 0 || cdf.P50 <= 0 || cdf.P50 > cdf.P100 {
				t.Fatalf("%s CDF malformed: count=%d p50=%v p100=%v", c.name, cdf.Count, cdf.P50, cdf.P100)
			}
		}
	})
}

// Determinism: the same seed in two separate bubbles delivers the same set of arrivals. Identities
// only — gossipsub's mesh selection draws from the process-global RNG, which advances between
// bubbles, so relay paths (and thus per-arrival delays) differ even at a fixed seed.
func TestDecoupledRunDeterministic(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	runOnce := func(t *testing.T) []string {
		var keys []string
		synctest.Test(t, func(t *testing.T) {
			a := sizedDecoupledAssignment(42)
			rec := metrics.NewRecorder()
			dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, FCAggFraction: 50}
			s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 6, dc)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			s.run(t, ctx, 5)
			for _, ar := range rec.Arrivals() {
				keys = append(keys, fmt.Sprintf("%d,%d,%d,%d,%d",
					ar.Node, ar.ID.Kind, ar.ID.Slot, ar.ID.Subnet, ar.ID.Attester))
			}
			slices.Sort(keys)
		})
		return keys
	}
	var first, second []string
	t.Run("run1", func(t *testing.T) { first = runOnce(t) })
	t.Run("run2", func(t *testing.T) { second = runOnce(t) })
	if !slices.Equal(first, second) {
		t.Fatal("decoupled run not deterministic across two same-seed bubbles")
	}
}
