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

// sizedSegregatedAssignment is sizedDecoupledAssignment's segregation twin (same M6 box: N=16,
// V=32, ac_vote_size=8, k=2, fs_subnets=2, fs_aggregators=2, num_columns=8, 5 slots), plus the
// per-validator round draw (so AC slot s carries only round-(s%k) votes) and the aggregator draw
// moved to EVERY slot. The test asserts the seed avoids the degenerate shapes (empty cells, no
// foreign aggregator).
func sizedSegregatedAssignment(seed uint64) *schedule.Assignment {
	const n, v, acVoteSize, k, fsSubnets, fsAgg, numColumns, numSlots = 16, 32, 8, 2, 2, 2, 8, 5
	full := []int{0, 1, 2, 3}
	allNodes := make([]int, n)
	for i := range allNodes {
		allNodes[i] = i
	}
	cols := make([][]int, numColumns)
	for c := range cols {
		cols[c] = allNodes
	}
	subnets := make([][]int, fsSubnets)
	for i := range n {
		subnets[i%fsSubnets] = append(subnets[i%fsSubnets], i)
	}
	// The two independent validator draws: voting subnet and round; vprs counts their cells.
	subnetOf := make([]int, v)
	roundOf := make([]int, v)
	vps := make([]int, fsSubnets)
	vprs := make([][]int, k)
	for r := range vprs {
		vprs[r] = make([]int, fsSubnets)
	}
	voteRng := rand.New(rand.NewPCG(seed, 300))
	roundRng := rand.New(rand.NewPCG(seed, 400))
	for val := range subnetOf {
		subnetOf[val] = voteRng.IntN(fsSubnets)
		roundOf[val] = roundRng.IntN(k)
		vps[subnetOf[val]]++
		vprs[roundOf[val]][subnetOf[val]]++
	}

	slots := make([]schedule.SlotPlan, numSlots)
	for s := range slots {
		sp := schedule.SlotPlan{Slot: s, Proposer: full[s%len(full)]}
		voters := rand.New(rand.NewPCG(seed, uint64(s)+100)).Perm(v)[:acVoteSize]
		for pos, val := range voters {
			sp.ACVoters = append(sp.ACVoters, schedule.AttesterRef{Node: val % n, Val: val, Position: pos})
		}
		// EVERY slot is a round: draw fsAgg aggregator VALIDATORS per subnet, whole set.
		aggRng := rand.New(rand.NewPCG(seed, uint64(s)+200))
		sp.FinalityAggregators = make([][]schedule.AttesterRef, fsSubnets)
		for i := range fsSubnets {
			for pos, val := range aggRng.Perm(v)[:fsAgg] {
				sp.FinalityAggregators[i] = append(sp.FinalityAggregators[i],
					schedule.AttesterRef{Node: val % n, Val: val, Subnet: i, Position: pos})
			}
		}
		slots[s] = sp
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: v, AcVoteSize: acVoteSize, AcSlotsPerFinalitySlot: k,
			FsSubnets: fsSubnets, FsAggregators: fsAgg, NumSlots: numSlots, Seed: seed,
		},
		NumColumns:               numColumns,
		FullCustody:              full,
		ColumnSubscribers:        cols,
		FinalitySubscribers:      subnets,
		FinalitySubnetOf:         subnetOf,
		ValidatorsPerSubnet:      vps,
		FinalityRoundOf:          roundOf,
		ValidatorsPerRoundSubnet: vprs,
		Slots:                    slots,
	}
}

// A sized segregated run exercises the steady per-slot hum: the column-gated AC vote plus, EVERY
// AC slot, a round's worth of finality votes and cell-scaled aggregates. Measured on fslot 1's
// two rounds (AC slots 2 and 3, settled): full coverage per round, every validator votes exactly
// once across the fslot, the round's votes reach its aggregators by the per-slot deadline, and
// the aggregate flood closes within the slot's last third — the variant's headline question.
func TestSegregatedFullRunOutputs(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		a := sizedSegregatedAssignment(42)
		// Guard the seed against the degenerate shapes the assertions rely on.
		for r, row := range a.ValidatorsPerRoundSubnet {
			for s, cell := range row {
				if cell == 0 {
					t.Fatalf("fixture degenerated: empty (round %d, subnet %d) cell", r, s)
				}
			}
		}
		foreign := false
		for _, slot := range []int{2, 3} {
			for subnet, refs := range a.Slots[slot].FinalityAggregators {
				for _, r := range refs {
					if !slices.Contains(a.FinalitySubscribersOf(subnet), r.Node) {
						foreign = true
					}
				}
			}
		}
		if !foreign {
			t.Fatal("fixture degenerated: every fslot-1 aggregator host is a member of its subnet")
		}

		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, Segregated: true, RoundAggFraction: 67}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 6, dc)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 5)

		// AC: unchanged under segregation — each slot's votes flood the global topic, all vote block.
		assertACVoteCoverage(t, a, rec, 1)
		if got := rec.FractionVotedACVote(1); got != 1.0 {
			t.Fatalf("FractionVotedACVote(1) = %v, want 1.0 (block + columns in time)", got)
		}

		// FC: both rounds of finality slot 1 (AC slots 2 and 3, settled), full coverage each.
		for _, slot := range []int{2, 3} {
			assertRoundVoteCoverage(t, a, rec, slot)
			assertRoundAggregateCoverage(t, a, rec, slot)
		}

		// Across the fslot every validator's vote shows up in exactly its round's AC slot.
		votedIn := map[int][]int{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind != node.KindFinalityVote || (ar.ID.Slot != 2 && ar.ID.Slot != 3) {
				continue
			}
			if !slices.Contains(votedIn[ar.ID.Attester], ar.ID.Slot) {
				votedIn[ar.ID.Attester] = append(votedIn[ar.ID.Attester], ar.ID.Slot)
			}
		}
		for val, round := range a.FinalityRoundOf {
			if want := []int{2 + round}; !slices.Equal(votedIn[val], want) {
				t.Fatalf("val %d voted in slots %v over fslot 1, want exactly %v", val, votedIn[val], want)
			}
		}

		// Coverage at the per-slot deadline: each round's votes reach ITS aggregator hosts within
		// (round_aggregation_fraction − fc_vote_offset) = 8.04s − 1s of the vote.
		due := 67*12*time.Second/100 - time.Second
		for _, slot := range []int{2, 3} {
			for subnet, refs := range a.Slots[slot].FinalityAggregators {
				aggs := aggHostNodes(refs)
				if got := rec.FinalityCoverageAtDeadline(slot, subnet, aggs, due); got != 1.0 {
					t.Fatalf("FinalityCoverageAtDeadline(slot %d, subnet %d) = %v, want 1.0", slot, subnet, got)
				}
			}
		}

		// The headline question: the round aggregate flood closes within the slot's LAST THIRD —
		// every arrival within 100%−67% = 33% of the slot (3.96s) of its publish instant.
		window := 12*time.Second - 67*12*time.Second/100
		for _, slot := range []int{2, 3} {
			var delays []time.Duration
			for _, ar := range rec.Arrivals() {
				if ar.ID.Kind == node.KindFinalityAggregate && ar.ID.Slot == slot {
					delays = append(delays, ar.Delay)
				}
			}
			cdf := metrics.Summarize(delays)
			if cdf.Count == 0 || cdf.P100 >= window {
				t.Fatalf("slot %d aggregate CDF p100 = %v (count %d), want < %v (the slot's last third)",
					slot, cdf.P100, cdf.Count, window)
			}
		}

		// Every headline CDF is populated and well-formed.
		for _, c := range []struct {
			name string
			kind node.Kind
			slot int
		}{
			{"AC block", node.KindBlock, 2}, {"AC vote", node.KindACVote, 2},
			{"round vote", node.KindFinalityVote, 2}, {"round aggregate", node.KindFinalityAggregate, 2},
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

// Determinism: the same seed in two separate bubbles delivers the same set of arrivals
// (identities only — relay paths draw from the process-global RNG; see the base twin).
func TestSegregatedRunDeterministic(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	runOnce := func(t *testing.T) []string {
		var keys []string
		synctest.Test(t, func(t *testing.T) {
			a := sizedSegregatedAssignment(42)
			rec := metrics.NewRecorder()
			dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, Segregated: true, RoundAggFraction: 67}
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
		t.Fatal("segregated run not deterministic across two same-seed bubbles")
	}
}
