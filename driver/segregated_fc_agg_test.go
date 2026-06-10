package driver_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/schedule"
	"github.com/ethp2p/slot-sim/validator"
)

// segregatedAggAssignment builds a segregated assignment focused on the round aggregate: V
// validators with subnetOf = val % fs (host = val % N — every vote stays on its host's own
// subnet under the evens/odds partitions used here, so the only dials are aggregator pre-joins)
// and roundOf = (val / fs) % k (every (round, subnet) cell populated). slotAggs[slot][subnet]
// lists the aggregator VALIDATOR ids per slot (empty slots carry empty per-subnet lists).
func segregatedAggAssignment(n, v, k int, subnets [][]int, slotAggs [][][]int, numSlots int) *schedule.Assignment {
	fs := len(subnets)
	subnetOf := make([]int, v)
	roundOf := make([]int, v)
	for val := range subnetOf {
		subnetOf[val] = val % fs
		roundOf[val] = (val / fs) % k
	}
	aggs := make([][][]int, numSlots)
	for s := range aggs {
		if s < len(slotAggs) && slotAggs[s] != nil {
			aggs[s] = slotAggs[s]
		} else {
			aggs[s] = make([][]int, fs)
		}
	}
	return segregatedFCAssignment(n, k, subnets, subnetOf, roundOf, aggs, 0, numSlots)
}

// assertRoundAggregateCoverage: each of AC slot `slot`'s aggregator HOSTS publishes ONE distinct
// aggregate on the global topic, reaching every node except itself exactly once — the base
// assertion re-keyed to the per-slot draw.
func assertRoundAggregateCoverage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	got := map[aggKey]map[int]int{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindFinalityAggregate || ar.ID.Slot != slot {
			continue
		}
		k := aggKey{ar.ID.Subnet, ar.ID.Attester}
		if got[k] == nil {
			got[k] = map[int]int{}
		}
		got[k][ar.Node]++
	}
	for subnet, refs := range a.Slots[slot].FinalityAggregators {
		for _, agg := range aggHostNodes(refs) {
			recvd := got[aggKey{subnet, agg}]
			for nd := range a.Params.N {
				want := 1
				if nd == agg {
					want = 0 // the aggregator published it; its own loopback is skipped
				}
				if recvd[nd] != want {
					t.Fatalf("round aggregate slot %d subnet %d agg %d node %d: got %d, want %d",
						slot, subnet, agg, nd, recvd[nd], want)
				}
			}
		}
	}
}

// Round aggregates publish on the global topic every AC slot, each reaching N−1, and each is
// sized to its (round, subnet) CELL population — validators_per_round_subnet[s%k][subnet], not
// the subnet's whole draw count (here cell = 2 vs subnet total 4: a wrong source doubles the
// bitfield). One aggregator per subnet is a NON-member (vals 3 and 2 — hosts on the opposite
// partition), exercising the per-slot pre-join Subscribe. Measured on a settled slot (2).
func TestSegregatedRoundAggregateCoverageAndCellSize(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		aggs := [][]int{{0, 3}, {1, 2}} // per subnet, drawn fresh-but-identical each slot
		a := segregatedAggAssignment(8, 8, 2,
			[][]int{{0, 2, 4, 6}, {1, 3, 5, 7}}, [][][]int{aggs, aggs, aggs, aggs, aggs}, 5)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, Segregated: true, RoundAggFraction: 67}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 4, dc)

		// Capture the wire size of each slot-2 aggregate at node 4 (a non-aggregator member that
		// receives all of them on the global topic); proto3 re-marshal is canonical.
		var mu sync.Mutex
		wireLen := map[aggKey]int{} // (subnet, aggregator origin) → marshaled bytes
		orig := s.nodes[4].OnReceive
		s.nodes[4].OnReceive = func(r node.Received) {
			if fa, ok := r.Obj.(*pb.FinalityAggregate); ok && fa.FinalitySlot == 2 {
				b, err := proto.Marshal(fa)
				if err != nil {
					t.Errorf("marshal received aggregate: %v", err)
				}
				mu.Lock()
				wireLen[aggKey{int(fa.Subnet), int(fa.Origin)}] = len(b)
				mu.Unlock()
			}
			orig(r)
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 5)

		assertRoundAggregateCoverage(t, a, rec, 2)
		assertRoundAggregateCoverage(t, a, rec, 3) // the next round floods independently

		// 4 aggregators per slot, each reaching N−1 = 7 ⇒ 28 arrivals for slot 2.
		got := 0
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindFinalityAggregate && ar.ID.Slot == 2 {
				got++
			}
		}
		if want := 4 * (a.Params.N - 1); got != want {
			t.Fatalf("round aggregate arrivals (slot 2) = %d, want %d", got, want)
		}

		// Cell-scaled size: slot 2 is round 0, so the bitfield covers the (0, subnet) cell only.
		mu.Lock()
		defer mu.Unlock()
		for subnet, refs := range a.Slots[2].FinalityAggregators {
			cell := a.ValidatorsPerRoundSubnet[0][subnet] // slot 2 % k=2 ⇒ round 0
			if cell == a.ValidatorsPerSubnet[subnet] {
				t.Fatalf("fixture drifted: cell %d == subnet total — size source indistinguishable", cell)
			}
			for _, agg := range aggHostNodes(refs) {
				want := len(validator.MakeFinalityAggregate(2, subnet, agg, cell).Payload)
				if got := wireLen[aggKey{subnet, agg}]; got != want {
					t.Fatalf("round aggregate subnet %d agg %d wire size = %d bytes, want %d (cell=%d)",
						subnet, agg, got, want, cell)
				}
			}
		}
	})
}

// Each slot-2 round aggregate publishes at exactly slotStart(2) + roundAggFraction%·slotDur =
// runStart + 24s + 8.04s — the per-AC-slot deadline arithmetic (a per-fslot deadline would land
// at 67%·k·12s instead).
func TestSegregatedRoundAggregatePublishInstant(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		aggs := [][]int{{0, 3}, {1, 2}}
		a := segregatedAggAssignment(8, 8, 2,
			[][]int{{0, 2, 4, 6}, {1, 3, 5, 7}}, [][][]int{aggs, aggs, aggs, aggs, aggs}, 5)
		tr := &timeTracer{}
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, Segregated: true, RoundAggFraction: 67}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, tr, 4, dc)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		runStart := s.run(t, ctx, 5)

		wantAt := runStart.Add(24*time.Second + 67*12*time.Second/100)
		aggsSeen := 0
		tr.mu.Lock()
		defer tr.mu.Unlock()
		for _, p := range tr.pubs {
			if p.id.Kind != node.KindFinalityAggregate || p.id.Slot != 2 {
				continue
			}
			aggsSeen++
			if !p.at.Equal(wantAt) {
				t.Fatalf("round aggregate %+v published at %v, want %v", p.id, p.at, wantAt)
			}
		}
		if want := 4; aggsSeen != want { // 2 subnets × 2 aggregator hosts, one aggregate each
			t.Fatalf("round aggregate publishes (slot 2) = %d, want %d", aggsSeen, want)
		}
	})
}

// A NON-member aggregator for AC slot s pre-joins the subnet during slot s−1 (dial + Subscribe)
// and holds ALL of the round's votes by its deadline; a member node that neither aggregates nor
// fans out dials nothing during the run. Only slot 2 carries aggregators here, isolating the
// per-slot pre-join window.
func TestSegregatedRoundAggregatePreJoin(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		// 12 nodes, 2 member subnets of 6; slot 2's aggregator vals {1} and {2} — val 1 (host 1,
		// odd) aggregates subnet 0, val 2 (host 2, even) aggregates subnet 1: both foreign.
		subnets := [][]int{{0, 2, 4, 6, 8, 10}, {1, 3, 5, 7, 9, 11}}
		a := segregatedAggAssignment(12, 12, 2, subnets,
			[][][]int{nil, nil, {{1}, {2}}, nil, nil}, 5)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, Segregated: true, RoundAggFraction: 67}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 4, dc)
		recorders := make([]*dialRecorder, len(s.nodes))
		for i, nd := range s.nodes {
			recorders[i] = &dialRecorder{inner: nd.Network}
			nd.Network = recorders[i]
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		runStart := s.run(t, ctx, 5)

		// Aggregator host 1 pre-joined extra subnet-0 members during slot 1 (the slot before its
		// round); node 4 (a member, never an aggregator, votes only on its own subnet) dialed
		// nothing during the run.
		aggDials := recorders[1].after(runStart)
		if len(aggDials) == 0 {
			t.Fatal("aggregator 1 dialed no extra peers during the run — pre-join missing")
		}
		for _, peer := range aggDials {
			if !slices.Contains(subnets[0], peer) {
				t.Fatalf("aggregator 1 pre-join dialed %d, want a subnet-0 member", peer)
			}
		}
		if d := recorders[4].after(runStart); len(d) != 0 {
			t.Fatalf("non-aggregator 4 dialed %v during the run, want none", d)
		}

		// The pre-join Subscribe collected the round's votes: host 1 (NOT a member of subnet 0)
		// received every subnet-0 vote of slot 2 — round 0's cell, the aggregator's whole job.
		got := map[int]bool{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindFinalityVote && ar.ID.Slot == 2 && ar.ID.Subnet == 0 && ar.Node == 1 {
				got[ar.ID.Attester] = true
			}
		}
		for val, subnet := range a.FinalitySubnetOf {
			if subnet == 0 && a.FinalityRoundOf[val] == 0 && !got[val] {
				t.Fatalf("aggregator host 1 missing slot-2 subnet-0 vote of val %d (got %v)", val, keys(got))
			}
		}
	})
}

// A host aggregating the SAME subnet in consecutive rounds keeps the subscription across the
// boundary: round s's teardown (its aggregation deadline) overlaps round s+1's live pre-join,
// and dropFinality's sparing must not drop what the next round still lists — under segregation
// this overlap happens every slot, so it is pinned explicitly (spec §12).
func TestSegregatedConsecutiveRoundsSameSubnetKeepsSubscription(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		subnets := [][]int{{0, 2, 4, 6, 8, 10}, {1, 3, 5, 7, 9, 11}}
		// Host 1 (foreign to subnet 0) aggregates subnet 0 in BOTH slots 2 and 3.
		a := segregatedAggAssignment(12, 12, 2, subnets,
			[][][]int{nil, nil, {{1}, {2}}, {{1}, {4}}, nil}, 5)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, Segregated: true, RoundAggFraction: 67}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 4, dc)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 5)

		// Host 1 holds EVERY subnet-0 vote of both rounds: slot 2 (round 0) and slot 3 (round 1).
		// Slot 3's votes arrive AFTER slot 2's teardown fired — only the sparing keeps them coming.
		for _, slot := range []int{2, 3} {
			got := map[int]bool{}
			for _, ar := range rec.Arrivals() {
				if ar.ID.Kind == node.KindFinalityVote && ar.ID.Slot == slot && ar.ID.Subnet == 0 && ar.Node == 1 {
					got[ar.ID.Attester] = true
				}
			}
			for val, subnet := range a.FinalitySubnetOf {
				if subnet == 0 && a.FinalityRoundOf[val] == slot%2 && !got[val] {
					t.Fatalf("host 1 missing slot-%d subnet-0 vote of val %d (got %v)", slot, val, keys(got))
				}
			}
		}
		// And it published both rounds' aggregates.
		assertRoundAggregateCoverage(t, a, rec, 2)
		assertRoundAggregateCoverage(t, a, rec, 3)
	})
}
