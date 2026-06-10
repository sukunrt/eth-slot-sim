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

// segregatedFCAssignment is decoupledFCAssignment's segregation twin: the per-validator round
// draw roundOf joins subnetOf (V = len = len(subnetOf); v's host is v % N), the (round, subnet)
// cell counts are accumulated from the two, and aggregators — slotAggs[slot][subnet] = validator
// ids — ride EVERY slot (each AC slot is a round). slotAggs may be nil (no aggregation phase:
// isolates the vote, like the base fixture).
func segregatedFCAssignment(
	n, k int, subnets [][]int, subnetOf, roundOf []int, slotAggs [][][]int, proposer, numSlots int,
) *schedule.Assignment {
	vps := make([]int, len(subnets))
	for _, s := range subnetOf {
		vps[s]++
	}
	vprs := make([][]int, k)
	for r := range vprs {
		vprs[r] = make([]int, len(subnets))
	}
	for v, s := range subnetOf {
		vprs[roundOf[v]][s]++
	}
	slots := make([]schedule.SlotPlan, numSlots)
	for s := range slots {
		slots[s] = schedule.SlotPlan{Slot: s, Proposer: proposer}
		if slotAggs != nil {
			refs := make([][]schedule.AttesterRef, len(subnets))
			for subnet, vals := range slotAggs[s] {
				for pos, v := range vals {
					refs[subnet] = append(refs[subnet],
						schedule.AttesterRef{Node: v % n, Val: v, Subnet: subnet, Position: pos})
				}
			}
			slots[s].FinalityAggregators = refs
		}
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: len(subnetOf), AcSlotsPerFinalitySlot: k, FsSubnets: len(subnets), NumSlots: numSlots,
		},
		FinalitySubscribers:      subnets,
		FinalitySubnetOf:         subnetOf,
		ValidatorsPerSubnet:      vps,
		FinalityRoundOf:          roundOf,
		ValidatorsPerRoundSubnet: vprs,
		Slots:                    slots,
	}
}

// svExpectedReceivers is the expected receiver set of validator val's round vote in AC slot
// `slot`: the stable members of its drawn subnet ∪ THAT SLOT's aggregator host nodes for the
// subnet ∪ the NEXT slot's aggregator hosts — those pre-join (Subscribe) at beginSlot(slot),
// before the vote publishes at +fc_vote_offset, so they hear the current round's tail by
// construction (the per-slot cadence's inherent overlap; in base mode the next pre-join always
// lands after the votes). Minus the publishing host when it is in the set.
func svExpectedReceivers(a *schedule.Assignment, slot, val int) map[int]bool {
	subnet := a.FinalitySubnetOf[val]
	want := map[int]bool{}
	for _, nd := range a.FinalitySubscribersOf(subnet) {
		want[nd] = true
	}
	for s := slot; s <= slot+1 && s < len(a.Slots); s++ {
		if refs := a.Slots[s].FinalityAggregators; refs != nil {
			for _, r := range refs[subnet] {
				want[r.Node] = true
			}
		}
	}
	delete(want, val%a.Params.N)
	return want
}

// assertRoundVoteCoverage checks the segregation invariant for AC slot `slot`: ONLY validators
// whose round is slot % k have votes there, and each reaches exactly svExpectedReceivers — no
// missing, no dup, no leak, no out-of-round vote.
func assertRoundVoteCoverage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	k := a.Params.AcSlotsPerFinalitySlot
	got := map[fvKey]map[int]bool{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindFinalityVote || ar.ID.Slot != slot {
			continue
		}
		if a.FinalityRoundOf[ar.ID.Attester] != slot%k {
			t.Fatalf("slot %d carries a vote from val %d of round %d (want only round %d)",
				slot, ar.ID.Attester, a.FinalityRoundOf[ar.ID.Attester], slot%k)
		}
		key := fvKey{ar.ID.Subnet, ar.ID.Attester}
		if got[key] == nil {
			got[key] = map[int]bool{}
		}
		if got[key][ar.Node] {
			t.Fatalf("duplicate round vote subnet %d val %d at node %d", key.subnet, key.val, ar.Node)
		}
		got[key][ar.Node] = true
	}
	for val, subnet := range a.FinalitySubnetOf {
		g := got[fvKey{subnet, val}]
		if a.FinalityRoundOf[val] != slot%k {
			if g != nil {
				t.Fatalf("val %d (round %d) voted in slot %d", val, a.FinalityRoundOf[val], slot)
			}
			continue
		}
		want := svExpectedReceivers(a, slot, val)
		for rcv := range g {
			if !want[rcv] {
				t.Fatalf("round vote subnet %d val %d leaked to %d", subnet, val, rcv)
			}
		}
		if len(g) != len(want) {
			t.Fatalf("round vote subnet %d val %d: got %d receivers %v, want %d %v",
				subnet, val, len(g), keys(g), len(want), keys(want))
		}
	}
}

// In AC slot s, ONLY the validators whose drawn round is s % k emit finality votes — each on its
// validator's drawn subnet, reaching exactly the subnet's other members — and over one full
// finality slot (k AC slots) every validator votes exactly once. Both rounds populate both
// subnets here, so each slot carries a member publish and a fan-out publish per node.
func TestSegregatedRoundVoteCoverage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 8 nodes, member partition evens/odds. V=16, host = v%8; vals 0..7 vote subnet 0,
		// vals 8..15 subnet 1; round = val % 2 ⇒ every (round, subnet) cell holds 4 validators.
		subnetOf := make([]int, 16)
		roundOf := make([]int, 16)
		for v := range subnetOf {
			subnetOf[v] = v / 8
			roundOf[v] = v % 2
		}
		a := segregatedFCAssignment(
			8, 2, [][]int{{0, 2, 4, 6}, {1, 3, 5, 7}}, subnetOf, roundOf, nil, 0, 2)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, Segregated: true, RoundAggFraction: 67}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 4, dc)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 2) // one full finality slot = rounds 0 and 1

		assertRoundVoteCoverage(t, a, rec, 0)
		assertRoundVoteCoverage(t, a, rec, 1)

		// Over the finality slot every validator's vote shows up in EXACTLY its round's slot.
		votedIn := map[int][]int{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind != node.KindFinalityVote {
				continue
			}
			if !slices.Contains(votedIn[ar.ID.Attester], ar.ID.Slot) {
				votedIn[ar.ID.Attester] = append(votedIn[ar.ID.Attester], ar.ID.Slot)
			}
		}
		for val, round := range roundOf {
			if !slices.Equal(votedIn[val], []int{round}) {
				t.Fatalf("val %d voted in slots %v, want exactly its round slot [%d]",
					val, votedIn[val], round)
			}
		}

		// Per slot: 4 member votes reach 3, 4 fan-out votes reach 4 ⇒ 28 arrivals each.
		for slot := range 2 {
			want := 0
			for val := range a.FinalitySubnetOf {
				if roundOf[val] == slot%2 {
					want += len(svExpectedReceivers(a, slot, val))
				}
			}
			if want != 28 {
				t.Fatalf("fixture drifted: slot %d expected-receiver total = %d, want 28", slot, want)
			}
			got := 0
			for _, ar := range rec.Arrivals() {
				if ar.ID.Kind == node.KindFinalityVote && ar.ID.Slot == slot {
					got++
				}
			}
			if got != want {
				t.Fatalf("slot %d round vote arrivals = %d, want %d", slot, got, want)
			}
		}
	})
}

// A node whose round validators all drew foreign subnets fans out per slot: members {0,1} vs
// {2,3} with every validator drawn onto the opposite subnet, one validator per (round, subnet)
// cell — each slot carries exactly two fan-out votes.
func TestSegregatedRoundVoteAllFanout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 4 nodes, V=4 (host = val): vals 0,1 vote subnet 1 (hosts members of 0), vals 2,3 vote
		// subnet 0; rounds: val 0,2 → round 0; val 1,3 → round 1.
		a := segregatedFCAssignment(
			4, 2, [][]int{{0, 1}, {2, 3}}, []int{1, 1, 0, 0}, []int{0, 1, 0, 1}, nil, 0, 2)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, Segregated: true, RoundAggFraction: 67}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 3, dc)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 2)

		assertRoundVoteCoverage(t, a, rec, 0)
		assertRoundVoteCoverage(t, a, rec, 1)
		// Each slot: 2 fan-out votes × 2 foreign members = 4 arrivals, all at members.
		for slot := range 2 {
			got := 0
			for _, ar := range rec.Arrivals() {
				if ar.ID.Kind == node.KindFinalityVote && ar.ID.Slot == slot {
					got++
					if !slices.Contains(a.FinalitySubscribersOf(ar.ID.Subnet), ar.Node) {
						t.Fatalf("vote on subnet %d arrived at non-member %d", ar.ID.Subnet, ar.Node)
					}
				}
			}
			if got != 4 {
				t.Fatalf("slot %d round vote arrivals = %d, want 4 (all fan-out)", slot, got)
			}
		}
	})
}
