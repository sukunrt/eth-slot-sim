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

// decoupledFCAssignment builds an N-node decoupled assignment focused on the finality-chain vote:
// the given finality-subnet member partition (the stable receiver core), the per-VALIDATOR subnet
// map subnetOf (V = len(subnetOf); v's host is v % N), and k AC slots per finality slot. No
// columns/AC voters — the FC vote is un-gated and fixed-time, so this isolates it. Node `proposer`
// publishes the (irrelevant) block.
func decoupledFCAssignment(
	n, k int, subnets [][]int, subnetOf []int, proposer, numSlots int,
) *schedule.Assignment {
	vps := make([]int, len(subnets))
	for _, s := range subnetOf {
		vps[s]++
	}
	slots := make([]schedule.SlotPlan, numSlots)
	for s := range slots {
		slots[s] = schedule.SlotPlan{Slot: s, Proposer: proposer}
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: len(subnetOf), AcSlotsPerFinalitySlot: k, FsSubnets: len(subnets), NumSlots: numSlots,
		},
		FinalitySubscribers: subnets,
		FinalitySubnetOf:    subnetOf,
		ValidatorsPerSubnet: vps,
		Slots:               slots,
	}
}

// fvKey identifies one published finality vote across its arrivals: its subnet and the validator
// that voted (the validator rides Attester; the publishing node is its host under uniform v%N).
type fvKey struct{ subnet, val int }

// fvExpectedReceivers is the expected receiver set of validator val's finality vote: the stable
// members of its drawn subnet ∪ the finality slot's aggregator host nodes for that subnet (they
// subscribe it from the pre-join slot through the aggregation deadline — collecting these votes is
// their job), minus the publishing host when it is in the set.
func fvExpectedReceivers(a *schedule.Assignment, fslot, val int) map[int]bool {
	subnet := a.FinalitySubnetOf[val]
	host := val % a.Params.N
	want := map[int]bool{}
	for _, nd := range a.FinalitySubscribersOf(subnet) {
		want[nd] = true
	}
	boundary := fslot * a.Params.AcSlotsPerFinalitySlot
	if refs := a.Slots[boundary].FinalityAggregators; refs != nil {
		for _, r := range refs[subnet] {
			want[r.Node] = true
		}
	}
	delete(want, host)
	return want
}

// assertFinalityVoteCoverage checks the per-validator invariant: each validator's finality vote
// reaches exactly fvExpectedReceivers — no missing (aggregators included: a slot-n aggregator must
// hold slot n's votes), no dup, no leak to a non-member non-aggregator.
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
	for val, subnet := range a.FinalitySubnetOf {
		want := fvExpectedReceivers(a, fslot, val)
		g := got[fvKey{subnet, val}]
		for rcv := range g {
			if !want[rcv] {
				t.Fatalf("finality vote subnet %d val %d leaked to %d", subnet, val, rcv)
			}
		}
		if len(g) != len(want) {
			t.Fatalf("finality subnet %d val %d: got %d receivers %v, want %d %v",
				subnet, val, len(g), keys(g), len(want), keys(want))
		}
	}
}

// At fc_vote_offset into the finality slot, every node emits one finality vote per validator it
// hosts, each on ITS VALIDATOR'S drawn subnet — fanning out (dial + Join-publish, warmed at the
// pre-join slot) where the node isn't a member. Each vote reaches exactly the subnet's other
// members; none leak. Here every node hosts a validator on BOTH subnets, so every vote on one of
// them is a non-member fan-out publish. Finality slot 0 votes at AC slot 0.
func TestDecoupledFinalityVoteCoverage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 8 nodes, member partition: evens on subnet 0, odds on subnet 1. V=16, host = v%8;
		// vals 0..7 vote subnet 0, vals 8..15 vote subnet 1 ⇒ each node votes once as a member
		// and once as a non-member (fan-out).
		subnetOf := make([]int, 16)
		for v := range subnetOf {
			subnetOf[v] = v / 8
		}
		a := decoupledFCAssignment(8, 2, [][]int{{0, 2, 4, 6}, {1, 3, 5, 7}}, subnetOf, 0, 2)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 4, dc) // peersP=4 forces relay within a subnet

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 2) // run finality slot 0 (votes at AC slot 0) plus its second AC slot

		assertFinalityVoteCoverage(t, a, rec, 0)

		// Total arrivals = Σ per vote of |expected receivers|: member votes reach 3, fan-out
		// votes reach all 4 members ⇒ 8·3 + 8·4 = 56.
		want := 0
		for val := range a.FinalitySubnetOf {
			want += len(fvExpectedReceivers(a, 0, val))
		}
		if want != 56 {
			t.Fatalf("fixture drifted: expected-receiver total = %d, want 56", want)
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

// A node whose validators all drew foreign subnets is NOT a passive member anywhere it votes:
// both its votes ride the fan-out path. Membership {0,1} vs {2,3} with all four validators of
// nodes 0,1 drawn onto the opposite subnet pins the fan-out publish end-to-end.
func TestDecoupledFinalityVoteAllFanout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 4 nodes, V=4 (host = val): members {0,1} on subnet 0, {2,3} on subnet 1; every
		// validator votes on the subnet its host is NOT a member of.
		a := decoupledFCAssignment(4, 2, [][]int{{0, 1}, {2, 3}}, []int{1, 1, 0, 0}, 0, 2)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 3, dc)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 2)

		assertFinalityVoteCoverage(t, a, rec, 0)
		// Every vote is a fan-out publish reaching BOTH members of the foreign subnet: 4·2 = 8.
		got := 0
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindFinalityVote && ar.ID.Slot == 0 {
				got++
			}
		}
		if got != 8 {
			t.Fatalf("finality vote arrivals = %d, want 8 (all fan-out)", got)
		}
		// And nothing arrived at the non-member hosts: subnet 0's votes live on nodes 0,1 only.
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindFinalityVote && ar.ID.Slot == 0 {
				if !slices.Contains(a.FinalitySubscribersOf(ar.ID.Subnet), ar.Node) {
					t.Fatalf("vote on subnet %d arrived at non-member %d", ar.ID.Subnet, ar.Node)
				}
			}
		}
	})
}
