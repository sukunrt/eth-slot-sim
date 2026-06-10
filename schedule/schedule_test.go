package schedule

import (
	"path/filepath"
	"slices"
	"testing"
)

// The contract test: a schedule.json that simctl/schedule.py actually produced
// unmarshals into the Go structs and the accessors agree with the raw data. This
// guards the Python→Go schema handoff — a renamed/missing field would leave a struct
// field zero and the self-consistency checks would fail.
func TestLoadFixtureContract(t *testing.T) {
	a, err := Load(filepath.Join("testdata", "schedule.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Knobs survived the round-trip (catches field-name drift).
	if a.Params.N != 6 || a.Params.V != 12 || a.Params.C != 2 || a.Params.Sc != 3 {
		t.Fatalf("params = %+v, want n6 v12 c2 sc3", a.Params)
	}
	if a.Params.SubscribeFloor != 2 || a.Params.NumSlots != 1 {
		t.Fatalf("params = %+v, want floor2 slots1", a.Params)
	}

	// Subscribe set: C subnets, each ≥ floor distinct sorted nodes, in range.
	if len(a.SubnetSubscribers) != a.Params.C {
		t.Fatalf("subnet_subscribers len %d, want %d", len(a.SubnetSubscribers), a.Params.C)
	}
	for subnet, members := range a.SubnetSubscribers {
		if len(members) < min(a.Params.SubscribeFloor, a.Params.N) {
			t.Fatalf("subnet %d has %d subscribers, want ≥%d", subnet, len(members), a.Params.SubscribeFloor)
		}
		if !slices.IsSorted(members) {
			t.Fatalf("subnet %d subscribers %v not sorted", subnet, members)
		}
		for _, m := range members {
			if m < 0 || m >= a.Params.N {
				t.Fatalf("subnet %d subscriber %d out of range", subnet, m)
			}
		}
	}

	if len(a.Slots) != a.Params.NumSlots {
		t.Fatalf("slots len %d, want %d", len(a.Slots), a.Params.NumSlots)
	}
	for _, sp := range a.Slots {
		if len(sp.Committees) != a.Params.C {
			t.Fatalf("slot %d committees %d, want %d", sp.Slot, len(sp.Committees), a.Params.C)
		}
		if !slices.Equal(sp.SubnetOf, []int{0, 1}) {
			t.Fatalf("slot %d subnet_of %v, want [0 1]", sp.Slot, sp.SubnetOf)
		}
		for ci, com := range sp.Committees {
			if len(com) != a.Params.Sc {
				t.Fatalf("slot %d committee %d size %d, want %d", sp.Slot, ci, len(com), a.Params.Sc)
			}
			for pos, r := range com {
				if r.Position != pos || r.Node != r.Val%a.Params.N || r.Subnet != sp.SubnetOf[ci] {
					t.Fatalf("slot %d committee %d ref %+v inconsistent", sp.Slot, ci, r)
				}
			}
		}
	}

	// Proposer survived the round-trip (one per slot, in range).
	if got, want := a.ProposerSchedule(), []int{a.Slots[0].Proposer}; !slices.Equal(got, want) {
		t.Fatalf("ProposerSchedule() = %v, want %v", got, want)
	}
	for _, sp := range a.Slots {
		if sp.Proposer < 0 || sp.Proposer >= a.Params.N {
			t.Fatalf("slot %d proposer %d out of range", sp.Slot, sp.Proposer)
		}
	}

	// Aggregators: ⊆ the subnet's subscribers, count == min(target, |subscribers|), sorted.
	if a.Params.TargetAggregators != 16 {
		t.Fatalf("params = %+v, want target_aggregators16", a.Params)
	}
	for _, sp := range a.Slots {
		if len(sp.Aggregators) != a.Params.C {
			t.Fatalf("slot %d aggregators %d, want %d", sp.Slot, len(sp.Aggregators), a.Params.C)
		}
		for ci, aggs := range sp.Aggregators {
			subs := a.SubnetSubscribers[sp.SubnetOf[ci]]
			if len(aggs) != min(a.Params.TargetAggregators, len(subs)) {
				t.Fatalf("slot %d committee %d aggregators %d, want %d", sp.Slot, ci,
					len(aggs), min(a.Params.TargetAggregators, len(subs)))
			}
			if !slices.IsSorted(aggs) {
				t.Fatalf("slot %d committee %d aggregators %v not sorted", sp.Slot, ci, aggs)
			}
			for _, ag := range aggs {
				if !slices.Contains(subs, ag) {
					t.Fatalf("slot %d committee %d aggregator %d not a subscriber", sp.Slot, ci, ag)
				}
			}
		}
	}
	// Accessors agree with the raw structure.
	for subnet := range a.Params.C {
		if got := a.Subscribers(subnet); !slices.Equal(got, a.SubnetSubscribers[subnet]) {
			t.Fatalf("Subscribers(%d) = %v, want %v", subnet, got, a.SubnetSubscribers[subnet])
		}
	}
	for node := range a.Params.N {
		v := a.Node(node)
		for slot := range a.Params.NumSlots {
			if got, want := v.AttestDuties(slot), rawDuties(a, node, slot); !slices.Equal(got, want) {
				t.Fatalf("node %d slot %d duties = %v, want %v", node, slot, got, want)
			}
		}
		for _, subnet := range v.SubscribedSubnets() {
			if !slices.Contains(a.SubnetSubscribers[subnet], node) {
				t.Fatalf("node %d SubscribedSubnets includes %d but isn't a member", node, subnet)
			}
		}
	}
}

// TestLoadDecoupledFixtureContract guards the Python→Go schema handoff for the decoupled-consensus
// fields: a schedule.json that simctl/schedule.py actually produced (decoupled mode) unmarshals
// into the new structs and the accessors agree. A renamed/missing json tag would leave a field zero.
func TestLoadDecoupledFixtureContract(t *testing.T) {
	a, err := Load(filepath.Join("testdata", "decoupled_schedule.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p := a.Params
	if p.N != 12 || p.V != 24 || p.AcVoteSize != 4 || p.AcSlotsPerFinalitySlot != 2 ||
		p.FsSubnets != 3 || p.FsAggregators != 2 {
		t.Fatalf("params = %+v, want n12 v24 acVote4 k2 fs3 fsAgg2", p)
	}

	// Finality subnets partition all N nodes (every node on exactly one) — the receiver core.
	var flat []int
	for _, members := range a.FinalitySubscribers {
		flat = append(flat, members...)
	}
	slices.Sort(flat)
	want := make([]int, p.N)
	for i := range want {
		want[i] = i
	}
	if !slices.Equal(flat, want) {
		t.Fatalf("finality subnets not a partition of N: %v", flat)
	}
	// FinalitySubnetOf partitions the VALIDATOR set: one subnet per validator, in range, and
	// ValidatorsPerSubnet is exactly that draw's per-subnet counts.
	if len(a.FinalitySubnetOf) != p.V {
		t.Fatalf("finality_subnet_of len %d, want V=%d", len(a.FinalitySubnetOf), p.V)
	}
	counts := make([]int, p.FsSubnets)
	for val, s := range a.FinalitySubnetOf {
		if s < 0 || s >= p.FsSubnets {
			t.Fatalf("finality_subnet_of[%d] = %d out of range", val, s)
		}
		counts[s]++
	}
	if !slices.Equal(counts, a.ValidatorsPerSubnet) {
		t.Fatalf("validators_per_subnet %v != draw counts %v", a.ValidatorsPerSubnet, counts)
	}

	for _, sp := range a.Slots {
		if len(sp.ACVoters) != p.AcVoteSize {
			t.Fatalf("slot %d ac_voters = %d, want %d", sp.Slot, len(sp.ACVoters), p.AcVoteSize)
		}
		if len(sp.Committees) != 0 {
			t.Fatalf("slot %d has committees under decoupled", sp.Slot)
		}
		for _, r := range sp.ACVoters {
			if r.Node != r.Val%p.N {
				t.Fatalf("slot %d ac voter %+v: node != val%%N", sp.Slot, r)
			}
		}
		if sp.Slot%p.AcSlotsPerFinalitySlot == 0 { // finality boundary
			if len(sp.FinalityAggregators) != p.FsSubnets {
				t.Fatalf("boundary slot %d aggregators = %d, want %d", sp.Slot, len(sp.FinalityAggregators), p.FsSubnets)
			}
			// Aggregators are VALIDATOR refs from the entire set; the host node carries the duty.
			for subnet, aggs := range sp.FinalityAggregators {
				if len(aggs) != min(p.FsAggregators, p.V) {
					t.Fatalf("boundary slot %d subnet %d aggregators = %d, want %d",
						sp.Slot, subnet, len(aggs), min(p.FsAggregators, p.V))
				}
				for _, r := range aggs {
					if r.Node != r.Val%p.N || r.Subnet != subnet {
						t.Fatalf("boundary slot %d subnet %d aggregator %+v inconsistent", sp.Slot, subnet, r)
					}
				}
			}
		} else if sp.FinalityAggregators != nil {
			t.Fatalf("non-boundary slot %d carries finality_aggregators", sp.Slot)
		}
	}

	// Accessors agree with the raw structure.
	for node := range p.N {
		s, member := a.Node(node).FinalitySubnet()
		if !member || !slices.Contains(a.FinalitySubscribers[s], node) {
			t.Fatalf("node %d FinalitySubnet = (%d,%v) inconsistent with FinalitySubscribers", node, s, member)
		}
		// One duty per hosted validator (uniform v%N here), on its drawn subnet, at its cell
		// rank (the rank itself is pinned by position_test.go).
		duties := a.Node(node).FinalityVoteDuties(0) // base mode: the slot arg is ignored
		var wantDuties []AttestDuty
		for val := node; val < p.V; val += p.N {
			wantDuties = append(wantDuties, AttestDuty{
				Subnet: a.FinalitySubnetOf[val], Val: val, Position: a.FinalityPosition(val)})
		}
		if !slices.Equal(duties, wantDuties) {
			t.Fatalf("node %d FinalityVoteDuties = %v, want %v", node, duties, wantDuties)
		}
	}
	// FinalityAggregations agrees with the boundary slot's refs (deduped to subnets).
	for n := 0; n*p.AcSlotsPerFinalitySlot < len(a.Slots); n++ {
		refs := a.Slots[n*p.AcSlotsPerFinalitySlot].FinalityAggregators
		for node := range p.N {
			var want []int
			for subnet, aggs := range refs {
				for _, r := range aggs {
					if r.Node == node && !slices.Contains(want, subnet) {
						want = append(want, subnet)
					}
				}
			}
			if got := a.Node(node).FinalityAggregations(n); !slices.Equal(got, want) {
				t.Fatalf("node %d FinalityAggregations(%d) = %v, want %v", node, n, got, want)
			}
		}
	}
}

// TestLoadSegregatedFixtureContract guards the Python→Go schema handoff for the
// validator-segregation fields: a schedule.json produced by simctl/schedule.py with
// validator_segregation on unmarshals into the new fields and the slot-keyed accessors agree.
func TestLoadSegregatedFixtureContract(t *testing.T) {
	a, err := Load(filepath.Join("testdata", "segregated_schedule.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p := a.Params
	if p.N != 12 || p.V != 24 || p.AcSlotsPerFinalitySlot != 2 || p.FsSubnets != 3 ||
		p.FsAggregators != 2 {
		t.Fatalf("params = %+v, want n12 v24 k2 fs3 fsAgg2", p)
	}
	k := p.AcSlotsPerFinalitySlot

	// Presence of finality_round_of IS the variant detection.
	if !a.Segregated() {
		t.Fatal("Segregated() = false on a segregated schedule")
	}

	// FinalityRoundOf partitions the VALIDATOR set into k rounds, and
	// ValidatorsPerRoundSubnet is exactly the two draws' (round, subnet) cell counts.
	if len(a.FinalityRoundOf) != p.V {
		t.Fatalf("finality_round_of len %d, want V=%d", len(a.FinalityRoundOf), p.V)
	}
	cells := make([][]int, k)
	for r := range cells {
		cells[r] = make([]int, p.FsSubnets)
	}
	for val, r := range a.FinalityRoundOf {
		if r < 0 || r >= k {
			t.Fatalf("finality_round_of[%d] = %d out of range", val, r)
		}
		cells[r][a.FinalitySubnetOf[val]]++
	}
	if len(a.ValidatorsPerRoundSubnet) != k {
		t.Fatalf("validators_per_round_subnet rows = %d, want k=%d",
			len(a.ValidatorsPerRoundSubnet), k)
	}
	for r := range cells {
		if !slices.Equal(cells[r], a.ValidatorsPerRoundSubnet[r]) {
			t.Fatalf("validators_per_round_subnet[%d] = %v, want draw counts %v",
				r, a.ValidatorsPerRoundSubnet[r], cells[r])
		}
	}
	for s := range p.FsSubnets { // column sums = the subnet draw's counts
		sum := 0
		for r := range k {
			sum += a.ValidatorsPerRoundSubnet[r][s]
		}
		if sum != a.ValidatorsPerSubnet[s] {
			t.Fatalf("subnet %d round-cell sum %d != validators_per_subnet %d",
				s, sum, a.ValidatorsPerSubnet[s])
		}
	}

	// EVERY slot carries a fresh per-round aggregator draw (not just boundaries).
	for _, sp := range a.Slots {
		if len(sp.FinalityAggregators) != p.FsSubnets {
			t.Fatalf("slot %d aggregators = %d, want %d (every slot is a round)",
				sp.Slot, len(sp.FinalityAggregators), p.FsSubnets)
		}
		for subnet, aggs := range sp.FinalityAggregators {
			if len(aggs) != min(p.FsAggregators, p.V) {
				t.Fatalf("slot %d subnet %d aggregators = %d, want %d",
					sp.Slot, subnet, len(aggs), min(p.FsAggregators, p.V))
			}
			for _, r := range aggs {
				if r.Node != r.Val%p.N || r.Subnet != subnet {
					t.Fatalf("slot %d subnet %d aggregator %+v inconsistent", sp.Slot, subnet, r)
				}
			}
		}
	}

	// FinalityVoteDuties(s): exactly the hosted validators whose round is s % k, on their
	// drawn subnets. Over one full finality slot every hosted validator appears exactly once.
	for node := range p.N {
		for slot := range len(a.Slots) {
			var want []AttestDuty
			for val := node; val < p.V; val += p.N {
				if a.FinalityRoundOf[val] == slot%k {
					want = append(want, AttestDuty{
						Subnet: a.FinalitySubnetOf[val], Val: val, Position: a.FinalityPosition(val)})
				}
			}
			if got := a.Node(node).FinalityVoteDuties(slot); !slices.Equal(got, want) {
				t.Fatalf("node %d FinalityVoteDuties(%d) = %v, want %v", node, slot, got, want)
			}
		}
		var fslotVals []int
		for slot := range k {
			for _, d := range a.Node(node).FinalityVoteDuties(slot) {
				fslotVals = append(fslotVals, d.Val)
			}
		}
		slices.Sort(fslotVals)
		var hosted []int
		for val := node; val < p.V; val += p.N {
			hosted = append(hosted, val)
		}
		if !slices.Equal(fslotVals, hosted) {
			t.Fatalf("node %d votes %v over a finality slot, want every hosted validator once %v",
				node, fslotVals, hosted)
		}
	}

	// FinalityAggregations(s) is keyed by the AC slot itself: slot s's refs, deduped to subnets.
	for slot := range len(a.Slots) {
		refs := a.Slots[slot].FinalityAggregators
		for node := range p.N {
			var want []int
			for subnet, aggs := range refs {
				for _, r := range aggs {
					if r.Node == node && !slices.Contains(want, subnet) {
						want = append(want, subnet)
					}
				}
			}
			if got := a.Node(node).FinalityAggregations(slot); !slices.Equal(got, want) {
				t.Fatalf("node %d FinalityAggregations(%d) = %v, want %v", node, slot, got, want)
			}
		}
	}
}

// Under segregation FinalityVoteDuties filters the hosted validators by round (slot % k), in
// both the uniform and the ValidatorCounts hosting models; a base (non-segregated) assignment
// ignores the slot argument entirely. Duty positions are the (subnet[, round]) cell ranks
// (pinned in position_test.go). Each section builds a fresh Assignment — the rank tables are
// built once per Assignment, so the draws must not be mutated after first use.
func TestSegregatedFinalityVoteDuties(t *testing.T) {
	segregated := func() *Assignment {
		return &Assignment{
			Params:           Params{N: 3, V: 6, AcSlotsPerFinalitySlot: 2},
			FinalitySubnetOf: []int{1, 0, 0, 1, 0, 1},
			FinalityRoundOf:  []int{0, 1, 0, 0, 1, 1},
			// Cells: (s1,r0)={0,3}, (s0,r1)={1,4}, (s0,r0)={2}, (s1,r1)={5}.
		}
	}
	// Uniform hosting (v % N): node 0 hosts vals 0 (round 0) and 3 (round 0).
	a := segregated()
	if got := a.Node(0).FinalityVoteDuties(0); !slices.Equal(got, []AttestDuty{
		{Subnet: 1, Val: 0, Position: 0}, {Subnet: 1, Val: 3, Position: 1},
	}) {
		t.Fatalf("node0 FinalityVoteDuties(0) = %v, want [(1,0,p0) (1,3,p1)]", got)
	}
	if got := a.Node(0).FinalityVoteDuties(1); got != nil {
		t.Fatalf("node0 FinalityVoteDuties(1) = %v, want nil (both vals are round 0)", got)
	}
	// Round arithmetic uses slot % k: slot 2 is round 0 again.
	if got := a.Node(0).FinalityVoteDuties(2); len(got) != 2 {
		t.Fatalf("node0 FinalityVoteDuties(2) = %v, want round-0 duties again", got)
	}
	// ValidatorCounts hosting: node 0 hosts vals 0,1,2 — rounds 0,1,0.
	a = segregated()
	a.ValidatorCounts = []int{3, 1, 2}
	if got := a.Node(0).FinalityVoteDuties(1); !slices.Equal(got, []AttestDuty{
		{Subnet: 0, Val: 1, Position: 0},
	}) {
		t.Fatalf("node0 counts FinalityVoteDuties(1) = %v, want [(0,1,p0)]", got)
	}
	if got := a.Node(0).FinalityVoteDuties(0); !slices.Equal(got, []AttestDuty{
		{Subnet: 1, Val: 0, Position: 0}, {Subnet: 0, Val: 2, Position: 0},
	}) {
		t.Fatalf("node0 counts FinalityVoteDuties(0) = %v, want [(1,0,p0) (0,2,p0)]", got)
	}
	// Base mode (no FinalityRoundOf): the slot argument is ignored — all duties, any slot;
	// positions are subnet-wide ranks (subnet 1 cell = {0,3,5}).
	a = segregated()
	a.FinalityRoundOf = nil
	for _, slot := range []int{0, 1, 7} {
		if got := a.Node(0).FinalityVoteDuties(slot); !slices.Equal(got, []AttestDuty{
			{Subnet: 1, Val: 0, Position: 0}, {Subnet: 1, Val: 3, Position: 1},
		}) {
			t.Fatalf("base node0 FinalityVoteDuties(%d) = %v, want all duties", slot, got)
		}
	}
}

// ProposerSchedule exposes the per-slot block proposer the Python generator wrote (a
// supernode), one entry per slot, in slot order.
func TestProposerSchedule(t *testing.T) {
	a := &Assignment{
		Params: Params{N: 6, NumSlots: 3},
		Slots: []SlotPlan{
			{Slot: 0, Proposer: 5},
			{Slot: 1, Proposer: 2},
			{Slot: 2, Proposer: 5},
		},
	}
	if got, want := a.ProposerSchedule(), []int{5, 2, 5}; !slices.Equal(got, want) {
		t.Fatalf("ProposerSchedule() = %v, want %v", got, want)
	}
}

func rawDuties(a *Assignment, node, slot int) []AttestDuty {
	var out []AttestDuty
	for _, com := range a.Slots[slot].Committees {
		for _, r := range com {
			if r.Node == node {
				out = append(out, AttestDuty{Subnet: r.Subnet, Val: r.Val, Position: r.Position})
			}
		}
	}
	return out
}

// The exported structs are the unmarshal target, so tests can build an arbitrary
// committee as an in-memory literal — no Python, no file — and exercise the accessors.
func TestLiteralAssignmentAccessors(t *testing.T) {
	a := &Assignment{
		Params:            Params{N: 4, V: 8, C: 2, Sc: 2, SubnetCount: 64, SubnetsPerNode: 1, NumSlots: 1},
		SubnetSubscribers: [][]int{{1, 2, 3}, {0, 2}},
		Slots: []SlotPlan{{
			Slot: 0,
			Committees: [][]AttesterRef{
				{{Node: 0, Val: 0, Subnet: 0, Position: 0}, {Node: 0, Val: 4, Subnet: 0, Position: 1}},
				{{Node: 1, Val: 1, Subnet: 1, Position: 0}, {Node: 2, Val: 2, Subnet: 1, Position: 1}},
			},
			SubnetOf: []int{0, 1},
		}},
	}

	// k>1: node 0 holds two attesters on subnet 0 ⇒ two duties.
	if got := a.Node(0).AttestDuties(0); len(got) != 2 {
		t.Fatalf("node0 duties = %v, want 2 (k>1 on one subnet)", got)
	}
	if got := a.Subscribers(0); !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("Subscribers(0) = %v, want [1 2 3]", got)
	}
	if got := a.Node(2).SubscribedSubnets(); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("node2 SubscribedSubnets = %v, want [0 1]", got)
	}
	if got := a.Node(0).SubscribedSubnets(); !slices.Equal(got, []int{1}) {
		t.Fatalf("node0 SubscribedSubnets = %v, want [1]", got)
	}
}

// CustodyColumns / IsFullCustody / ColumnSubscribersOf expose the data-column custody set
// carried in schedule.json: a full-custody node holds every column (the relay backbone); an
// ordinary node holds its seeded-random membership in column_subscribers.
func TestCustodyColumns(t *testing.T) {
	a := &Assignment{
		Params:     Params{N: 5, V: 8, NumSlots: 1},
		NumColumns: 4,
		// full-custody nodes 0,1 hold all columns; ordinary 2,3,4 drew subsets.
		FullCustody: []int{0, 1},
		ColumnSubscribers: [][]int{
			{0, 1, 2},    // column 0
			{0, 1, 3},    // column 1
			{0, 1, 2, 4}, // column 2
			{0, 1, 4},    // column 3
		},
		Slots: []SlotPlan{{Slot: 0, Proposer: 0}},
	}

	if !a.Node(0).IsFullCustody() || !a.Node(1).IsFullCustody() {
		t.Fatal("nodes 0,1 should be full-custody")
	}
	if a.Node(2).IsFullCustody() {
		t.Fatal("node 2 should not be full-custody")
	}
	// Full-custody node holds every column.
	if got := a.Node(0).CustodyColumns(); !slices.Equal(got, []int{0, 1, 2, 3}) {
		t.Fatalf("node0 custody = %v, want all 4 columns", got)
	}
	// Ordinary nodes hold their column_subscribers membership (sorted).
	if got := a.Node(2).CustodyColumns(); !slices.Equal(got, []int{0, 2}) {
		t.Fatalf("node2 custody = %v, want [0 2]", got)
	}
	if got := a.Node(4).CustodyColumns(); !slices.Equal(got, []int{2, 3}) {
		t.Fatalf("node4 custody = %v, want [2 3]", got)
	}
	if got := a.Node(3).CustodyColumns(); !slices.Equal(got, []int{1}) {
		t.Fatalf("node3 custody = %v, want [1]", got)
	}
	// ColumnSubscribersOf returns a column's custodier set; out-of-range ⇒ nil.
	if got := a.ColumnSubscribersOf(2); !slices.Equal(got, []int{0, 1, 2, 4}) {
		t.Fatalf("ColumnSubscribersOf(2) = %v, want [0 1 2 4]", got)
	}
	if got := a.ColumnSubscribersOf(99); got != nil {
		t.Fatalf("ColumnSubscribersOf(99) = %v, want nil", got)
	}
}

// SyncSubnet / SyncSubscribersOf / SyncAggregateSubnets expose the sync-committee membership
// carried in schedule.json: a member is on exactly one subnet (round-robin), and the per-slot
// sync aggregators are drawn from a subnet's members.
func TestSyncMembership(t *testing.T) {
	a := &Assignment{
		Params: Params{N: 6, NumSlots: 1},
		// 4 members: subnet 0 = {0,2}, subnet 1 = {4,5}; nodes 1,3 are non-members.
		SyncSubscribers: [][]int{{0, 2}, {4, 5}},
		Slots: []SlotPlan{{
			Slot:            0,
			SyncAggregators: [][]int{{0}, {4, 5}}, // subnet 0 → {0}; subnet 1 → {4,5}
		}},
	}
	// SyncSubnet: members map to their one subnet; non-members return (-1,false).
	for node, want := range map[int]int{0: 0, 2: 0, 4: 1, 5: 1} {
		if got, member := a.Node(node).SyncSubnet(); !member || got != want {
			t.Fatalf("node %d SyncSubnet = (%d,%v), want (%d,true)", node, got, member, want)
		}
	}
	for _, node := range []int{1, 3} {
		if got, member := a.Node(node).SyncSubnet(); member || got != -1 {
			t.Fatalf("node %d SyncSubnet = (%d,%v), want (-1,false)", node, got, member)
		}
	}
	// SyncSubscribersOf: a subnet's member set; out-of-range ⇒ nil.
	if got := a.SyncSubscribersOf(1); !slices.Equal(got, []int{4, 5}) {
		t.Fatalf("SyncSubscribersOf(1) = %v, want [4 5]", got)
	}
	if got := a.SyncSubscribersOf(9); got != nil {
		t.Fatalf("SyncSubscribersOf(9) = %v, want nil", got)
	}
	// SyncAggregateSubnets: the subnets whose aggregator set includes the node.
	if got := a.Node(5).SyncAggregateSubnets(0); !slices.Equal(got, []int{1}) {
		t.Fatalf("node5 SyncAggregateSubnets = %v, want [1]", got)
	}
	if got := a.Node(0).SyncAggregateSubnets(0); !slices.Equal(got, []int{0}) {
		t.Fatalf("node0 SyncAggregateSubnets = %v, want [0]", got)
	}
	// A member that isn't an aggregator this slot aggregates nothing.
	if got := a.Node(2).SyncAggregateSubnets(0); got != nil {
		t.Fatalf("node2 SyncAggregateSubnets = %v, want nil", got)
	}
}

// ACVoteDuties / FinalitySubnet / FinalityVoteDuties / FinalitySubscribersOf / FinalityAggregations
// expose the decoupled-consensus membership carried in schedule.json: a per-slot flat AC-voter set
// (no subnets), a node-partition into finality subnets (the stable receiver core), a VALIDATOR
// partition (finality_subnet_of — each hosted validator votes on its drawn subnet), and the
// per-finality-slot aggregator refs on the boundary slot (validators from the entire set; the host
// node carries the duty).
func TestDecoupledMembership(t *testing.T) {
	a := &Assignment{
		Params: Params{N: 6, V: 12, AcSlotsPerFinalitySlot: 2, NumSlots: 2},
		// Finality subnets partition all 6 nodes: subnet 0 = {0,2,4}, subnet 1 = {1,3,5}.
		FinalitySubscribers: [][]int{{0, 2, 4}, {1, 3, 5}},
		// The validator partition: val → subnet, independent of hosting.
		FinalitySubnetOf:    []int{0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 0, 1},
		ValidatorsPerSubnet: []int{6, 6},
		Slots: []SlotPlan{
			{
				Slot:     0,
				ACVoters: []AttesterRef{{Node: 0, Val: 0}, {Node: 1, Val: 7}, {Node: 0, Val: 6}},
				// Boundary slot (0 % 2 == 0). Aggregator refs: node 0 aggregates subnet 0 and —
				// via two of its validators (dedup) — subnet 1; node 3 aggregates subnet 1. Node 0
				// is NOT a member of subnet 1: aggregators come from the whole validator set.
				FinalityAggregators: [][]AttesterRef{
					{{Node: 0, Val: 0, Subnet: 0}},
					{{Node: 3, Val: 9, Subnet: 1}, {Node: 0, Val: 6, Subnet: 1}, {Node: 0, Val: 0, Subnet: 1}},
				},
			},
			{Slot: 1, ACVoters: []AttesterRef{{Node: 2, Val: 2}}},
		},
	}

	// ACVoteDuties: node 0 votes vals 0 and 6 in slot 0 (k>1), node 1 votes val 7; non-voters get nil.
	if got := a.Node(0).ACVoteDuties(0); len(got) != 2 || got[0].Val != 0 || got[1].Val != 6 {
		t.Fatalf("node0 ACVoteDuties(0) = %+v, want vals [0 6]", got)
	}
	if got := a.Node(2).ACVoteDuties(0); got != nil {
		t.Fatalf("node2 ACVoteDuties(0) = %+v, want nil (not a voter)", got)
	}
	if got := a.Node(2).ACVoteDuties(1); len(got) != 1 || got[0].Val != 2 {
		t.Fatalf("node2 ACVoteDuties(1) = %+v, want val [2]", got)
	}

	// FinalitySubnet: every node maps to its one subnet; there are no non-members (partition).
	for node, want := range map[int]int{0: 0, 2: 0, 4: 0, 1: 1, 3: 1, 5: 1} {
		if got, member := a.Node(node).FinalitySubnet(); !member || got != want {
			t.Fatalf("node %d FinalitySubnet = (%d,%v), want (%d,true)", node, got, member, want)
		}
	}

	// FinalityVoteDuties: one (val, subnet) pair per hosted validator (uniform v%N), the subnet
	// from finality_subnet_of — a node's keys can land on several subnets. Positions are the
	// subnet cells' ranks: subnet0 = {0,2,4,7,8,10}, subnet1 = {1,3,5,6,9,11}.
	if got := a.Node(0).FinalityVoteDuties(0); !slices.Equal(got, []AttestDuty{
		{Subnet: 0, Val: 0, Position: 0}, {Subnet: 1, Val: 6, Position: 3},
	}) {
		t.Fatalf("node0 FinalityVoteDuties = %v, want [(0,0,p0) (1,6,p3)]", got)
	}
	if got := a.Node(5).FinalityVoteDuties(0); !slices.Equal(got, []AttestDuty{
		{Subnet: 1, Val: 5, Position: 2}, {Subnet: 1, Val: 11, Position: 5},
	}) {
		t.Fatalf("node5 FinalityVoteDuties = %v, want [(1,5,p2) (1,11,p5)]", got)
	}

	// FinalitySubscribersOf: a subnet's member set; out-of-range ⇒ nil.
	if got := a.FinalitySubscribersOf(1); !slices.Equal(got, []int{1, 3, 5}) {
		t.Fatalf("FinalitySubscribersOf(1) = %v, want [1 3 5]", got)
	}
	if got := a.FinalitySubscribersOf(9); got != nil {
		t.Fatalf("FinalitySubscribersOf(9) = %v, want nil", got)
	}

	// FinalityAggregations(0): the boundary slot is 0*k=0. Node 0 aggregates both subnets (its
	// two refs on subnet 1 dedup to one duty); node 3 only subnet 1; node 2 none.
	if got := a.Node(0).FinalityAggregations(0); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("node0 FinalityAggregations(0) = %v, want [0 1]", got)
	}
	if got := a.Node(3).FinalityAggregations(0); !slices.Equal(got, []int{1}) {
		t.Fatalf("node3 FinalityAggregations(0) = %v, want [1]", got)
	}
	if got := a.Node(2).FinalityAggregations(0); got != nil {
		t.Fatalf("node2 FinalityAggregations(0) = %v, want nil", got)
	}
	if got := a.Node(0).FinalityAggregations(7); got != nil {
		t.Fatalf("FinalityAggregations(7) = %v, want nil (out of range)", got)
	}
}

// The Dist seam (skewed-validators-spec.md): with ValidatorCounts, validator ids are contiguous
// by node — node i hosts [Σ counts[:i], Σ counts[:i+1]) — and the ranges partition 0..V-1. The
// uniform fallback (no counts) is pinned by TestDecoupledMembership above.
func TestFinalityVoteDutiesWithCounts(t *testing.T) {
	a := &Assignment{
		Params:           Params{N: 3, V: 6},
		ValidatorCounts:  []int{3, 1, 2},
		FinalitySubnetOf: []int{1, 0, 0, 1, 0, 1},
	}
	// Positions are the subnet cells' ranks: subnet0 = {1,2,4}, subnet1 = {0,3,5}.
	want := [][]AttestDuty{
		{{Subnet: 1, Val: 0, Position: 0}, {Subnet: 0, Val: 1, Position: 0}, {Subnet: 0, Val: 2, Position: 1}},
		{{Subnet: 1, Val: 3, Position: 1}},
		{{Subnet: 0, Val: 4, Position: 2}, {Subnet: 1, Val: 5, Position: 2}},
	}
	var all []int
	for node, w := range want {
		got := a.Node(node).FinalityVoteDuties(0) // base mode: the slot arg is ignored
		if !slices.Equal(got, w) {
			t.Fatalf("node%d FinalityVoteDuties = %v, want %v", node, got, w)
		}
		for _, d := range got {
			all = append(all, d.Val)
		}
	}
	// The per-node ranges partition the id space: every validator hosted exactly once.
	if !slices.Equal(all, []int{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("ranges do not partition 0..V-1: %v", all)
	}
}

// AggregateSubnets returns the subnets a node aggregates this slot — the committees whose
// aggregator set includes it. A node can aggregate several committees, or none.
func TestAggregateSubnets(t *testing.T) {
	a := &Assignment{
		Params:            Params{N: 4, V: 8, C: 2, Sc: 2, SubnetCount: 64, NumSlots: 1},
		SubnetSubscribers: [][]int{{0, 1, 2}, {0, 3}},
		Slots: []SlotPlan{{
			Slot:        0,
			Committees:  [][]AttesterRef{{}, {}},
			SubnetOf:    []int{0, 1},
			Aggregators: [][]int{{0, 2}, {0, 3}}, // node 0 aggregates both committees
		}},
	}
	if got := a.Node(0).AggregateSubnets(0); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("node0 AggregateSubnets = %v, want [0 1]", got)
	}
	if got := a.Node(2).AggregateSubnets(0); !slices.Equal(got, []int{0}) {
		t.Fatalf("node2 AggregateSubnets = %v, want [0]", got)
	}
	if got := a.Node(1).AggregateSubnets(0); got != nil {
		t.Fatalf("node1 AggregateSubnets = %v, want nil (aggregates nothing)", got)
	}
}
