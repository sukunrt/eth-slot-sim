package committee

import (
	"path/filepath"
	"slices"
	"testing"
)

// The contract test: a committee.json that simctl/committee.py actually produced
// unmarshals into the Go structs and the accessors agree with the raw data. This
// guards the Python→Go schema handoff — a renamed/missing field would leave a struct
// field zero and the self-consistency checks would fail.
func TestLoadFixtureContract(t *testing.T) {
	a, err := Load(filepath.Join("testdata", "committee.json"))
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
				out = append(out, AttestDuty{Subnet: r.Subnet, Val: r.Val})
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
// carried in committee.json: a full-custody node holds every column (the relay backbone); an
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
