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
	if a.Params.N != 4 || a.Params.V != 8 || a.Params.C != 1 || a.Params.Sc != 4 {
		t.Fatalf("params = %+v, want n4 v8 c1 sc4", a.Params)
	}
	if a.Params.BackbonePerNode != 2 || a.Params.AggsPerCommittee != 2 || a.Params.NumSlots != 2 {
		t.Fatalf("params = %+v, want backbone2 aggs2 slots2", a.Params)
	}

	// Backbone: exactly BackbonePerNode distinct subnets per node, in range.
	if len(a.Backbone) != a.Params.N {
		t.Fatalf("backbone len %d, want %d", len(a.Backbone), a.Params.N)
	}
	for node, subs := range a.Backbone {
		if len(subs) != a.Params.BackbonePerNode {
			t.Fatalf("node %d backbone %v, want %d subnets", node, subs, a.Params.BackbonePerNode)
		}
		for _, s := range subs {
			if s < 0 || s >= a.Params.SubnetCount {
				t.Fatalf("node %d backbone subnet %d out of range", node, s)
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
		for ci, com := range sp.Committees {
			if len(com) != a.Params.Sc {
				t.Fatalf("slot %d committee %d size %d, want %d", sp.Slot, ci, len(com), a.Params.Sc)
			}
			for pos, r := range com {
				if r.Position != pos {
					t.Fatalf("slot %d committee %d pos %d != %d", sp.Slot, ci, r.Position, pos)
				}
				if r.Node != r.Val%a.Params.N {
					t.Fatalf("ref %+v: node != val%%N", r)
				}
				if r.Subnet != sp.SubnetOf[ci] {
					t.Fatalf("ref %+v: subnet != subnet_of[%d]=%d", r, ci, sp.SubnetOf[ci])
				}
			}
		}
	}

	// Accessors agree with the raw structure, for every node and slot.
	for node := range a.Params.N {
		v := a.Node(node)
		for slot := range a.Params.NumSlots {
			if got, want := v.AttestDuties(slot), rawDuties(a, node, slot); !slices.Equal(got, want) {
				t.Fatalf("node %d slot %d duties = %v, want %v", node, slot, got, want)
			}
		}
		if got, want := v.Backbone(), a.Backbone[node]; !slices.Equal(got, want) {
			t.Fatalf("node %d Backbone() = %v, want %v", node, got, want)
		}
	}
	// ExpectedSubscribers maps subnet→subscribers via subnet_of.
	for _, sp := range a.Slots {
		for ci, subnet := range sp.SubnetOf {
			if got := a.ExpectedSubscribers(sp.Slot, subnet); !slices.Equal(got, sp.Subscribers[ci]) {
				t.Fatalf("slot %d subnet %d subscribers = %v, want %v", sp.Slot, subnet, got, sp.Subscribers[ci])
			}
		}
	}
}

// rawDuties recomputes a node's duties straight from the parsed committees, independent
// of the accessor under test.
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
		Params:   Params{N: 3, V: 6, C: 2, Sc: 2, SubnetCount: 64, BackbonePerNode: 1, NumSlots: 1},
		Backbone: [][]int{{5}, {5}, {9}},
		Slots: []SlotPlan{{
			Slot: 0,
			Committees: [][]AttesterRef{
				{{Node: 0, Val: 0, Subnet: 0, Position: 0}, {Node: 0, Val: 3, Subnet: 0, Position: 1}},
				{{Node: 1, Val: 1, Subnet: 1, Position: 0}, {Node: 2, Val: 2, Subnet: 1, Position: 1}},
			},
			SubnetOf:    []int{0, 1},
			Aggregators: [][]AttesterRef{{{Node: 0, Val: 0, Subnet: 0, Position: 0}}, {{Node: 2, Val: 2, Subnet: 1, Position: 1}}},
			Subscribers: [][]int{{0}, {2}},
		}},
	}

	// k>1: node 0 holds two attesters on subnet 0 ⇒ two duties.
	if got := a.Node(0).AttestDuties(0); len(got) != 2 {
		t.Fatalf("node0 duties = %v, want 2 (k>1 on one subnet)", got)
	}
	if got := a.Node(2).AttestDuties(0); len(got) != 1 || got[0].Subnet != 1 || got[0].Val != 2 {
		t.Fatalf("node2 duties = %v, want one on subnet 1 val 2", got)
	}
	if got := a.Node(2).AggregatorSubnets(0); !slices.Equal(got, []int{1}) {
		t.Fatalf("node2 aggregator subnets = %v, want [1]", got)
	}
	if got := a.Node(0).AggregatorSubnets(0); !slices.Equal(got, []int{0}) {
		t.Fatalf("node0 aggregator subnets = %v, want [0]", got)
	}
	if got := a.ExpectedSubscribers(0, 1); !slices.Equal(got, []int{2}) {
		t.Fatalf("subnet 1 subscribers = %v, want [2]", got)
	}
}
