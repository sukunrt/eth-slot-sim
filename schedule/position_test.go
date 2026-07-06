package schedule

import (
	"slices"
	"testing"
)

// AttestDuties carries each seat's committee position through to the duty — the partial
// transport's wire identity (partial-attestation-spec.md §3): Position == AttesterRef.Position.
func TestAttestDutiesCarryPosition(t *testing.T) {
	a := &Assignment{
		Params: Params{N: 4, V: 8, C: 2, Sc: 2, NumSlots: 1},
		Slots: []SlotPlan{{
			Slot: 0,
			Committees: [][]AttesterRef{
				{{Node: 0, Val: 0, Subnet: 0, Position: 0}, {Node: 0, Val: 4, Subnet: 0, Position: 1}},
				{{Node: 1, Val: 1, Subnet: 1, Position: 0}, {Node: 0, Val: 2, Subnet: 1, Position: 1}},
			},
			SubnetOf: []int{0, 1},
		}},
	}
	if got, want := a.Node(0).AttestDuties[0], []AttestDuty{
		{Subnet: 0, Val: 0, Position: 0}, {Subnet: 0, Val: 4, Position: 1},
		{Subnet: 1, Val: 2, Position: 1},
	}; !slices.Equal(got, want) {
		t.Fatalf("node0 duties = %v, want %v", got, want)
	}
}

// The finality rank (spec §3, locked): position(val) = rank of val among the ascending
// validators of its subnet cell. Bijective per cell (val→position→val round-trips), cell sizes
// equal the carried ValidatorsPerSubnet, and the group key is ignored in base mode.
func TestFinalityRankBase(t *testing.T) {
	a := &Assignment{
		Params:              Params{N: 3, V: 6},
		FinalitySubnetOf:    []int{1, 0, 0, 1, 0, 1}, // subnet0: {1,2,4}, subnet1: {0,3,5}
		ValidatorsPerSubnet: []int{3, 3},
	}
	wantPos := []int{0, 0, 1, 1, 2, 2} // val → rank within its subnet cell
	for val, want := range wantPos {
		if got := a.FinalityPosition(val); got != want {
			t.Fatalf("FinalityPosition(%d) = %d, want %d", val, got, want)
		}
	}
	// position → val inverts, for any group key (base mode ignores it).
	for val := range 6 {
		s := a.FinalitySubnetOf[val]
		if got := a.FinalityValAt(s, 7, a.FinalityPosition(val)); got != val {
			t.Fatalf("FinalityValAt(%d, 7, pos(%d)) = %d, want %d", s, val, got, val)
		}
	}
	for s := range 2 {
		if got := a.FinalityCellSize(s, 0); got != a.ValidatorsPerSubnet[s] {
			t.Fatalf("FinalityCellSize(%d, 0) = %d, want %d", s, got, a.ValidatorsPerSubnet[s])
		}
	}
	// Out of range: no such position / subnet / validator.
	if got := a.FinalityValAt(0, 0, 3); got != -1 {
		t.Fatalf("FinalityValAt(0,0,3) = %d, want -1", got)
	}
	if got := a.FinalityValAt(9, 0, 0); got != -1 {
		t.Fatalf("FinalityValAt(9,0,0) = %d, want -1", got)
	}
	if got := a.FinalityPosition(99); got != -1 {
		t.Fatalf("FinalityPosition(99) = %d, want -1", got)
	}
}

// Under segregation the cell is (subnet, round): position(val) = rank of val among the
// ascending validators with its (FinalitySubnetOf, FinalityRoundOf) pair; the group key
// selects the round as group % k. Cell sizes equal the carried ValidatorsPerRoundSubnet
// counts; per-subnet sums equal ValidatorsPerSubnet; the grand total is V.
func TestFinalityRankSegregated(t *testing.T) {
	a := &Assignment{
		Params:                   Params{N: 3, V: 6, AcSlotsPerFinalitySlot: 2},
		FinalitySubnetOf:         []int{1, 0, 0, 1, 0, 1},
		FinalityRoundOf:          []int{0, 1, 0, 0, 1, 1},
		ValidatorsPerSubnet:      []int{3, 3},
		ValidatorsPerRoundSubnet: [][]int{{1, 2}, {2, 1}},
	}
	// Cells: (s0,r0)={2}, (s0,r1)={1,4}, (s1,r0)={0,3}, (s1,r1)={5}.
	wantPos := []int{0, 0, 0, 1, 1, 0}
	for val, want := range wantPos {
		if got := a.FinalityPosition(val); got != want {
			t.Fatalf("FinalityPosition(%d) = %d, want %d", val, got, want)
		}
	}
	// Round-trip through a group key with group % k == the validator's round.
	for val := range 6 {
		s, r := a.FinalitySubnetOf[val], a.FinalityRoundOf[val]
		group := r + 2*5
		if got := a.FinalityValAt(s, group, a.FinalityPosition(val)); got != val {
			t.Fatalf("FinalityValAt(%d, %d, pos(%d)) = %d, want %d", s, group, val, got, val)
		}
	}
	total := 0
	for s := range 2 {
		subnetSum := 0
		for r := range 2 {
			size := a.FinalityCellSize(s, r)
			if size != a.ValidatorsPerRoundSubnet[r][s] {
				t.Fatalf("FinalityCellSize(%d, %d) = %d, want %d",
					s, r, size, a.ValidatorsPerRoundSubnet[r][s])
			}
			subnetSum += size
			total += size
		}
		if subnetSum != a.ValidatorsPerSubnet[s] {
			t.Fatalf("subnet %d cell sum = %d, want %d", s, subnetSum, a.ValidatorsPerSubnet[s])
		}
	}
	if total != a.Params.V {
		t.Fatalf("cell grand total = %d, want V=%d", total, a.Params.V)
	}
}

// HostOf is the hosting rule the partial transport's identity resolver uses (spec §6):
// contiguous ValidatorCounts ranges when present, else uniform val % N.
func TestHostOf(t *testing.T) {
	a := &Assignment{Params: Params{N: 3, V: 6}}
	for val := range 6 {
		if got := a.HostOf(val); got != val%3 {
			t.Fatalf("uniform HostOf(%d) = %d, want %d", val, got, val%3)
		}
	}
	a.ValidatorCounts = []int{3, 1, 2} // node0: 0..2, node1: 3, node2: 4..5
	want := []int{0, 0, 0, 1, 2, 2}
	for val, w := range want {
		if got := a.HostOf(val); got != w {
			t.Fatalf("counts HostOf(%d) = %d, want %d", val, got, w)
		}
	}
	if got := a.HostOf(6); got != -1 {
		t.Fatalf("HostOf(6) = %d, want -1 (past Σ counts)", got)
	}
}
