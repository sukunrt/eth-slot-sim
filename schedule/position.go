package schedule

import "sync"

// The finality position space (partial-attestation-spec.md §3, locked): a finality vote's wire
// identity on the partial transport is its committee position — the rank of its validator among
// the ascending validators of its cell. The cell is the subnet's whole per-validator draw in
// base mode, the (subnet, round) intersection under segregation. Derived purely from
// FinalitySubnetOf (+ FinalityRoundOf), so Go and Python agree by construction with no
// schedule.json change; cell sizes equal the carried ValidatorsPerSubnet /
// ValidatorsPerRoundSubnet counts (pinned by position_test.go).
type finalityRanks struct {
	once  sync.Once
	cells [][][]int // [subnet][round] → ascending validator ids (a single round in base mode)
	pos   []int     // [val] → rank within its cell
}

// finalityRanks builds (once) and returns the cell and rank tables. Iterating validators in
// ascending id order makes each cell ascending and the rank its append index.
func (a *Assignment) finalityRanks() ([][][]int, []int) {
	r := &a.ranks
	r.once.Do(func() {
		rounds := 1
		if a.Segregated() {
			rounds = a.Params.AcSlotsPerFinalitySlot
		}
		subnets := 0
		for _, s := range a.FinalitySubnetOf {
			subnets = max(subnets, s+1)
		}
		r.cells = make([][][]int, subnets)
		for s := range r.cells {
			r.cells[s] = make([][]int, rounds)
		}
		r.pos = make([]int, len(a.FinalitySubnetOf))
		for val, s := range a.FinalitySubnetOf {
			round := 0
			if a.Segregated() {
				round = a.FinalityRoundOf[val]
			}
			r.pos[val] = len(r.cells[s][round])
			r.cells[s][round] = append(r.cells[s][round], val)
		}
	})
	return r.cells, r.pos
}

// round reduces a finality group key to its rank-table round: group % k under segregation
// (every AC slot is a round), the single row 0 in base mode (the key is the finality slot).
func (a *Assignment) round(group int) int {
	if a.Segregated() {
		return group % a.Params.AcSlotsPerFinalitySlot
	}
	return 0
}

// FinalityPosition returns val's finality-vote committee position — its rank within its
// (subnet[, round]) cell. -1 when val is out of range.
func (a *Assignment) FinalityPosition(val int) int {
	_, pos := a.finalityRanks()
	if val < 0 || val >= len(pos) {
		return -1
	}
	return pos[val]
}

// FinalityValAt inverts FinalityPosition: the validator at position of finality subnet's cell
// in the round selected by group key (the finality slot in base mode — ignored — or the AC
// slot under segregation). -1 when out of range.
func (a *Assignment) FinalityValAt(subnet, group, position int) int {
	cells, _ := a.finalityRanks()
	if subnet < 0 || subnet >= len(cells) {
		return -1
	}
	cell := cells[subnet][a.round(group)]
	if position < 0 || position >= len(cell) {
		return -1
	}
	return cell[position]
}

// FinalityCellSize returns the voting population of one (subnet, group key) cell — the partial
// transport's committee size (bitmap width) for finality votes. 0 when out of range.
func (a *Assignment) FinalityCellSize(subnet, group int) int {
	cells, _ := a.finalityRanks()
	if subnet < 0 || subnet >= len(cells) {
		return 0
	}
	return len(cells[subnet][a.round(group)])
}

// HostOf returns the node hosting validator val — the inverse of the hosting rule
// FinalityVoteDuties enumerates: the contiguous ValidatorCounts range when the Dist seam is
// present (node i hosts [Σ counts[:i], Σ counts[:i+1])), else uniform val % N. -1 when val is
// out of range.
func (a *Assignment) HostOf(val int) int {
	if val < 0 {
		return -1
	}
	if c := a.ValidatorCounts; len(c) > 0 {
		start := 0
		for node, n := range c {
			if val < start+n {
				return node
			}
			start += n
		}
		return -1
	}
	if a.Params.N <= 0 {
		return -1
	}
	return val % a.Params.N
}
