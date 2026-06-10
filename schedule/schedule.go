// Package schedule is the Go consumer of the per-run schedule that
// simctl/schedule.py generates: the proposer schedule, the attestation committees +
// subnet subscribers, and the data-column custody. It unmarshals schedule.json into typed
// values and exposes per-node accessors; it holds no generation logic. The schedule is
// produced once, in Python (the topology seam: validators→nodes→committees→subnets), and
// passed in. The exported structs are exactly the schedule.json schema, so the unmarshal
// output and a hand-built value are the same type — tests build arbitrary schedules as
// in-memory literals, independent of the Python generator.
package schedule

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
)

// Params are the assignment knobs (V, C, s_c independent; only C·s_c ≤ V is enforced,
// in the generator). Fields mirror schedule.json.
type Params struct {
	N              int `json:"n"`
	V              int `json:"v"`
	C              int `json:"c"`
	Sc             int `json:"sc"`
	SubnetCount    int `json:"subnet_count"`
	SubnetsPerNode int `json:"subnets_per_node"`
	SubscribeFloor int `json:"subscribe_floor"`
	// TargetAggregators is the aggregators drawn per committee (clamped to the subnet's
	// subscriber count); each publishes one distinct aggregate.
	TargetAggregators int    `json:"target_aggregators"`
	Seed              uint64 `json:"seed"`
	NumSlots          int    `json:"num_slots"`
	// Decoupled-consensus knobs (0 ⇒ phase off). AcVoteSize is the per-slot VRF-selected validator
	// count voting on the availability chain (one global topic); AcSlotsPerFinalitySlot is k (a
	// finality slot spans k AC slots); FsSubnets is the node-partitioned finality subnet count;
	// FsAggregators is the aggregators drawn per finality subnet per finality slot. See
	// decoupled-consensus-spec.md.
	AcVoteSize             int `json:"ac_vote_size,omitempty"`
	AcSlotsPerFinalitySlot int `json:"ac_slots_per_finality_slot,omitempty"`
	FsSubnets              int `json:"fs_subnets,omitempty"`
	FsAggregators          int `json:"fs_aggregators,omitempty"`
}

// AttesterRef is one committee seat: which node publishes, which validator, on which
// subnet, at which position within the schedule.
type AttesterRef struct {
	Node     int `json:"node"`
	Val      int `json:"val"`
	Subnet   int `json:"subnet"`
	Position int `json:"position"`
}

// SlotPlan is one slot's committee draw (who attests where) plus its block proposer
// and aggregators.
type SlotPlan struct {
	Slot        int             `json:"slot"`
	Committees  [][]AttesterRef `json:"committees"`  // [committee] → its s_c attesters
	SubnetOf    []int           `json:"subnet_of"`   // [committee] → subnet id
	Proposer    int             `json:"proposer"`    // node that publishes this slot's block (a supernode)
	Aggregators [][]int         `json:"aggregators"` // [committee] → aggregator node ids
	// SyncAggregators[i] = aggregator node ids on sync subnet i (drawn from SyncSubscribers[i]);
	// each publishes one contribution on the global topic. Empty/nil when the sync phase is off.
	SyncAggregators [][]int `json:"sync_aggregators,omitempty"`
	// ACVoters = the VRF-selected validators voting on the availability chain this slot — a flat set
	// (no committees, no subnets; one global topic), so a node hosting m of them emits m votes.
	// Empty/nil when the decoupled-consensus phase is off.
	ACVoters []AttesterRef `json:"ac_voters,omitempty"`
	// FinalityAggregators[i] = the aggregator refs for finality subnet i this finality slot —
	// fs_aggregators VALIDATORS sampled from the entire set (unrelated to subnet membership or
	// hosting); the host node carries the duty. Present only on a finality-boundary slot
	// (slot % k == 0), nil otherwise.
	FinalityAggregators [][]AttesterRef `json:"finality_aggregators,omitempty"`
}

// Assignment is the whole run's plan: the stable per-subnet subscribe set plus the
// per-slot committee draws.
type Assignment struct {
	Params Params `json:"params"`
	// SubnetSubscribers[s] = nodes subscribing active subnet s (stable for the run); the
	// receivers/relayers a publisher dials into.
	SubnetSubscribers [][]int `json:"subnet_subscribers"`
	// Data-columns custody (the dissemination + gate phase). Empty/0 when the phase is off.
	// NumColumns is the column-subnet count; ColumnSubscribers[i] = nodes custodying column
	// i (the full-custody backbone ∪ the ordinary nodes that drew i); FullCustody = nodes
	// holding every column. Generated in Python, carried here verbatim (Go never re-derives
	// the full-custody set — see data-columns-spec.md §3).
	NumColumns        int     `json:"num_columns,omitempty"`
	ColumnSubscribers [][]int `json:"column_subscribers,omitempty"`
	FullCustody       []int   `json:"full_custody,omitempty"`
	// Sync-committee membership (the dissemination + contribution phase). Empty when off.
	// SyncSubscribers[i] = the member nodes on sync subnet i (stable for the run; a member is on
	// exactly one subnet) — both the subnet's mesh and the per-subnet coverage set. Generated in
	// Python, carried here verbatim. The transpose of the per-node membership attribute.
	SyncSubscribers [][]int `json:"sync_subscribers,omitempty"`
	// Decoupled-consensus membership (empty when the phase is off). FinalitySubscribers[i] = the
	// member nodes on finality subnet i — a partition of ALL N nodes (every node on exactly one
	// subnet), the subnet's STABLE mesh/receiver core. FinalitySubnetOf[val] = the subnet
	// validator val votes on — finality subnets partition the VALIDATOR set (an independent
	// uniform draw per validator, decoupled from where keys live); a node publishes on its
	// duties' subnets via fan-out when it isn't a member. ValidatorsPerSubnet[i] = that draw's
	// per-subnet counts (~V/S ± binomial noise), carried for the scaled aggregate size and the
	// coverage count. Generated in Python.
	FinalitySubscribers [][]int    `json:"finality_subscribers,omitempty"`
	FinalitySubnetOf    []int      `json:"finality_subnet_of,omitempty"`
	ValidatorsPerSubnet []int      `json:"validators_per_subnet,omitempty"`
	// ValidatorCounts[node] = hosted-validator count (the Dist seam; see
	// skewed-validators-spec.md). When present, validator ids are contiguous by node: node i
	// hosts [Σ counts[:i], Σ counts[:i+1]) and Params.V = Σ counts. Empty ⇒ uniform v % N.
	// Generated in Python, carried here verbatim.
	ValidatorCounts []int      `json:"validator_counts,omitempty"`
	Slots           []SlotPlan `json:"slots"`
}

// Load reads a schedule.json produced by simctl/schedule.py.
func Load(path string) (*Assignment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var a Assignment
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &a, nil
}

// Subscribers returns the nodes subscribing subnet (stable for the run) — the expected
// receiver set for any attestation on it. Nil if subnet is out of range.
func (a *Assignment) Subscribers(subnet int) []int {
	if subnet < 0 || subnet >= len(a.SubnetSubscribers) {
		return nil
	}
	return a.SubnetSubscribers[subnet]
}

// ColumnSubscribersOf returns the nodes custodying column col (its stable subscriber/relay
// set) — the expected receiver set for that column's sidecar. Nil if col is out of range.
func (a *Assignment) ColumnSubscribersOf(col int) []int {
	if col < 0 || col >= len(a.ColumnSubscribers) {
		return nil
	}
	return a.ColumnSubscribers[col]
}

// SyncSubscribersOf returns the member nodes on sync subnet i (its stable subscriber set) — the
// expected receiver set for any sync message on it. Nil if i is out of range.
func (a *Assignment) SyncSubscribersOf(subnet int) []int {
	if subnet < 0 || subnet >= len(a.SyncSubscribers) {
		return nil
	}
	return a.SyncSubscribers[subnet]
}

// FinalitySubscribersOf returns the member nodes on finality subnet i (its stable membership) — the
// expected receiver set for any finality vote on it. Nil if i is out of range.
func (a *Assignment) FinalitySubscribersOf(subnet int) []int {
	if subnet < 0 || subnet >= len(a.FinalitySubscribers) {
		return nil
	}
	return a.FinalitySubscribers[subnet]
}

// ProposerSchedule returns the per-slot block proposer (a supernode), one entry per slot in
// slot order — the schedule the Validator obeys instead of the cyclic slot%N rule. Both
// backends read it from the same schedule.json, so they propose identically.
func (a *Assignment) ProposerSchedule() []int {
	out := make([]int, len(a.Slots))
	for i, sp := range a.Slots {
		out[i] = sp.Proposer
	}
	return out
}

// AttestDuty is one attestation a node owes: which validator, on which subnet.
// Content-free — the vote is decided later, by the coupling.
type AttestDuty struct {
	Subnet int
	Val    int
}

// View narrows the assignment to one node's slice (what a single host runs).
type View struct {
	a    *Assignment
	node int
}

// Node returns the view of the assignment for one node.
func (a *Assignment) Node(node int) View { return View{a: a, node: node} }

// AttestDuties returns the attestations node owes this slot — one per hosted validator
// seated in one of the slot's committees (k>1 possible on the same subnet).
func (v View) AttestDuties(slot int) []AttestDuty {
	var duties []AttestDuty
	for _, com := range v.a.Slots[slot].Committees {
		for _, r := range com {
			if r.Node == v.node {
				duties = append(duties, AttestDuty{Subnet: r.Subnet, Val: r.Val})
			}
		}
	}
	return duties
}

// SubscribedSubnets returns the subnets this node subscribes (stable; it meshes on these
// for the whole run).
func (v View) SubscribedSubnets() []int {
	var out []int
	for subnet, members := range v.a.SubnetSubscribers {
		if slices.Contains(members, v.node) {
			out = append(out, subnet)
		}
	}
	return out
}

// IsFullCustody reports whether this node holds every column — a data-column supernode that
// forms the relay backbone (drawn from the 1 Gbit supernodes; see data-columns-spec.md §3).
func (v View) IsFullCustody() bool {
	return slices.Contains(v.a.FullCustody, v.node)
}

// CustodyColumns returns the column subnets this node custodies (subscribes + relays), stable
// for the run. A full-custody node holds all NumColumns; an ordinary node holds the
// seeded-random subset it drew (its membership in ColumnSubscribers).
func (v View) CustodyColumns() []int {
	if v.IsFullCustody() {
		out := make([]int, v.a.NumColumns)
		for i := range out {
			out[i] = i
		}
		return out
	}
	var out []int
	for col, members := range v.a.ColumnSubscribers {
		if slices.Contains(members, v.node) {
			out = append(out, col)
		}
	}
	return out
}

// AggregateSubnets returns the subnets this node aggregates this slot — the committees whose
// aggregator set includes it. It publishes one aggregate on each (on the global aggregate
// topic). Empty if the node isn't an aggregator this slot.
func (v View) AggregateSubnets(slot int) []int {
	if slot < 0 || slot >= len(v.a.Slots) {
		return nil
	}
	sp := v.a.Slots[slot]
	var out []int
	for ci, aggs := range sp.Aggregators {
		if slices.Contains(aggs, v.node) {
			out = append(out, sp.SubnetOf[ci])
		}
	}
	return out
}

// SyncSubnet returns this node's sync-committee subnet and whether it is a member. Membership is
// node-based and stable: a member is on exactly one subnet (round-robin assignment) and emits one
// message there; a non-member returns (-1, false) and emits none.
func (v View) SyncSubnet() (subnet int, member bool) {
	for s, members := range v.a.SyncSubscribers {
		if slices.Contains(members, v.node) {
			return s, true
		}
	}
	return -1, false
}

// SyncAggregateSubnets returns the sync subnets this node aggregates this slot — the subnets whose
// aggregator set includes it. It publishes one contribution on each (on the global contribution
// topic). Empty if the node isn't a sync aggregator this slot. SyncAggregators is indexed by
// subnet directly (unlike attestation Aggregators, which need the SubnetOf map).
func (v View) SyncAggregateSubnets(slot int) []int {
	if slot < 0 || slot >= len(v.a.Slots) {
		return nil
	}
	var out []int
	for subnet, aggs := range v.a.Slots[slot].SyncAggregators {
		if slices.Contains(aggs, v.node) {
			out = append(out, subnet)
		}
	}
	return out
}

// ACVoteDuties returns the availability-chain votes this node owes this slot — one per hosted
// validator in the slot's VRF draw (k>1 possible). The AC vote is on one global topic, so Subnet is
// unused (left zero); only Val is meaningful. Empty when the node holds none of the slot's voters.
func (v View) ACVoteDuties(slot int) []AttestDuty {
	if slot < 0 || slot >= len(v.a.Slots) {
		return nil
	}
	var duties []AttestDuty
	for _, r := range v.a.Slots[slot].ACVoters {
		if r.Node == v.node {
			duties = append(duties, AttestDuty{Val: r.Val})
		}
	}
	return duties
}

// FinalitySubnet returns this node's finality subnet and whether it is a member. Membership is
// node-based and stable: finality subnets partition ALL N nodes, so under the decoupled phase every
// node is a member of exactly one subnet; (-1, false) when the phase is off (no membership).
func (v View) FinalitySubnet() (subnet int, member bool) {
	for s, members := range v.a.FinalitySubscribers {
		if slices.Contains(members, v.node) {
			return s, true
		}
	}
	return -1, false
}

// FinalityVoteDuties returns the finality votes this node owes every finality slot — one
// (val, subnet) pair per hosted validator, the subnet from FinalitySubnetOf (so one node's keys
// can land on many subnets; it fans out where it isn't a member). With ValidatorCounts (the Dist
// seam) hosted ids are contiguous by node: [Σ counts[:node], Σ counts[:node+1]); otherwise
// uniform V→N: the validators v with v % N == node.
func (v View) FinalityVoteDuties() []AttestDuty {
	duty := func(val int) AttestDuty {
		return AttestDuty{Subnet: v.a.FinalitySubnetOf[val], Val: val}
	}
	if c := v.a.ValidatorCounts; len(c) > 0 {
		start := 0
		for _, n := range c[:v.node] {
			start += n
		}
		out := make([]AttestDuty, c[v.node])
		for i := range out {
			out[i] = duty(start + i)
		}
		return out
	}
	var out []AttestDuty
	for val := v.node; val < v.a.Params.V; val += v.a.Params.N {
		out = append(out, duty(val))
	}
	return out
}

// FinalityAggregations returns the finality subnets this node aggregates for finality slot n —
// the subnets with an aggregator ref hosted here, deduped (two selected validators on one node
// yield one aggregate). Aggregator validators are sampled from the ENTIRE set, so the node is
// generally NOT a member of these subnets: it pre-joins their meshes at AC slot n·k−1. Drawn per
// finality slot and stored on the boundary AC slot (n·k), so this reads
// Slots[n·k].FinalityAggregators. Nil if the node aggregates nothing or n is out of range.
func (v View) FinalityAggregations(n int) []int {
	k := v.a.Params.AcSlotsPerFinalitySlot
	if k <= 0 {
		return nil
	}
	boundary := n * k
	if boundary < 0 || boundary >= len(v.a.Slots) {
		return nil
	}
	var out []int
	for s, aggs := range v.a.Slots[boundary].FinalityAggregators {
		for _, r := range aggs {
			if r.Node == v.node {
				out = append(out, s) // subnets ascend, so out is sorted
				break
			}
		}
	}
	return out
}
