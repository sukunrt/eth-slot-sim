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
	FinalitySubscribers [][]int `json:"finality_subscribers,omitempty"`
	FinalitySubnetOf    []int   `json:"finality_subnet_of,omitempty"`
	ValidatorsPerSubnet []int   `json:"validators_per_subnet,omitempty"`
	// Validator segregation (empty when off — its presence IS how this side detects the
	// variant; see Segregated). FinalityRoundOf[val] = the round (0..k-1) validator val votes
	// in: AC slot s is round s % k, and only round-(s%k) validators emit finality votes in it
	// (an independent uniform draw, stable for the run). ValidatorsPerRoundSubnet[r][i] = the
	// (round, subnet) cell counts of the two independent draws (Σ_r per subnet =
	// ValidatorsPerSubnet[i], ΣΣ = V) — they size the cell-scaled round aggregate. Generated in
	// Python. See validator-segregation-spec.md.
	FinalityRoundOf          []int   `json:"finality_round_of,omitempty"`
	ValidatorsPerRoundSubnet [][]int `json:"validators_per_round_subnet,omitempty"`
	// ValidatorCounts[node] = hosted-validator count (the Dist seam; see
	// skewed-validators-spec.md). When present, validator ids are contiguous by node: node i
	// hosts [Σ counts[:i], Σ counts[:i+1]) and Params.V = Σ counts. Empty ⇒ uniform v % N.
	// Generated in Python, carried here verbatim.
	ValidatorCounts []int      `json:"validator_counts,omitempty"`
	Slots           []SlotPlan `json:"slots"`

	// ranks caches the finality position tables (built once on first use, so the finality
	// draws must not be mutated afterward) — the partial transport's position space (spec §3).
	ranks finalityRanks
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

// Segregated reports whether this plan was generated with validator segregation
// (per-AC-slot finality rounds): the presence of finality_round_of in schedule.json is the
// variant detection — the schedule itself is the one source of truth, so the slot-keyed
// accessors (FinalityVoteDuties, FinalityAggregations) branch on it, not on a driver flag.
func (a *Assignment) Segregated() bool {
	return len(a.FinalityRoundOf) > 0
}

// FinalitySubscribersOf returns the member nodes on finality subnet i (its stable membership) — the
// expected receiver set for any finality vote on it. Nil if i is out of range.
func (a *Assignment) FinalitySubscribersOf(subnet int) []int {
	if subnet < 0 || subnet >= len(a.FinalitySubscribers) {
		return nil
	}
	return a.FinalitySubscribers[subnet]
}

// AttestDuty is one attestation a node owes: which validator, on which subnet, at which
// committee position — the partial transport's wire identity (the committee seat for standard
// attestations, the finality cell rank for finality votes; zero for AC votes, which stay
// classic). Content-free — the vote is decided later, by the coupling.
type AttestDuty struct {
	Subnet   int
	Val      int
	Position int
}

// ACVoteDuty is one availability-chain vote a node owes: which validator. The AC vote rides one
// global topic (no subnet) and is content-free until the coupling decides it — so unlike AttestDuty
// it carries only Val.
type ACVoteDuty struct {
	Val int
}

// View is one node's slice of the plan, materialized once by Assignment.Node: the node's own
// duties per slot plus the shared membership tables it needs (dial targets, aggregate sizing).
// Plain data — a test can build one as a literal. It is the only handle a NodeRunner holds on
// the plan, so a runner can never ask about another node's duties. The zero value (NumSlots 0)
// is the no-plan sentinel: block-only runs propose cyclically and owe nothing. The shared
// tables are slice headers into the Assignment, not copies.
type View struct {
	NumSlots   int
	NumColumns int // column-subnet count (0 ⇒ the column phase is off)

	// Per-AC-slot duties, indexed by slot (each len == NumSlots).
	Proposes             []bool         // this node publishes slot s's block (the Python draw)
	AttestDuties         [][]AttestDuty // one per hosted validator seated in slot s's committees
	ACVoteDuties         [][]ACVoteDuty // one per hosted validator in slot s's VRF draw
	AggregateSubnets     [][]int        // subnets this node aggregates in slot s (one aggregate each)
	SyncAggregateSubnets [][]int        // sync subnets this node aggregates in slot s (one contribution each)

	// Finality rounds, indexed by the round key n: the finality slot in base mode
	// (ceil(NumSlots/k) rounds, duties identical every round), the AC slot under segregation
	// (NumSlots rounds, round n%k's validators only). Nil when the plan has no finality chain.
	FinalityVoteDuties   [][]AttestDuty // one (val, subnet) per hosted validator voting in round n
	FinalityAggregations [][]int        // finality subnets this node aggregates in round n (deduped)

	// Stable per-node facts.
	SubscribedSubnets []int // attestation subnets this node meshes for the whole run
	CustodyColumns    []int // column subnets this node custodies (all NumColumns when FullCustody)
	FullCustody       bool  // data-column supernode: holds every column (the relay backbone)
	SyncSubnet        int   // this node's sync-committee subnet; -1 unless SyncMember
	SyncMember        bool
	FinalitySubnet    int // this node's stable finality subnet; -1 unless FinalityMember
	FinalityMember    bool

	// Shared tables (the whole plan's: dial targets and aggregate sizing).
	Subscribers              [][]int // Subscribers[subnet] = the nodes subscribing an attestation subnet
	FinalitySubscribers      [][]int // FinalitySubscribers[subnet] = the nodes on a finality subnet
	ValidatorsPerSubnet      []int   // per-finality-subnet draw count (sizes base-mode aggregates)
	ValidatorsPerRoundSubnet [][]int // (round, subnet) cell counts (sizes round aggregates)
}

// Node materializes one node's view of the assignment.
func (a *Assignment) Node(node int) View {
	v := View{
		NumSlots:                 len(a.Slots),
		NumColumns:               a.NumColumns,
		FullCustody:              slices.Contains(a.FullCustody, node),
		SyncSubnet:               -1,
		FinalitySubnet:           -1,
		Subscribers:              a.SubnetSubscribers,
		FinalitySubscribers:      a.FinalitySubscribers,
		ValidatorsPerSubnet:      a.ValidatorsPerSubnet,
		ValidatorsPerRoundSubnet: a.ValidatorsPerRoundSubnet,
	}

	// Stable memberships. Sync subnets cover members only; finality subnets partition all N
	// nodes (every node is a member when the decoupled phase is on).
	for subnet, members := range a.SubnetSubscribers {
		if slices.Contains(members, node) {
			v.SubscribedSubnets = append(v.SubscribedSubnets, subnet)
		}
	}
	if v.FullCustody { // a full-custody node holds every column
		v.CustodyColumns = make([]int, a.NumColumns)
		for i := range v.CustodyColumns {
			v.CustodyColumns[i] = i
		}
	} else { // an ordinary node holds its seeded-random membership in ColumnSubscribers
		for col, members := range a.ColumnSubscribers {
			if slices.Contains(members, node) {
				v.CustodyColumns = append(v.CustodyColumns, col)
			}
		}
	}
	for s, members := range a.SyncSubscribers {
		if slices.Contains(members, node) {
			v.SyncSubnet, v.SyncMember = s, true
			break
		}
	}
	for s, members := range a.FinalitySubscribers {
		if slices.Contains(members, node) {
			v.FinalitySubnet, v.FinalityMember = s, true
			break
		}
	}

	// Per-slot duties.
	v.Proposes = make([]bool, len(a.Slots))
	v.AttestDuties = make([][]AttestDuty, len(a.Slots))
	v.ACVoteDuties = make([][]ACVoteDuty, len(a.Slots))
	v.AggregateSubnets = make([][]int, len(a.Slots))
	v.SyncAggregateSubnets = make([][]int, len(a.Slots))
	for s, sp := range a.Slots {
		v.Proposes[s] = sp.Proposer == node
		for _, com := range sp.Committees {
			for _, r := range com {
				if r.Node == node { // k>1 possible on the same subnet
					v.AttestDuties[s] = append(v.AttestDuties[s],
						AttestDuty{Subnet: r.Subnet, Val: r.Val, Position: r.Position})
				}
			}
		}
		for _, r := range sp.ACVoters {
			if r.Node == node {
				v.ACVoteDuties[s] = append(v.ACVoteDuties[s], ACVoteDuty{Val: r.Val})
			}
		}
		// Aggregators is per committee (SubnetOf maps committee → subnet); SyncAggregators is
		// indexed by subnet directly.
		for ci, aggs := range sp.Aggregators {
			if slices.Contains(aggs, node) {
				v.AggregateSubnets[s] = append(v.AggregateSubnets[s], sp.SubnetOf[ci])
			}
		}
		for subnet, aggs := range sp.SyncAggregators {
			if slices.Contains(aggs, node) {
				v.SyncAggregateSubnets[s] = append(v.SyncAggregateSubnets[s], subnet)
			}
		}
	}

	// Finality rounds (present when the plan carries the decoupled chain).
	if k := a.Params.AcSlotsPerFinalitySlot; k > 0 && len(a.FinalitySubnetOf) > 0 {
		rounds := (len(a.Slots) + k - 1) / k // finality slots (base mode)
		if a.Segregated() {
			rounds = len(a.Slots) // every AC slot is a round
		}
		v.FinalityVoteDuties = make([][]AttestDuty, rounds)
		v.FinalityAggregations = make([][]int, rounds)
		for n := range rounds {
			v.FinalityVoteDuties[n] = a.finalityVoteDuties(node, n)
			v.FinalityAggregations[n] = a.finalityAggregations(node, n)
		}
	}
	return v
}

// finalityVoteDuties returns the finality votes node owes in round `slot` — one (val, subnet)
// pair per hosted validator, the subnet from FinalitySubnetOf (one node's keys can land on many
// subnets; it fans out where it isn't a member). Base mode ignores slot: every hosted validator
// votes once per finality slot. Under segregation the duties are the hosted validators whose
// FinalityRoundOf round is slot % k. With ValidatorCounts (the Dist seam) hosted ids are
// contiguous by node: [Σ counts[:node], Σ counts[:node+1]); otherwise uniform V→N: the
// validators v with v % N == node.
func (a *Assignment) finalityVoteDuties(node, slot int) []AttestDuty {
	include := func(int) bool { return true }
	if a.Segregated() {
		round := slot % a.Params.AcSlotsPerFinalitySlot
		include = func(val int) bool { return a.FinalityRoundOf[val] == round }
	}
	var out []AttestDuty
	duty := func(val int) {
		if include(val) {
			out = append(out, AttestDuty{Subnet: a.FinalitySubnetOf[val], Val: val,
				Position: a.FinalityPosition(val)})
		}
	}
	if c := a.ValidatorCounts; len(c) > 0 {
		start := 0
		for _, n := range c[:node] {
			start += n
		}
		for val := start; val < start+c[node]; val++ {
			duty(val)
		}
		return out
	}
	for val := node; val < a.Params.V; val += a.Params.N {
		duty(val)
	}
	return out
}

// finalityAggregations returns the finality subnets node aggregates in round `slot` — the
// subnets with an aggregator ref hosted here, deduped (two selected validators on one node
// yield one aggregate). Aggregator validators are sampled from the ENTIRE set, so the node is
// generally NOT a member of these subnets: it pre-joins their meshes one AC slot ahead. Base
// mode: the round's draw is stored on the boundary AC slot (slot·k). Under segregation every
// AC slot is a round with its own draw. Nil if the node aggregates nothing or the round is out
// of range.
func (a *Assignment) finalityAggregations(node, slot int) []int {
	k := a.Params.AcSlotsPerFinalitySlot
	if k <= 0 {
		return nil
	}
	idx := slot
	if !a.Segregated() {
		idx = slot * k
	}
	if idx < 0 || idx >= len(a.Slots) {
		return nil
	}
	var out []int
	for s, aggs := range a.Slots[idx].FinalityAggregators {
		for _, r := range aggs {
			if r.Node == node {
				out = append(out, s) // subnets ascend, so out is sorted
				break
			}
		}
	}
	return out
}
