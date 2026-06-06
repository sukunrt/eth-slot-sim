// Package committee is the Go consumer of the committee assignment that
// simctl/committee.py generates. It unmarshals committee.json into typed values and
// exposes per-node accessors; it holds no generation logic. The assignment is produced
// once, in Python (the topology seam: validators→nodes→committees→subnets), and passed
// in. The exported structs are exactly the committee.json schema, so the unmarshal
// output and a hand-built value are the same type — tests build arbitrary committees as
// in-memory literals, independent of the Python generator.
package committee

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
)

// Params are the assignment knobs (V, C, s_c independent; only C·s_c ≤ V is enforced,
// in the generator). Fields mirror committee.json.
type Params struct {
	N                int    `json:"n"`
	V                int    `json:"v"`
	C                int    `json:"c"`
	Sc               int    `json:"sc"`
	SubnetCount      int    `json:"subnet_count"`
	BackbonePerNode  int    `json:"backbone_per_node"`
	AggsPerCommittee int    `json:"aggs_per_committee"`
	Seed             uint64 `json:"seed"`
	NumSlots         int    `json:"num_slots"`
}

// AttesterRef is one committee seat: which node publishes, which validator, on which
// subnet, at which position within the committee.
type AttesterRef struct {
	Node     int `json:"node"`
	Val      int `json:"val"`
	Subnet   int `json:"subnet"`
	Position int `json:"position"`
}

// SlotPlan is everything about one slot's attestation phase.
type SlotPlan struct {
	Slot        int             `json:"slot"`
	Committees  [][]AttesterRef `json:"committees"`  // [committee] → its s_c attesters
	SubnetOf    []int           `json:"subnet_of"`   // [committee] → subnet id
	Aggregators [][]AttesterRef `json:"aggregators"` // [committee] → its aggregator refs
	Subscribers [][]int         `json:"subscribers"` // [committee] → subscribing node ids
}

// Assignment is the whole run's committee plan.
type Assignment struct {
	Params   Params     `json:"params"`
	Backbone [][]int    `json:"backbone"` // [node] → its backbone subnets (stable)
	Slots    []SlotPlan `json:"slots"`
}

// Load reads a committee.json produced by simctl/committee.py.
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

// AttestDuty is one attestation a node owes: which validator, on which subnet, at which
// committee position. Content-free — the vote is decided later, by the coupling.
type AttestDuty struct {
	Subnet   int
	Val      int
	Position int
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
				duties = append(duties, AttestDuty{Subnet: r.Subnet, Val: r.Val, Position: r.Position})
			}
		}
	}
	return duties
}

// Backbone returns node's long-lived backbone subnets (subscribed whole-run).
func (v View) Backbone() []int { return v.a.Backbone[v.node] }

// AggregatorSubnets returns the subnets node aggregates this slot (subscribed for the
// slot only) — distinct, sorted.
func (v View) AggregatorSubnets(slot int) []int {
	seen := map[int]bool{}
	var out []int
	for _, aggs := range v.a.Slots[slot].Aggregators {
		for _, r := range aggs {
			if r.Node == v.node && !seen[r.Subnet] {
				seen[r.Subnet] = true
				out = append(out, r.Subnet)
			}
		}
	}
	slices.Sort(out)
	return out
}

// ExpectedSubscribers returns the node ids subscribing subnet this slot — the expected
// receiver set for any attestation published on it (backbone subscribers ∪ this slot's
// aggregators). Nil if no committee maps to subnet this slot.
func (a *Assignment) ExpectedSubscribers(slot, subnet int) []int {
	sp := a.Slots[slot]
	for ci, s := range sp.SubnetOf {
		if s == subnet {
			return sp.Subscribers[ci]
		}
	}
	return nil
}
