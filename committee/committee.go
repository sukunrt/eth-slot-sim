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
	N              int    `json:"n"`
	V              int    `json:"v"`
	C              int    `json:"c"`
	Sc             int    `json:"sc"`
	SubnetCount    int    `json:"subnet_count"`
	SubnetsPerNode int    `json:"subnets_per_node"`
	SubscribeFloor int    `json:"subscribe_floor"`
	Seed           uint64 `json:"seed"`
	NumSlots       int    `json:"num_slots"`
}

// AttesterRef is one committee seat: which node publishes, which validator, on which
// subnet, at which position within the committee.
type AttesterRef struct {
	Node     int `json:"node"`
	Val      int `json:"val"`
	Subnet   int `json:"subnet"`
	Position int `json:"position"`
}

// SlotPlan is one slot's committee draw (who attests where).
type SlotPlan struct {
	Slot       int             `json:"slot"`
	Committees [][]AttesterRef `json:"committees"` // [committee] → its s_c attesters
	SubnetOf   []int           `json:"subnet_of"`  // [committee] → subnet id
}

// Assignment is the whole run's plan: the stable per-subnet subscribe set plus the
// per-slot committee draws.
type Assignment struct {
	Params Params `json:"params"`
	// SubnetSubscribers[s] = nodes subscribing active subnet s (stable for the run); the
	// receivers/relayers a publisher dials into.
	SubnetSubscribers [][]int    `json:"subnet_subscribers"`
	Slots             []SlotPlan `json:"slots"`
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

// Subscribers returns the nodes subscribing subnet (stable for the run) — the expected
// receiver set for any attestation on it. Nil if subnet is out of range.
func (a *Assignment) Subscribers(subnet int) []int {
	if subnet < 0 || subnet >= len(a.SubnetSubscribers) {
		return nil
	}
	return a.SubnetSubscribers[subnet]
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
