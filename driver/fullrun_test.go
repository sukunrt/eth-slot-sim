package driver_test

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
)

// M6: a sized full run. Every published attestation reaches exactly its subnet's
// subscribers (minus the publisher) and nobody else — per-subnet coverage + no-leakage,
// the strongest new invariant. Race-skipped (the flood overflows TSan's epoch, as the
// block N-node run already is); coverage stays under race via the small M2/M4/M5 tests.
func TestFullRunCoverageNoLeakage(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		a := genAssignment(16, 32, 1, 8, 2, 2, 42) // N=16, V=32, C=1, s_c=8
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertCoverageNoLeakage(t, a, rec, 0)
	})
}

// M6 multi-subnet: two committees on two subnets — a message on one subnet must never
// leak to the other's subscribers.
func TestFullRunMultiSubnetNoLeakage(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		a := genAssignment(16, 32, 2, 4, 2, 2, 7) // C=2, s_c=4
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertCoverageNoLeakage(t, a, rec, 0)
	})
}

// M6 k>1: a node holding two of the slot's attesters on one subnet emits two distinct
// attestations, each fully disseminated.
func TestFullRunKGreaterThanOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// committee 0 on subnet 0: vals 0 and 4 both live on node 0 (val % 4) ⇒ k=2.
		a := &committee.Assignment{
			Params: committee.Params{N: 4, V: 8, C: 1, Sc: 4, SubnetCount: 64, BackbonePerNode: 1, NumSlots: 1},
			Backbone: [][]int{{0}, {0}, {0}, {0}},
			Slots: []committee.SlotPlan{{
				Slot: 0,
				Committees: [][]committee.AttesterRef{{
					{Node: 0, Val: 0, Subnet: 0, Position: 0},
					{Node: 0, Val: 4, Subnet: 0, Position: 1},
					{Node: 1, Val: 1, Subnet: 0, Position: 2},
					{Node: 2, Val: 2, Subnet: 0, Position: 3},
				}},
				SubnetOf:    []int{0},
				Aggregators: [][]committee.AttesterRef{{}},
				Subscribers: [][]int{{0, 1, 2, 3}},
			}},
		}
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertCoverageNoLeakage(t, a, rec, 0)

		// Node 0 published two distinct attestations (vals 0 and 4).
		from0 := map[int]bool{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindAttestation && ar.ID.Origin == 0 {
				from0[ar.ID.Attester] = true
			}
		}
		if len(from0) != 2 || !from0[0] || !from0[4] {
			t.Fatalf("node 0 attesters = %v, want {0,4} (k=2)", keys(from0))
		}
	})
}

// M6 determinism: the same seed in two fresh bubbles (each clock from 0) yields
// byte-identical sorted arrivals (same identities and the same relative delays). Two
// runs in ONE bubble would differ — the second starts at a different absolute time, so
// gossipsub's heartbeat lands at a different phase.
func TestFullRunDeterministic(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	capture := func(t *testing.T) []metrics.Arrival {
		var got []metrics.Arrival
		synctest.Test(t, func(t *testing.T) {
			a := genAssignment(16, 24, 1, 6, 2, 2, 99)
			rec := metrics.NewRecorder()
			s := buildScenario(t, a, 4*time.Second, nil, rec)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.run(t, ctx, 1)
			got = rec.Arrivals()
		})
		return got
	}
	var a1, a2 []metrics.Arrival
	t.Run("bubble1", func(t *testing.T) { a1 = capture(t) })
	t.Run("bubble2", func(t *testing.T) { a2 = capture(t) })
	if !equalArrivals(a1, a2) {
		t.Fatalf("non-deterministic: %d vs %d arrivals (or differing identities/delays)", len(a1), len(a2))
	}
}

// equalArrivals compares the arrival *identity* sets (who received which attestation,
// with which vote) — the reproducible invariant. Absolute delays are not compared:
// gossipsub's mesh peer selection uses non-seeded randomness, so relay paths (hence
// per-hop delays) vary run to run even at a fixed seed; the set of arrivals does not.
func equalArrivals(a, b []metrics.Arrival) bool {
	key := func(x metrics.Arrival) [6]int {
		v := 0
		if x.VotedBlock {
			v = 1
		}
		return [6]int{x.Node, x.ID.Slot, x.ID.Subnet, x.ID.Attester, x.ID.Origin, v}
	}
	ka := make([][6]int, len(a))
	kb := make([][6]int, len(b))
	for i, x := range a {
		ka[i] = key(x)
	}
	for i, x := range b {
		kb[i] = key(x)
	}
	slices.SortFunc(ka, func(x, y [6]int) int { return slices.Compare(x[:], y[:]) })
	slices.SortFunc(kb, func(x, y [6]int) int { return slices.Compare(x[:], y[:]) })
	return slices.Equal(ka, kb)
}
