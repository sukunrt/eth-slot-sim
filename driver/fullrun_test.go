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

// M6: a sized full run with a SPARSE base graph (peersP well below a subnet's subscriber
// count), so a publisher is not directly connected to all of its subnet's subscribers and
// genuinely depends on dialing a couple and letting the subnet mesh relay. Every published
// attestation must still reach exactly its subnet's subscribers (minus the publisher) and
// nobody else. Race-skipped (the flood overflows TSan's epoch, like the block N-node run).
func TestFullRunCoverageNoLeakage(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		// C=4 subnets, ~2 per node, so many attesters are NOT subscribers of the subnet
		// they attest and must dial in. peersP=6 ≪ ~8 subscribers/subnet ⇒ no direct
		// publisher→all-subscribers edge; coverage must come from the relay.
		a := genAssignment(16, 32, 4, 4, 2, 5, 42)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 6)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertCoverageNoLeakage(t, a, rec, 0)
	})
}

// M6 k>1: a node holding two of the slot's attesters on one subnet emits two distinct
// attestations, each fully disseminated to that subnet's subscribers.
func TestFullRunKGreaterThanOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// vals 0 and 4 both live on node 0 (val % 4) ⇒ k=2; node 0 is not a subscriber of
		// subnet 0, so it dials in.
		a := &committee.Assignment{
			Params:            committee.Params{N: 4, V: 8, C: 1, Sc: 2, SubnetCount: 64, SubnetsPerNode: 1, SubscribeFloor: 3, NumSlots: 1},
			SubnetSubscribers: [][]int{{1, 2, 3}},
			Slots: []committee.SlotPlan{{
				Slot: 0,
				Committees: [][]committee.AttesterRef{{
					{Node: 0, Val: 0, Subnet: 0, Position: 0},
					{Node: 0, Val: 4, Subnet: 0, Position: 1},
				}},
				SubnetOf: []int{0},
			}},
		}
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 3)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertCoverageNoLeakage(t, a, rec, 0)

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

// M6 determinism: the same seed in two fresh bubbles yields the same arrival identity set
// (who received which attestation, with which vote). Delays vary with gossipsub's
// non-seeded mesh selection, so only identities are compared.
func TestFullRunDeterministic(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	capture := func(t *testing.T) []metrics.Arrival {
		var got []metrics.Arrival
		synctest.Test(t, func(t *testing.T) {
			a := genAssignment(16, 24, 2, 6, 2, 5, 99)
			rec := metrics.NewRecorder()
			s := buildScenario(t, a, 4*time.Second, nil, rec, 6)
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
		t.Fatalf("non-deterministic: %d vs %d arrivals (or differing identities)", len(a1), len(a2))
	}
}

// equalArrivals compares the arrival identity sets (who received which attestation, with
// which vote). Absolute delays are not compared: gossipsub's mesh peer selection uses
// non-seeded randomness, so relay paths vary run to run; the set of arrivals does not.
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
	cmp := func(x, y [6]int) int { return slices.Compare(x[:], y[:]) }
	slices.SortFunc(ka, cmp)
	slices.SortFunc(kb, cmp)
	return slices.Equal(ka, kb)
}
