package driver_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// arrivalKeys flattens a recorder's arrivals of one kind into sorted comparable strings —
// the parity currency: who received which message.
func arrivalKeys(rec *metrics.Recorder, kind node.Kind) []string {
	var keys []string
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != kind {
			continue
		}
		keys = append(keys, fmt.Sprintf("%d,%d,%d,%d,%d",
			ar.Node, ar.ID.Slot, ar.ID.Subnet, ar.ID.Attester, ar.ID.Origin))
	}
	slices.Sort(keys)
	return keys
}

// The headline invariant (partial-attestation-spec.md §8): a classic and a partial run on ONE
// schedule produce set-equal attestation arrival identity sets — same coverage, same
// identities, transport-independent metrics. The partial run additionally passes the absolute
// exact-coverage check (subscribers minus publisher, no dup, no leak).
func TestPartialClassicArrivalParity(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	runs := map[string][]string{}
	for _, transport := range []string{"classic", "partial"} {
		t.Run(transport, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a := genAssignment(16, 32, 4, 4, 2, 5, 42)
				rec := metrics.NewRecorder()
				var s *scenario
				if transport == "partial" {
					s = buildPartialScenario(t, a, 4*time.Second, nil, rec, 6)
				} else {
					s = buildScenario(t, a, 4*time.Second, nil, rec, 6)
				}
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				s.run(t, ctx, 1)

				assertCoverageNoLeakage(t, a, rec, 0)
				runs[transport] = arrivalKeys(rec, node.KindAttestation)
			})
		})
	}
	if len(runs["classic"]) == 0 || !slices.Equal(runs["classic"], runs["partial"]) {
		t.Fatalf("classic (%d) and partial (%d) arrival sets differ",
			len(runs["classic"]), len(runs["partial"]))
	}
}

// The vote-flip coupling under the partial transport: one attester held past the deadline
// votes prior head — its attestation lands in a SECOND fork bucket (different
// attestation_data) yet still reaches every other member, and FractionVotedBlock is exactly
// (s_c−1)/s_c. The coupling, the metric, and the fork-bucket transport all in one shot.
func TestPartialVoteFlipForkBuckets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := oneCommittee(4, []int{1, 2, 3}) // node 0 proposes; 1,2,3 attest + subscribe
		rec := metrics.NewRecorder()
		s := buildPartialScenario(t, a, 4*time.Second, map[int]bool{1: true}, rec, 3)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertCoverageNoLeakage(t, a, rec, 0) // both fork buckets delivered in full
		if got, want := rec.FractionVotedBlock(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedBlock = %v, want %v (one of three held)", got, want)
		}
	})
}

// The non-member duty publish under the partial transport: the attester subscribes nothing on
// its duty subnet — beginSlot's Join + dial-2 warmup feeds the eager fanout batch, which the
// subscribers relay to exact coverage; the uninvolved node receives nothing.
func TestPartialFanoutDutyCoverage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Node 0 attests subnet 0 but subscribes nothing; subscribers {1,2}; node 3 uninvolved.
		a := &schedule.Assignment{
			Params: schedule.Params{N: 4, V: 4, C: 1, Sc: 1, SubnetCount: 64,
				SubnetsPerNode: 1, SubscribeFloor: 2, Seed: 1, NumSlots: 1},
			SubnetSubscribers: [][]int{{1, 2}},
			Slots: []schedule.SlotPlan{{
				Slot:       0,
				Committees: [][]schedule.AttesterRef{{{Node: 0, Val: 0, Subnet: 0, Position: 0}}},
				SubnetOf:   []int{0},
			}},
		}
		rec := metrics.NewRecorder()
		s := buildPartialScenario(t, a, 4*time.Second, nil, rec, 2)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertCoverageNoLeakage(t, a, rec, 0) // exactly {1,2}; 0 (publisher), 3 (outsider) excluded
	})
}
