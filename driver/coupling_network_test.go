package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
)

// M5 (B): hold the block from exactly one attester and the crossing is forced by
// construction — that node votes prior, the others vote block, and the fraction is
// exactly (s_c−1)/s_c. The proposer (node 0) is outside the committee, so nodes 2,3
// see its block in time while node 1 never processes it.
func TestCouplingForcedFlip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := oneCommittee(4, []int{1, 2, 3})
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, map[int]bool{1: true}, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, time.Now(), 1)

		if got, want := rec.FractionVotedBlock(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedBlock = %v, want %v ((s_c-1)/s_c)", got, want)
		}

		// Per attester (val == node): node 1 voted prior, nodes 2,3 voted block; each
		// attestation reached the other two subscribers.
		voted := map[int]bool{}
		seen := map[int]int{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind != node.KindAttestation {
				continue
			}
			voted[ar.ID.Attester] = ar.VotedBlock
			seen[ar.ID.Attester]++
		}
		if voted[1] {
			t.Fatal("attester 1 (block held) voted block, want prior")
		}
		if !voted[2] || !voted[3] {
			t.Fatalf("attesters 2,3 voted prior, want block (voted=%v)", voted)
		}
		for _, att := range []int{1, 2, 3} {
			if seen[att] != 2 {
				t.Fatalf("attester %d arrivals = %d, want 2", att, seen[att])
			}
		}
	})
}

// M5 metric/monotonicity: move the deadline across the block-arrival time over the real
// receive path (no suppression). Earlier than the block can arrive ⇒ everyone votes
// prior (0); well past it ⇒ everyone votes block (1). With the forced-flip's (s_c−1)/s_c
// in between, the fraction is non-decreasing in the deadline.
func TestCouplingDeadlineSweep(t *testing.T) {
	cases := []struct {
		name string
		due  time.Duration
		want float64
	}{
		{"deadline before block arrives", time.Millisecond, 0.0}, // block (≈5ms) is always late
		{"deadline well after block", 4 * time.Second, 1.0},      // block always in time
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a := oneCommittee(4, []int{1, 2, 3})
				rec := metrics.NewRecorder()
				s := buildScenario(t, a, c.due, nil, rec)

				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				s.run(t, ctx, time.Now(), 1)

				if got := rec.FractionVotedBlock(0); got != c.want {
					t.Fatalf("due=%v: FractionVotedBlock = %v, want %v", c.due, got, c.want)
				}
			})
		})
	}
}
