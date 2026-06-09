package driver_test

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// Hold the block from exactly one member past the deadline: it votes prior head, the others vote
// head, and the fraction is exactly (members−1)/members — the sync analogue of the attestation
// forced flip, but on the un-gated block-seen rule (min(block_seen, deadline)).
func TestSyncCouplingForcedFlip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := syncAssignment(4, [][]int{{1, 2, 3}}, 0) // members 1,2,3 on subnet 0; proposer 0 (outside)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, map[int]bool{1: true}, rec, 3) // block held from member 1

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		if got, want := rec.FractionVotedHead(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedHead = %v, want %v ((members-1)/members)", got, want)
		}

		// Per member (member == publishing node): member 1 voted prior, 2,3 voted head; each
		// member's message reached the other two.
		voted := map[int]bool{}
		seen := map[int]int{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind != node.KindSyncMessage {
				continue
			}
			voted[ar.ID.Attester] = ar.VotedBlock
			seen[ar.ID.Attester]++
		}
		if voted[1] {
			t.Fatal("member 1 (block held) voted head, want prior")
		}
		if !voted[2] || !voted[3] {
			t.Fatalf("members 2,3 voted prior, want head (voted=%v)", voted)
		}
		for _, m := range []int{1, 2, 3} {
			if seen[m] != 2 {
				t.Fatalf("member %d arrivals = %d, want 2", m, seen[m])
			}
		}
	})
}

// oneCommitteeColumnsSync extends oneCommitteeColumns with sync membership: the committee's nodes
// also form a single sync subnet, so each is both an attester-with-custody and a sync member —
// letting a dropped column flip a node's attestation (DA-gated) while its sync vote (un-gated)
// stays head.
func oneCommitteeColumnsSync(n int, attesters []int, numColumns int) *schedule.Assignment {
	a := oneCommitteeColumns(n, attesters, numColumns)
	a.SyncSubscribers = [][]int{slices.Sorted(slices.Values(attesters))}
	return a
}

// The headline contrast: sync is un-gated by data availability. Drop one custody column from a
// node that HAS the block and is both an attester and a sync member — its attestation votes prior
// (the column gate), but its sync message votes head (block-seen alone). The two metrics diverge
// for exactly that node, which is what isolates the DA gate's effect on the head vote.
func TestSyncUngatedByColumns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := oneCommitteeColumnsSync(4, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 3)
		dropColumnTo(s, 1, 0) // node 1 has the block but never completes custody column 0

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		// Attestation: node 1 is missing a column ⇒ prior; 2,3 ⇒ block. Sync: all have the block ⇒ head.
		if got, want := rec.FractionVotedBlock(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedBlock = %v, want %v (node 1 missing a custody column)", got, want)
		}
		if got := rec.FractionVotedHead(0); got != 1.0 {
			t.Fatalf("FractionVotedHead = %v, want 1.0 (sync un-gated by the missing column)", got)
		}

		// The same node 1: attestation prior, sync head — the divergence the metric reports.
		att := map[int]bool{}
		sync := map[int]bool{}
		for _, ar := range rec.Arrivals() {
			switch ar.ID.Kind {
			case node.KindAttestation:
				att[ar.ID.Attester] = ar.VotedBlock
			case node.KindSyncMessage:
				sync[ar.ID.Attester] = ar.VotedBlock
			}
		}
		if att[1] {
			t.Fatal("node 1 attestation voted block, want prior (missing custody column)")
		}
		if !sync[1] {
			t.Fatal("node 1 sync voted prior, want head (un-gated by the missing column)")
		}
	})
}
