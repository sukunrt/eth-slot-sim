package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// epbsPTCAssignment extends oneCommitteeColumns with a PTC draw: ptcVoters (one validator
// each, val == node) vote payload timeliness on the global topic. The proposer (node 0) may
// be among them — its self-case (it never self-receives payload or columns) is part of the
// contract under test.
func epbsPTCAssignment(n int, attesters, ptcVoters []int, numColumns int) *schedule.Assignment {
	a := oneCommitteeColumns(n, attesters, numColumns)
	refs := make([]schedule.AttesterRef, len(ptcVoters))
	for i, nd := range ptcVoters {
		refs[i] = schedule.AttesterRef{Node: nd, Val: nd}
	}
	a.Params.PTCSize = len(ptcVoters)
	a.Slots[0].PTCVoters = refs
	return a
}

// dropPayloadTo installs a test-only OnReceive filter on node victim that holds back the
// execution payload (mirroring dropColumnTo) — so the victim never records payload-seen.
func dropPayloadTo(s *scenario, victim int) {
	nd := s.nodes[victim]
	orig := nd.OnReceive
	nd.OnReceive = func(rec node.Received) {
		if rec.Kind == node.KindExecutionPayload {
			return
		}
		orig(rec)
	}
}

// dropConsensusBlockTo holds back the ePBS consensus block from node victim — the "no block
// seen ⇒ no PTC vote" case (the ePBS twin of buildScenario's suppressBlock knob, which only
// drops the legacy Block kind).
func dropConsensusBlockTo(s *scenario, victim int) {
	nd := s.nodes[victim]
	orig := nd.OnReceive
	nd.OnReceive = func(rec node.Received) {
		if rec.Kind == node.KindConsensusBlock {
			return
		}
		orig(rec)
	}
}

// ptcKey identifies one published PTC vote across its arrivals (val rides Attester, the
// publisher rides Origin — the AC-vote identity shape).
type ptcKey struct{ val, origin int }

// assertPTCVoteCoverage checks the global-topic invariant: each scheduled member's PTC vote
// reaches every node except its publisher, exactly once (N−1 coverage, no leak, no dup).
func assertPTCVoteCoverage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	got := map[ptcKey]map[int]int{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindPTCVote || ar.ID.Slot != slot {
			continue
		}
		k := ptcKey{ar.ID.Attester, ar.ID.Origin}
		if got[k] == nil {
			got[k] = map[int]int{}
		}
		got[k][ar.Node]++
	}
	for _, r := range a.Slots[slot].PTCVoters {
		recvd := got[ptcKey{r.Val, r.Node}]
		for nd := range a.Params.N {
			want := 1
			if nd == r.Node {
				want = 0 // the publisher's own loopback is skipped
			}
			if recvd[nd] != want {
				t.Fatalf("PTC vote val %d origin %d at node %d: got %d arrivals, want %d",
					r.Val, r.Node, nd, recvd[nd], want)
			}
		}
	}
}

// Happy path: every PTC member (the proposer included — the self-case) votes present, and
// each vote reaches every other node exactly once.
func TestEPBSPTCVoteCoverage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := epbsPTCAssignment(4, []int{1, 2, 3}, []int{0, 1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildEPBSScenario(t, a, 4*time.Second, nil, rec, 3, epbsParams())

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertPTCVoteCoverage(t, a, rec, 0)
		if got := rec.FractionVotedPTC(0); got != 1.0 {
			t.Fatalf("FractionVotedPTC = %v, want 1.0 (all members saw payload + columns)", got)
		}
	})
}

// A held-back payload flips exactly its victim's vote to present=false — the vote still
// publishes (the consensus block was seen), it just reports the payload missing.
func TestEPBSPTCPayloadHeldBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := epbsPTCAssignment(4, []int{1, 2, 3}, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildEPBSScenario(t, a, 4*time.Second, nil, rec, 3, epbsParams())
		dropPayloadTo(s, 1)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		assertPTCVoteCoverage(t, a, rec, 0)
		if got, want := rec.FractionVotedPTC(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedPTC = %v, want %v (one member missed the payload)", got, want)
		}
	})
}

// A held-back custody column flips the victim's PTC vote to present=false while its
// attestation still votes block — under ePBS the DA check moved from the attestation gate
// to the PTC vote; it didn't die.
func TestEPBSPTCColumnHeldBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := epbsPTCAssignment(4, []int{1, 2, 3}, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildEPBSScenario(t, a, 4*time.Second, nil, rec, 3, epbsParams())
		dropColumnTo(s, 1, 0)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		if got := rec.FractionVotedBlock(0); got != 1.0 {
			t.Fatalf("FractionVotedBlock = %v, want 1 (the attestation is not column-gated)", got)
		}
		if got, want := rec.FractionVotedPTC(0), 2.0/3.0; got != want {
			t.Fatalf("FractionVotedPTC = %v, want %v (one member missed a custody column)", got, want)
		}
	})
}

// No consensus block seen ⇒ no PTC vote at all (spec: nothing to attest to); the other
// members still publish.
func TestEPBSPTCNoBlockNoVote(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := epbsPTCAssignment(4, []int{1, 2, 3}, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildEPBSScenario(t, a, 4*time.Second, nil, rec, 3, epbsParams())
		dropConsensusBlockTo(s, 1)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		voters := map[int]bool{}
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindPTCVote {
				voters[ar.ID.Origin] = true
			}
		}
		if voters[1] {
			t.Fatal("node 1 published a PTC vote without seeing the consensus block")
		}
		if !voters[2] || !voters[3] {
			t.Fatalf("PTC votes seen from origins %v, want 2 and 3", voters)
		}
	})
}
