package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
)

func epbsParams() *driver.EPBSParams {
	return &driver.EPBSParams{
		ConsensusBlockSize: 2048,
		PayloadOffset:      500 * time.Millisecond,
		PayloadJitter:      500 * time.Millisecond,
		PTCDue:             9 * time.Second, // 75% of the 12s scenario slot
	}
}

// Under ePBS the column gate is off: the vote gates on the consensus block alone, so an
// attester with a held-back custody column still votes block (the exact scenario that flips
// the vote in TestColumnGateForcedFlip). The payload and columns still disseminate.
func TestEPBSColumnGateOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := oneCommitteeColumns(4, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildEPBSScenario(t, a, 4*time.Second, nil, rec, 3, epbsParams())
		dropColumnTo(s, 1, 0) // attester 1 never completes custody — must not matter now

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		if got := rec.FractionVotedBlock(0); got != 1.0 {
			t.Fatalf("FractionVotedBlock = %v, want 1 (ePBS: consensus-block-only gate)", got)
		}
		seen := map[node.Kind]int{}
		for _, ar := range rec.Arrivals() {
			seen[ar.ID.Kind]++
		}
		if seen[node.KindBlock] != 0 {
			t.Fatalf("legacy Block arrivals = %d, want 0 under ePBS", seen[node.KindBlock])
		}
		// The proposer never self-receives: 3 receivers each for block-family messages.
		if seen[node.KindConsensusBlock] != 3 || seen[node.KindExecutionPayload] != 3 {
			t.Fatalf("consensus block / payload arrivals = %d/%d, want 3/3",
				seen[node.KindConsensusBlock], seen[node.KindExecutionPayload])
		}
		if seen[node.KindColumn] == 0 {
			t.Fatal("no column arrivals — the burst must ride the payload instant")
		}
	})
}

// The proposer publishes the consensus block at the block instant and the payload (with the
// column burst at the same instant) 0.5-1s later — the builder's reveal delay.
func TestEPBSPayloadTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ep := epbsParams()
		a := oneCommitteeColumns(4, []int{1, 2, 3}, 2)
		tr := &timeTracer{}
		s := buildEPBSScenario(t, a, 4*time.Second, nil, tr, 3, ep)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		tr.mu.Lock()
		defer tr.mu.Unlock()
		var consensusAt, payloadAt time.Time
		var attVoted, columnsAtPayload, columns int
		for _, p := range tr.pubs {
			switch p.id.Kind {
			case node.KindBlock:
				t.Fatalf("legacy Block published under ePBS: %+v", p.id)
			case node.KindConsensusBlock:
				consensusAt = p.at
			case node.KindExecutionPayload:
				payloadAt = p.at
			case node.KindColumn:
				columns++
				if p.at.Equal(payloadAt) {
					columnsAtPayload++
				}
			case node.KindAttestation:
				if p.voted {
					attVoted++
				}
			}
		}
		if consensusAt.IsZero() || payloadAt.IsZero() {
			t.Fatal("missing consensus block or payload publish")
		}
		lag := payloadAt.Sub(consensusAt)
		if lag < ep.PayloadOffset || lag >= ep.PayloadOffset+ep.PayloadJitter {
			t.Fatalf("payload lag = %v, want in [%v, %v)",
				lag, ep.PayloadOffset, ep.PayloadOffset+ep.PayloadJitter)
		}
		if columns != 2 || columnsAtPayload != 2 {
			t.Fatalf("columns published = %d (%d at the payload instant), want 2 (2)",
				columns, columnsAtPayload)
		}
		if attVoted != 3 {
			t.Fatalf("attestations voting block = %d, want 3", attVoted)
		}
	})
}

// ePBS composes with decoupled consensus unchanged: the AC vote gates on the consensus block
// alone, so the held-back custody column that flips the vote in
// TestDecoupledACVoteColumnGateFlip no longer does.
func TestEPBSDecoupledACVoteGateOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := decoupledACAssignment(4, []int{1, 2, 3}, 2)
		rec := metrics.NewRecorder()
		s := buildScenarioWith(t, a, 4*time.Second, nil, rec, 3,
			false, false, &driver.DecoupledParams{K: 1}, nil, epbsParams())
		dropColumnTo(s, 1, 0)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)

		if got := rec.FractionVotedACVote(0); got != 1.0 {
			t.Fatalf("FractionVotedACVote = %v, want 1 (ePBS: consensus-block-only gate)", got)
		}
	})
}
