package driver_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
)

type pubEntry struct {
	id    metrics.MsgID
	voted bool
	at    time.Time
}

// timeTracer records publish identities/times and arrival identities, for asserting
// both the count and the exact emit instant. Concurrency-safe.
type timeTracer struct {
	mu   sync.Mutex
	pubs []pubEntry
	arrs []metrics.MsgID
}

func (t *timeTracer) OnPublish(id metrics.MsgID, voted bool, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pubs = append(t.pubs, pubEntry{id, voted, at})
}

func (t *timeTracer) OnReceive(_ int, id metrics.MsgID, _ time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.arrs = append(t.arrs, id)
}

// M4: a committee of s_c attesters, all subscribed, with NO block processed (the block
// is held from every attester) — isolating firing from the coupling. Each attester
// emits at the deadline voting prior; every other member receives ⇒ s_c·(s_c−1)
// arrivals, all published exactly at slotStart + ATTESTATION_DUE.
func TestAttestationsFireAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const sc, due = 3, 4 * time.Second
		a := oneCommittee(4, []int{1, 2, 3}) // node 0 is the (excluded) proposer
		tr := &timeTracer{}
		s := buildScenario(t, a, due, map[int]bool{1: true, 2: true, 3: true}, tr, 3)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		runStart := s.run(t, ctx, 1)

		tr.mu.Lock()
		defer tr.mu.Unlock()
		var attArrivals int
		for _, id := range tr.arrs {
			if id.Kind == node.KindAttestation {
				attArrivals++
			}
		}
		if want := sc * (sc - 1); attArrivals != want {
			t.Fatalf("attestation arrivals = %d, want %d", attArrivals, want)
		}

		var attPubs int
		wantAt := runStart.Add(due)
		for _, p := range tr.pubs {
			if p.id.Kind != node.KindAttestation {
				continue
			}
			attPubs++
			if !p.at.Equal(wantAt) {
				t.Fatalf("attestation publish at %v, want %v (slotStart+due)", p.at, wantAt)
			}
			if p.voted {
				t.Fatalf("attestation %+v voted block, want prior (block held)", p.id)
			}
		}
		if attPubs != sc {
			t.Fatalf("attestation publishes = %d, want %d", attPubs, sc)
		}
	})
}
