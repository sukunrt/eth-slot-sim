package driver_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
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

// M4: one committee of s_c attesters, all subscribed to its subnet, no coupling. Each
// attester emits at the deadline (voting prior, since the rule isn't block-driven yet);
// every other member receives ⇒ s_c·(s_c−1) arrivals, all published exactly at
// slotStart + ATTESTATION_DUE. (A block also disseminates; we assert on attestations.)
func TestAttestationsFireAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const sc, due = 4, 4 * time.Second
		a := &committee.Assignment{
			Params:   committee.Params{N: sc, V: sc, C: 1, Sc: sc, SubnetCount: 64, BackbonePerNode: 1, NumSlots: 1},
			Backbone: [][]int{{0}, {0}, {0}, {0}}, // all subscribe subnet 0
			Slots: []committee.SlotPlan{{
				Slot: 0,
				Committees: [][]committee.AttesterRef{{
					{Node: 0, Val: 0, Subnet: 0, Position: 0},
					{Node: 1, Val: 1, Subnet: 0, Position: 1},
					{Node: 2, Val: 2, Subnet: 0, Position: 2},
					{Node: 3, Val: 3, Subnet: 0, Position: 3},
				}},
				SubnetOf:    []int{0},
				Aggregators: [][]committee.AttesterRef{{}},
				Subscribers: [][]int{{0, 1, 2, 3}},
			}},
		}
		nw, err := netsim.NewWithCommittee(a, netsim.Config{
			N: sc, P: 3, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		tr := &timeTracer{}
		d := driver.New(nw, driver.Config{
			BlockSize: 1024, SlotDuration: 12 * time.Second, Jitter: 0,
			VerifyDelay:       func() time.Duration { return 0 },
			AttestVerifyDelay: func() time.Duration { return 0 },
			AttestBatchWindow: 10 * time.Millisecond,
			D:                 8, Dlo: 6, Dhi: 12, Seed: 1,
			Committee: a, AttestationDue: due,
		}, tr)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		runStart := time.Now()
		d.Run(ctx, runStart, 1)

		tr.mu.Lock()
		defer tr.mu.Unlock()
		// (a) exactly s_c·(s_c−1) attestation arrivals.
		var attArrivals int
		for _, id := range tr.arrs {
			if id.Kind == node.KindAttestation {
				attArrivals++
			}
		}
		if want := sc * (sc - 1); attArrivals != want {
			t.Fatalf("attestation arrivals = %d, want %d", attArrivals, want)
		}
		// (b) every attester published exactly at slotStart + due, voting prior.
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
				t.Fatalf("attestation %+v voted block, want prior (no coupling)", p.id)
			}
		}
		if attPubs != sc {
			t.Fatalf("attestation publishes = %d, want %d", attPubs, sc)
		}
	})
}
