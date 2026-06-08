package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
)

// With a committee but Attest:false the Driver is block-only: the block still follows the
// committee's proposer schedule (so dissemination is measured on the same network), but no
// attestations are emitted — not even by a proposer who is also an attester this slot.
func TestBlockOnlyKeepsScheduleDropsAttestations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := oneCommittee(4, []int{1, 2, 3}) // subnet-0 committee {1,2,3}
		a.Slots[0].Proposer = 1              // proposer is also an attester — must still stay silent

		nw, err := netsim.NewWithCommittee(a, netsim.Config{
			N: 4, P: 3, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		tr := &timeTracer{}
		d := driver.New(nw, driver.Config{
			BlockSize: 1024, SlotDuration: 12 * time.Second, Jitter: 0,
			VerifyDelay: func() time.Duration { return 0 },
			D:           8, Dlo: 6, Dhi: 12, Seed: 1,
			Committee: a, Attest: false, AttestationDue: 4 * time.Second,
		}, tr)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), 1)

		tr.mu.Lock()
		defer tr.mu.Unlock()
		var blocks int
		for _, p := range tr.pubs {
			if p.id.Kind == node.KindAttestation {
				t.Fatalf("attestation published with Attest=false: %+v", p.id)
			}
			if p.id.Kind == node.KindBlock {
				blocks++
				if p.id.Origin != 1 {
					t.Fatalf("block origin = %d, want 1 (scheduled proposer)", p.id.Origin)
				}
			}
		}
		if blocks != 1 {
			t.Fatalf("block publishes = %d, want 1", blocks)
		}
	})
}
