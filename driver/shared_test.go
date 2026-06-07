package driver_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
)

// netsim is the in-process Fabric the simnet driver runs over.
var _ driver.Fabric = (*netsim.Netsim)(nil)

type recvCall struct {
	node, slot, origin int
}

// fakeTracer records calls; concurrency-safe (publish/receive fire from
// separate goroutines).
type fakeTracer struct {
	mu    sync.Mutex
	pubs  []recvCall // node field unused for publishes
	recvs []recvCall
}

func (f *fakeTracer) OnPublish(id metrics.MsgID, _ bool, _ time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubs = append(f.pubs, recvCall{slot: id.Slot, origin: id.Origin})
}

func (f *fakeTracer) OnReceive(rcv int, id metrics.MsgID, _ time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recvs = append(f.recvs, recvCall{rcv, id.Slot, id.Origin})
}

// A block-only NodeRunner publishes its node's block duty; a peer records it, and the
// runner skips its own loopback (origin == self). Drives the full Driver for one slot.
func TestNodeRunnerPublishesBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{
			N: 2, P: 1, Seed: 1,
			MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		tr := &fakeTracer{}
		d := driver.New(nw, driver.Config{
			BlockSize: 1024, SlotDuration: 12 * time.Second, Jitter: 0,
			VerifyDelay: func() time.Duration { return 10 * time.Millisecond },
			D:           8, Dlo: 6, Dhi: 12, Seed: 1,
		}, tr)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), 1) // slot 0 → node 0 proposes

		if want := (recvCall{slot: 0, origin: 0}); len(tr.pubs) != 1 || tr.pubs[0] != want {
			t.Fatalf("pubs = %+v, want one %+v", tr.pubs, want)
		}
		if want := (recvCall{1, 0, 0}); len(tr.recvs) != 1 || tr.recvs[0] != want {
			t.Fatalf("recvs = %+v, want one %+v (peer records, proposer skips loopback)", tr.recvs, want)
		}
	})
}
