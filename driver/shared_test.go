package driver_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
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

func (f *fakeTracer) OnPublish(slot, origin int, _ time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubs = append(f.pubs, recvCall{slot: slot, origin: origin})
}

func (f *fakeTracer) OnReceive(node, slot, origin int, _ time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recvs = append(f.recvs, recvCall{node, slot, origin})
}

// RouteReceived records a block from another origin and skips the node's own
// loopback publish (origin == self).
func TestRouteReceivedSkipsLoopback(t *testing.T) {
	tr := &fakeTracer{}
	blk := &pb.Block{Slot: 4, Origin: 2}
	r := node.Received{Kind: node.KindBlock, Obj: blk, At: time.Unix(100, 0)}

	driver.RouteReceived(2, r, tr) // self == origin → skipped
	if len(tr.recvs) != 0 {
		t.Fatalf("loopback not skipped: %+v", tr.recvs)
	}

	driver.RouteReceived(5, r, tr) // different node → recorded
	if want := (recvCall{5, 4, 2}); len(tr.recvs) != 1 || tr.recvs[0] != want {
		t.Fatalf("recvs = %+v, want one %+v", tr.recvs, want)
	}
}

// SlotLoop runs one node's slot loop: it publishes the node's own duties (and
// records the publish), and a peer receives the block.
func TestSlotLoopPublishesDuties(t *testing.T) {
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
		nodes := make([]*node.Node, 2)
		for i := range nodes {
			nodes[i] = &node.Node{
				Num: i, Host: nw.Host(i), Network: nw,
				VerifyDelay: func() time.Duration { return 10 * time.Millisecond },
				D:           8, Dlo: 6, Dhi: 12,
				OnReceive: func(r node.Received) { driver.RouteReceived(i, r, tr) },
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		for _, nd := range nodes {
			if err := nd.Start(ctx); err != nil {
				t.Fatalf("start %d: %v", nd.Num, err)
			}
		}
		time.Sleep(time.Second)
		for _, nd := range nodes {
			nd.ConnectToPeers(nw.Peers(nd.Num))
		}
		for _, nd := range nodes {
			if err := nd.JoinTopics(ctx); err != nil {
				t.Fatalf("join %d: %v", nd.Num, err)
			}
		}
		time.Sleep(time.Second)
		defer func() {
			for _, nd := range nodes {
				nd.Close()
			}
		}()

		// node 0, n=2 → proposes at slot 0; no offset/jitter so it publishes at runStart.
		val := validator.New(0, 2, 1024, 0, 0, rand.New(rand.NewPCG(1, 0)))
		driver.SlotLoop(ctx, nodes[0], val, tr, time.Now(), 1, 12*time.Second)
		synctest.Wait()

		if want := (recvCall{slot: 0, origin: 0}); len(tr.pubs) != 1 || tr.pubs[0] != want {
			t.Fatalf("pubs = %+v, want one %+v", tr.pubs, want)
		}
		if want := (recvCall{1, 0, 0}); len(tr.recvs) != 1 || tr.recvs[0] != want {
			t.Fatalf("recvs = %+v, want one %+v", tr.recvs, want)
		}
	})
}
