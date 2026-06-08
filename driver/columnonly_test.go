package driver_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/validator"
)

// A data column published on its subnet round-trips end to end: the custodier's NodeRunner
// decodes it (KindColumn) and records exactly one arrival via the tracer, while the
// publisher skips its own loopback. Smallest end-to-end for pb.Column + ColumnTopic +
// decode + the onReceive column case — the full t=0 burst + custody arrive in later milestones.
func TestNodeRunnerRecordsColumn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{
			N: 2, P: 1, Seed: 1,
			MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		rec := metrics.NewRecorder()
		nodes := make([]*node.Node, 2)
		for i := range nodes {
			val := validator.New(i, 2, 1024, 0, 0, rand.New(rand.NewPCG(1, uint64(i))), nil)
			nd := &node.Node{
				Num: i, Host: nw.Host(i), Network: nw,
				VerifyDelay: func() time.Duration { return 0 },
				D:           8, Dlo: 6, Dhi: 12,
			}
			driver.NewRunner(i, nd, val, nil, false, rec, 12*time.Second, 0, 0, 0, 1, nw.Peers(i)).Attach()
			nodes[i] = nd
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		for _, nd := range nodes {
			if err := nd.Start(ctx); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		for _, nd := range nodes {
			nd.ConnectToPeers(nw.Peers(nd.Num))
		}
		for _, nd := range nodes {
			if err := nd.JoinTopics(ctx); err != nil {
				t.Fatal(err)
			}
			if err := nd.Subscribe(validator.ColumnTopic(0)); err != nil {
				t.Fatalf("subscribe column: %v", err)
			}
		}
		time.Sleep(2 * time.Second) // settle the column mesh

		// Node 0 publishes one column on subnet 0; both nodes custody it (subscribed above).
		col := validator.MakeColumn(0, 0, 0)
		rec.OnPublish(metrics.ColumnID(0, 0, 0), false, time.Now())
		if err := nodes[0].Publish(ctx, col.Topic, col.Payload); err != nil {
			t.Fatalf("publish column: %v", err)
		}
		time.Sleep(time.Second) // propagate + drain
		for _, nd := range nodes {
			nd.Close()
		}

		arrs := rec.Arrivals()
		if len(arrs) != 1 {
			t.Fatalf("arrivals = %d, want 1 (node 1 records; node 0 skips loopback)", len(arrs))
		}
		if got := arrs[0]; got.Node != 1 || got.ID != metrics.ColumnID(0, 0, 0) {
			t.Fatalf("arrival = node %d %+v, want node 1 ColumnID(0,0,0)", got.Node, got.ID)
		}
	})
}
