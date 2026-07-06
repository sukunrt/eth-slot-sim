package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
	"github.com/ethp2p/slot-sim/validator"
)

// A sync-committee message published on its subnet round-trips end to end: the subscriber's
// NodeRunner decodes it (KindSyncMessage) and records exactly one arrival via the tracer, while
// the publisher skips its own loopback. Smallest end-to-end for pb.SyncMessage + SyncMessageTopic
// + decode + the onReceive sync-message case — membership + the coupling arrive in later milestones.
func TestNodeRunnerRecordsSyncMessage(t *testing.T) {
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
			nd := &node.Node{
				Num: i, Host: nw.Host(i), Network: nw,
				VerifyDelay: func() time.Duration { return 0 },
				D:           8, Dlo: 6, Dhi: 12,
			}
			driver.NewRunner(nd, schedule.View{}, nw.Peers(i), rec, driver.RunnerConfig{
				NumNodes: 2, BlockSize: 1024, SlotDuration: 12 * time.Second, Seed: 1}).Attach()
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
			if err := nd.Subscribe(validator.SyncMessageTopic(0)); err != nil {
				t.Fatalf("subscribe sync subnet: %v", err)
			}
		}
		time.Sleep(2 * time.Second) // settle the sync mesh

		// Node 0 (a member of subnet 0) publishes one sync message voting the prior head.
		sm := validator.MakeSyncMessage(0, 0, 0, -1)
		rec.OnPublish(metrics.SyncMessageID(0, 0, 0), false, time.Now())
		if err := nodes[0].Publish(ctx, sm.Topic, sm.Payload); err != nil {
			t.Fatalf("publish sync message: %v", err)
		}
		time.Sleep(time.Second) // propagate + drain
		for _, nd := range nodes {
			nd.Close()
		}

		arrs := rec.Arrivals()
		if len(arrs) != 1 {
			t.Fatalf("arrivals = %d, want 1 (node 1 records; node 0 skips loopback)", len(arrs))
		}
		if got := arrs[0]; got.Node != 1 || got.ID != metrics.SyncMessageID(0, 0, 0) {
			t.Fatalf("arrival = node %d %+v, want node 1 SyncMessageID(0,0,0)", got.Node, got.ID)
		}
	})
}

// A sync contribution published on the global topic round-trips: the subscriber decodes it
// (KindSyncContribution) and records one arrival; the publisher skips its loopback. The
// contribution topic shares the message-topic prefix, so this also guards that decode matches the
// exact contribution topic before the message prefix.
func TestNodeRunnerRecordsSyncContribution(t *testing.T) {
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
			nd := &node.Node{
				Num: i, Host: nw.Host(i), Network: nw,
				VerifyDelay: func() time.Duration { return 0 },
				D:           8, Dlo: 6, Dhi: 12,
			}
			driver.NewRunner(nd, schedule.View{}, nw.Peers(i), rec, driver.RunnerConfig{
				NumNodes: 2, BlockSize: 1024, SlotDuration: 12 * time.Second, Seed: 1}).Attach()
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
			if err := nd.Subscribe(validator.SyncContributionTopic); err != nil {
				t.Fatalf("subscribe contribution topic: %v", err)
			}
		}
		time.Sleep(2 * time.Second) // settle the contribution mesh

		// Node 0 (an aggregator) publishes one contribution for subnet 0 on the global topic.
		sc := validator.MakeSyncContribution(0, 0, 0)
		rec.OnPublish(metrics.SyncContributionID(0, 0, 0), false, time.Now())
		if err := nodes[0].Publish(ctx, sc.Topic, sc.Payload); err != nil {
			t.Fatalf("publish sync contribution: %v", err)
		}
		time.Sleep(time.Second) // propagate + drain
		for _, nd := range nodes {
			nd.Close()
		}

		arrs := rec.Arrivals()
		if len(arrs) != 1 {
			t.Fatalf("arrivals = %d, want 1 (node 1 records; node 0 skips loopback)", len(arrs))
		}
		if got := arrs[0]; got.Node != 1 || got.ID != metrics.SyncContributionID(0, 0, 0) {
			t.Fatalf("arrival = node %d %+v, want node 1 SyncContributionID(0,0,0)", got.Node, got.ID)
		}
	})
}
