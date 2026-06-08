package node_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// buildNodes wires count passive Node objects to a netsim. No validator, tracer,
// or slot clock — those belong to a Driver; these tests drive the node directly.
func buildNodes(nw *netsim.Netsim, count int) []*node.Node {
	nodes := make([]*node.Node, count)
	for i := range count {
		nodes[i] = &node.Node{
			Num: i, Host: nw.Host(i), Network: nw,
			VerifyDelay: func() time.Duration { return 10 * time.Millisecond },
			D:           8, Dlo: 6, Dhi: 12,
		}
	}
	return nodes
}

// bringUp is the minimal start-up cadence a node test needs: start, settle, dial
// peers, join topics (starts receive loops), settle so the mesh forms.
func bringUp(t *testing.T, ctx context.Context, nodes []*node.Node, nw *netsim.Netsim) {
	t.Helper()
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
}

// A node, driven directly, publishes a payload that its peer receives, decodes,
// and reports outward via OnReceive — the message-agnostic mechanism in
// isolation, no driver or validator.
func TestNodePublishReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{N: 2, P: 1, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		nodes := buildNodes(nw, 2)
		got := make(chan node.Received, 8)
		nodes[1].OnReceive = func(r node.Received) { got <- r }

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		bringUp(t, ctx, nodes, nw)
		defer func() {
			for _, nd := range nodes {
				nd.Close()
			}
		}()

		blk := &pb.Block{Slot: 0, Origin: 0, Payload: []byte("payload")}
		data, err := proto.Marshal(blk)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := nodes[0].Publish(ctx, validator.BlockTopic, data); err != nil {
			t.Fatalf("publish: %v", err)
		}

		r := <-got
		if r.Kind != node.KindBlock {
			t.Fatalf("kind = %d, want KindBlock", r.Kind)
		}
		rb, ok := r.Obj.(*pb.Block)
		if !ok {
			t.Fatalf("obj type %T, want *pb.Block", r.Obj)
		}
		if rb.Slot != 0 || rb.Origin != 0 || string(rb.Payload) != "payload" {
			t.Fatalf("decoded %+v, want slot0 origin0 payload=%q", rb, "payload")
		}
		if r.At.IsZero() {
			t.Fatal("receive time not stamped")
		}
	})
}

// An aggregate published on the global aggregate topic is received and decoded as
// KindAggregate with its fields intact. The aggregate topic isn't joined by JoinTopics
// (the driver does that), so the test subscribes it explicitly.
func TestNodeAggregateDecodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{N: 2, P: 1, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		nodes := buildNodes(nw, 2)
		got := make(chan node.Received, 8)
		nodes[1].OnReceive = func(r node.Received) { got <- r }

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		bringUp(t, ctx, nodes, nw)
		defer func() {
			for _, nd := range nodes {
				nd.Close()
			}
		}()
		for _, nd := range nodes {
			if err := nd.Subscribe(validator.AggregateTopic); err != nil {
				t.Fatalf("subscribe aggregate %d: %v", nd.Num, err)
			}
		}
		time.Sleep(time.Second) // let the aggregate mesh form

		msg := validator.MakeAggregate(2, 5, 1) // slot2 subnet5 aggIdx1
		if err := nodes[0].Publish(ctx, validator.AggregateTopic, msg.Payload); err != nil {
			t.Fatalf("publish: %v", err)
		}

		r := <-got
		if r.Kind != node.KindAggregate {
			t.Fatalf("kind = %d, want KindAggregate", r.Kind)
		}
		agg, ok := r.Obj.(*pb.Aggregate)
		if !ok {
			t.Fatalf("obj type %T, want *pb.Aggregate", r.Obj)
		}
		if agg.Slot != 2 || agg.Subnet != 5 || agg.AggIdx != 1 {
			t.Fatalf("decoded %+v, want slot2 subnet5 aggIdx1", agg)
		}
	})
}

// Publishing to a topic the node never joined is an error, not a panic.
func TestPublishUnjoinedTopic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{N: 1, P: 1, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		nodes := buildNodes(nw, 1)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := nodes[0].Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		if err := nodes[0].JoinTopics(ctx); err != nil {
			t.Fatalf("join: %v", err)
		}
		defer nodes[0].Close()
		if err := nodes[0].Publish(ctx, "/eth2/never/joined", []byte("x")); err == nil {
			t.Fatal("publish to unjoined topic: got nil error, want failure")
		}
	})
}
