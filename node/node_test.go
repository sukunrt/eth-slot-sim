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

		msg := validator.MakeAggregate(2, 5, 0) // slot2 subnet5 origin0 (node 0 aggregates)
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
		if agg.Slot != 2 || agg.Subnet != 5 || agg.Origin != 0 {
			t.Fatalf("decoded %+v, want slot2 subnet5 origin0", agg)
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

// Unsubscribe is the inverse of Subscribe: the node leaves the topic's mesh and stops
// receiving, but the topic stays joined so it can still fan-out publish there (the finality
// aggregator's post-publish drop). Idempotent; a never-subscribed topic is a no-op.
func TestNodeUnsubscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{N: 3, P: 2, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		nodes := buildNodes(nw, 3)
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

		// Nodes 1 and 2 subscribe a finality subnet; node 0 is a fan-out publisher (Join only).
		topic := validator.FinalityVoteTopic(3)
		for _, nd := range nodes[1:] {
			if err := nd.Subscribe(topic); err != nil {
				t.Fatalf("subscribe %d: %v", nd.Num, err)
			}
		}
		if err := nodes[0].Join(topic); err != nil {
			t.Fatalf("join: %v", err)
		}
		time.Sleep(time.Second) // mesh forms

		publish := func(val int) {
			t.Helper()
			msg := validator.MakeFinalityVote(0, 3, val, 0)
			if err := nodes[0].Publish(ctx, topic, msg.Payload); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
		publish(7)
		r := <-got
		if r.Kind != node.KindFinalityVote {
			t.Fatalf("kind = %d, want KindFinalityVote", r.Kind)
		}

		// After Unsubscribe (twice — idempotent), node 1 receives nothing further.
		nodes[1].Unsubscribe(topic)
		nodes[1].Unsubscribe(topic)
		nodes[1].Unsubscribe("/eth2/never/subscribed") // no-op
		time.Sleep(time.Second)                        // the leave propagates
		publish(8)
		time.Sleep(time.Second)
		select {
		case r := <-got:
			t.Fatalf("received %+v after Unsubscribe", r.ID)
		default:
		}

		// The topic stays joined: node 1 can still publish, and subscriber 2 receives it.
		got2 := make(chan node.Received, 8)
		nodes[2].OnReceive = func(r node.Received) { got2 <- r }
		msg := validator.MakeFinalityVote(0, 3, 9, 1)
		if err := nodes[1].Publish(ctx, topic, msg.Payload); err != nil {
			t.Fatalf("publish after unsubscribe: %v", err)
		}
		<-got2
	})
}
