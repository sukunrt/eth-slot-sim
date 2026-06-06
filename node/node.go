// Package node is the message-agnostic half of the simulator: it owns the
// libp2p host, gossipsub, topic membership, peer connections, the slot clock,
// and send/receive. Each slot it asks its Validator for duties and publishes
// them; it knows nothing about "block". Adding attestations later grows the
// Validator, not the Node.
package node

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// Node is one simulated beacon node. Exported fields are set by the caller
// before Start; the Node loads no configuration itself.
type Node struct {
	Num          int
	Host         host.Host
	Network      Network
	Validator    *validator.Validator
	Tracer       metrics.Tracer
	SlotDuration time.Duration
	VerifyDelay  func() time.Duration // per-hop processing cost (validation-as-sleep)
	D, Dlo, Dhi  int

	ps    *pubsub.PubSub
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

// Start brings up gossipsub with the Prysm-tuned parameters. It does not set
// WithFloodPublish or WithPeerScore — matching Prysm, which leaves both at the
// library default. ctx is the pubsub lifecycle context.
func (n *Node) Start(ctx context.Context) error {
	params := pubsub.DefaultGossipSubParams()
	params.D = n.D
	params.Dlo = n.Dlo
	params.Dhi = n.Dhi
	params.Dout = 1
	params.Dlazy = 6
	params.HeartbeatInterval = 700 * time.Millisecond
	params.FanoutTTL = 60 * time.Second
	params.HistoryLength = 6
	params.HistoryGossip = 3

	ps, err := pubsub.NewGossipSub(ctx, n.Host,
		pubsub.WithGossipSubParams(params),
		pubsub.WithMessageIdFn(MessageIDFunc),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(1000),
		pubsub.WithValidateQueueSize(600),
	)
	if err != nil {
		return err
	}
	n.ps = ps
	return nil
}

// JoinTopics registers the verify hook, joins the global block topic, and
// subscribes. The verify hook sleeps the processing cost and accepts; gossipsub
// won't forward until it returns, so the delay sits on the propagation path at
// every hop. Async (default) form — never inline, which would serialize relays.
func (n *Node) JoinTopics() error {
	err := n.ps.RegisterTopicValidator(validator.BlockTopic,
		func(context.Context, peer.ID, *pubsub.Message) pubsub.ValidationResult {
			time.Sleep(n.VerifyDelay())
			return pubsub.ValidationAccept
		})
	if err != nil {
		return err
	}
	topic, err := n.ps.Join(validator.BlockTopic)
	if err != nil {
		return err
	}
	n.topic = topic
	sub, err := topic.Subscribe(pubsub.WithBufferSize(4096))
	if err != nil {
		return err
	}
	n.sub = sub
	return nil
}

// ConnectToPeers dials the given node numbers. It skips peers <= Num so each
// undirected edge is dialed once; the QUIC connection is bidirectional.
func (n *Node) ConnectToPeers(peers []int) {
	ctx := context.Background()
	var wg sync.WaitGroup
	sema := make(chan struct{}, 20)
	for _, peerNum := range peers {
		if peerNum <= n.Num {
			continue
		}
		sema <- struct{}{}
		wg.Go(func() {
			defer func() { <-sema }()
			peerID, err := PeerIDFromNodeNum(peerNum)
			if err != nil {
				slog.Error("peer ID failed", "node", n.Num, "peer", peerNum, "err", err)
				return
			}
			addr := n.Network.PeerAddr(peerNum)
			if err := n.Host.Connect(ctx, peer.AddrInfo{ID: peerID, Addrs: []ma.Multiaddr{addr}}); err != nil {
				slog.Error("connect failed", "node", n.Num, "peer", peerNum, "err", err)
			}
		})
	}
	wg.Wait()
}

// Run executes numSlots slots from runStart (shared by all nodes so arrival
// times share an origin), publishing each slot's duties at their offset and
// recording received blocks. It returns once the run plus a drain window
// completes.
func (n *Node) Run(ctx context.Context, runStart time.Time, numSlots int) {
	recvCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { n.receive(recvCtx) })

	for slot := range numSlots {
		slotStart := runStart.Add(time.Duration(slot) * n.SlotDuration)
		duties := n.Validator.Duties(slot)
		sort.Slice(duties, func(i, j int) bool { return duties[i].At < duties[j].At })
		for _, d := range duties {
			time.Sleep(time.Until(slotStart.Add(d.At)))
			n.publish(ctx, d.Msg)
		}
		time.Sleep(time.Until(slotStart.Add(n.SlotDuration)))
	}

	time.Sleep(n.SlotDuration) // drain in-flight receives
	stop()
	wg.Wait()
}

// publish records the publish time (synchronously, before the send) then
// publishes the raw payload onto the topic.
func (n *Node) publish(ctx context.Context, msg validator.Message) {
	n.Tracer.OnPublish(msg.Slot, n.Num, time.Now())
	if err := n.topic.Publish(ctx, msg.Payload); err != nil {
		slog.Error("publish failed", "node", n.Num, "slot", msg.Slot, "err", err)
	}
}

// receive decodes each block off the subscription and records its arrival,
// skipping the node's own locally-delivered publish (origin == self).
func (n *Node) receive(ctx context.Context) {
	for {
		msg, err := n.sub.Next(ctx)
		if err != nil {
			return
		}
		var blk pb.Block
		if err := proto.Unmarshal(msg.Data, &blk); err != nil {
			slog.Error("unmarshal failed", "node", n.Num, "err", err)
			continue
		}
		if int(blk.Origin) == n.Num {
			continue
		}
		n.Tracer.OnReceive(n.Num, int(blk.Slot), int(blk.Origin), time.Now())
	}
}
