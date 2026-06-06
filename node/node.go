// Package node is the message-agnostic, passive half of the simulator: it owns
// the libp2p host, gossipsub, topic membership, peer connections, and
// send/receive. It responds to requests (Connect, JoinTopics, Publish) and
// reports what it receives outward via OnReceive; it knows nothing about slots,
// duties, or metrics. A Driver supplies the timing and orchestration.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// Kind tags a received, decoded message so a Driver can dispatch on it without
// type-asserting to discover what it got. Values are predefined per message type.
type Kind int

const (
	KindBlock       Kind = 1
	KindAttestation Kind = 2
)

// Received is the node's outward hand-off for one decoded message: the node
// infers the type from the gossipsub topic, unmarshals, and tags Obj with Kind.
type Received struct {
	Kind Kind
	Obj  any
	At   time.Time
}

// Node is one simulated beacon node. Exported fields are set by the caller
// before Start; the Node loads no configuration itself.
type Node struct {
	Num         int
	Host        host.Host
	Network     Network
	VerifyDelay func() time.Duration // per-hop processing cost (validation-as-sleep)
	D, Dlo, Dhi int
	OnReceive   func(Received) // outward sink; set before JoinTopics

	ps     *pubsub.PubSub
	topics map[string]*pubsub.Topic
	sub    *pubsub.Subscription
	cancel context.CancelFunc // stops the receive loop
	wg     sync.WaitGroup     // tracks the receive loop until Close
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

// JoinTopics registers the verify hook, joins the global block topic,
// subscribes, and starts the receive loop on ctx. The verify hook sleeps the
// processing cost and accepts; gossipsub won't forward until it returns, so the
// delay sits on the propagation path at every hop. Async (default) form — never
// inline, which would serialize relays. Set OnReceive before calling.
func (n *Node) JoinTopics(ctx context.Context) error {
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
	n.topics = map[string]*pubsub.Topic{validator.BlockTopic: topic}
	sub, err := topic.Subscribe(pubsub.WithBufferSize(4096))
	if err != nil {
		return err
	}
	n.sub = sub
	rctx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	n.wg.Go(func() { n.receive(rctx) })
	return nil
}

// Close stops the receive loop and waits for it to exit. A no-op if the node
// never joined; safe to call once.
func (n *Node) Close() {
	if n.cancel != nil {
		n.cancel()
	}
	n.wg.Wait()
	if n.sub != nil {
		n.sub.Cancel()
	}
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

// Publish sends payload on the named (already-joined) topic.
func (n *Node) Publish(ctx context.Context, topic string, payload []byte) error {
	t, ok := n.topics[topic]
	if !ok {
		return fmt.Errorf("publish: topic %q not joined", topic)
	}
	return t.Publish(ctx, payload)
}

// receive decodes each message off the subscription and hands it outward via
// OnReceive. It does not skip the node's own loopback publish — there is no
// clean origin in gossipsub, so that policy lives in the consumer.
func (n *Node) receive(ctx context.Context) {
	for {
		msg, err := n.sub.Next(ctx)
		if err != nil {
			return
		}
		rec, err := decode(msg.GetTopic(), msg.Data, time.Now())
		if err != nil {
			slog.Error("decode failed", "node", n.Num, "err", err)
			continue
		}
		if n.OnReceive != nil {
			n.OnReceive(rec)
		}
	}
}

// decode infers the message type from the gossipsub topic, unmarshals it, and
// tags it with its Kind.
func decode(topic string, data []byte, at time.Time) (Received, error) {
	switch topic {
	case validator.BlockTopic:
		blk := new(pb.Block)
		if err := proto.Unmarshal(data, blk); err != nil {
			return Received{}, err
		}
		return Received{Kind: KindBlock, Obj: blk, At: at}, nil
	default:
		return Received{}, fmt.Errorf("unknown topic %q", topic)
	}
}
