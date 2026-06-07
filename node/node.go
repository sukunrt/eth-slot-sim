// Package node is the message-agnostic, passive half of the simulator: it owns
// the libp2p host, gossipsub, topic membership, peer connections, and
// send/receive. It responds to requests (Connect, JoinTopics, Join, Subscribe,
// Publish) and reports what it receives outward via OnReceive; it knows nothing
// about slots, duties, or metrics. A Driver supplies the timing and orchestration.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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

// defaultBatchWindow is the attestation verifier's batch window when unset; it must
// be > 0 so the verifier's idle timer parks (a zero window busy-spins, §8.1).
const defaultBatchWindow = 50 * time.Millisecond

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
	VerifyDelay func() time.Duration // per-hop block verify cost (validation-as-sleep)
	D, Dlo, Dhi int
	OnReceive   func(Received) // outward sink; set before JoinTopics

	// Attestation verify-hook (batched): models the t≈4s flood as a single-server
	// queue. AttestVerifyDelay defaults to VerifyDelay; AttestBatchWindow to
	// defaultBatchWindow.
	AttestVerifyDelay func() time.Duration
	AttestPerItem     time.Duration
	AttestBatchWindow time.Duration

	ps       *pubsub.PubSub
	verifier *batchVerifier

	mu        sync.Mutex
	topics    map[string]*pubsub.Topic
	validated map[string]bool // topics with a registered verify hook (register-once)
	subs      map[string]*pubsub.Subscription

	rctx   context.Context // parent of every receive goroutine; cancelled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Start brings up gossipsub with the Prysm-tuned parameters (D/Dlo/Dhi, 700ms heartbeat,
// 60s fanout TTL, mcache 6/3). Flood-publish is intentionally OFF, matching Prysm/mainnet
// (go-libp2p-pubsub defaults it off, and Prysm sets no WithFloodPublish): a publisher
// pushes to its mesh and relays carry it multi-hop. A node publishing on a duty subnet it
// only Joined (no mesh) still reaches subscribers via gossipsub fanout — it has dialed 2 of
// them at slot start, so the fanout has somewhere to land. Flooding instead would make the
// proposer upload K copies of the block (K×size), which a low-bandwidth proposer can't push
// at large K. ctx is the pubsub lifecycle context.
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

	// maxMessageSize matches Ethereum mainnet's GOSSIP_MAX_SIZE (10 MiB). The
	// pubsub default is 1 MiB, which silently drops a 1 MiB block once wrapped in
	// its protobuf/pubsub envelope.
	const maxMessageSize = 10 * 1024 * 1024
	ps, err := pubsub.NewGossipSub(ctx, n.Host,
		pubsub.WithGossipSubParams(params),
		pubsub.WithMessageIdFn(MessageIDFunc),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(1000),
		pubsub.WithValidateQueueSize(600),
		pubsub.WithMaxMessageSize(maxMessageSize),
	)
	if err != nil {
		return err
	}
	n.ps = ps
	return nil
}

// JoinTopics initializes topic state, starts the per-node attestation verifier, and
// joins + subscribes the global block topic (starting its receive loop). Per-subnet
// membership is added later via Join (publish-only) and Subscribe (mesh). Set
// OnReceive before calling.
func (n *Node) JoinTopics(ctx context.Context) error {
	n.topics = make(map[string]*pubsub.Topic)
	n.validated = make(map[string]bool)
	n.subs = make(map[string]*pubsub.Subscription)
	n.rctx, n.cancel = context.WithCancel(ctx)

	base := n.AttestVerifyDelay
	if base == nil {
		base = n.VerifyDelay
	}
	window := n.AttestBatchWindow
	if window <= 0 {
		window = defaultBatchWindow
	}
	n.verifier = newBatchVerifier(base, n.AttestPerItem, window, slog.Default())
	go n.verifier.run()

	// The block topic: Join + Subscribe with the fixed verify hook, as Phase 1.
	return n.Subscribe(validator.BlockTopic)
}

// Join makes the node a publisher on topic without joining its mesh (fan-out
// publish). Idempotent; registers the topic's verify hook on first join.
func (n *Node) Join(topic string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.joinLocked(topic)
}

// joinLocked joins topic and registers its verify hook; caller holds n.mu.
func (n *Node) joinLocked(topic string) error {
	if _, ok := n.topics[topic]; ok {
		return nil
	}
	if err := n.registerVerifyHook(topic); err != nil {
		return err
	}
	t, err := n.ps.Join(topic)
	if err != nil {
		return err
	}
	n.topics[topic] = t
	return nil
}

// Subscribe joins topic's mesh (relays + receives), starting a receive goroutine.
// Idempotent. A node subscribes its own subnets once at bring-up; per-slot duty
// subnets it doesn't subscribe are Join-only (publish-only fan-out).
func (n *Node) Subscribe(topic string) error {
	n.mu.Lock()
	if err := n.joinLocked(topic); err != nil {
		n.mu.Unlock()
		return err
	}
	if _, ok := n.subs[topic]; ok {
		n.mu.Unlock()
		return nil
	}
	t := n.topics[topic]
	n.mu.Unlock()

	sub, err := t.Subscribe(pubsub.WithBufferSize(4096))
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.subs[topic] = sub
	n.mu.Unlock()
	n.wg.Go(func() { n.receive(n.rctx, sub) })
	return nil
}

// registerVerifyHook registers topic's validation-as-sleep hook once: attestation
// subnets share the batched verifier (the M/D/1 flood queue); everything else (the
// block) gets the fixed per-hop delay. Caller holds n.mu.
func (n *Node) registerVerifyHook(topic string) error {
	if n.validated[topic] {
		return nil
	}
	var hook pubsub.ValidatorEx
	if strings.HasPrefix(topic, validator.AttestationTopicPrefix) {
		hook = func(context.Context, peer.ID, *pubsub.Message) pubsub.ValidationResult {
			n.verifier.submitAndWait(verificationItem{Attestations: []any{nil}})
			return pubsub.ValidationAccept
		}
	} else {
		hook = func(context.Context, peer.ID, *pubsub.Message) pubsub.ValidationResult {
			time.Sleep(n.VerifyDelay())
			return pubsub.ValidationAccept
		}
	}
	if err := n.ps.RegisterTopicValidator(topic, hook); err != nil {
		return err
	}
	n.validated[topic] = true
	return nil
}

// Close stops the receive loops and the verifier and waits for them to exit. A
// no-op if the node never joined; safe to call once.
func (n *Node) Close() {
	if n.cancel != nil {
		n.cancel()
	}
	n.wg.Wait()
	n.mu.Lock()
	for _, sub := range n.subs {
		sub.Cancel()
	}
	n.mu.Unlock()
	if n.verifier != nil {
		n.verifier.stop()
	}
}

// ConnectToPeers dials the given node numbers for the long-lived base graph, skipping
// peers <= Num so each undirected edge is dialed once; the QUIC connection is bidirectional.
func (n *Node) ConnectToPeers(peers []int) {
	out := peers[:0:0]
	for _, p := range peers {
		if p > n.Num {
			out = append(out, p)
		}
	}
	n.Dial(out)
}

// Dial connects to the given node numbers (no dedup skip — for the per-slot subnet
// dials). An already-open connection is reused.
func (n *Node) Dial(peers []int) {
	ctx := context.Background()
	var wg sync.WaitGroup
	sema := make(chan struct{}, 20)
	for _, peerNum := range peers {
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

// Disconnect closes the connections to the given node numbers (the per-slot subnet dials
// dropped at slot end).
func (n *Node) Disconnect(peers []int) {
	for _, peerNum := range peers {
		peerID, err := PeerIDFromNodeNum(peerNum)
		if err != nil {
			continue
		}
		if err := n.Host.Network().ClosePeer(peerID); err != nil {
			slog.Error("disconnect failed", "node", n.Num, "peer", peerNum, "err", err)
		}
	}
}

// Publish sends payload on the named (already-joined) topic.
func (n *Node) Publish(ctx context.Context, topic string, payload []byte) error {
	n.mu.Lock()
	t, ok := n.topics[topic]
	n.mu.Unlock()
	if !ok {
		return fmt.Errorf("publish: topic %q not joined", topic)
	}
	return t.Publish(ctx, payload)
}

// receive decodes each message off the subscription and hands it outward via
// OnReceive. It does not skip the node's own loopback publish — there is no
// clean origin in gossipsub, so that policy lives in the consumer.
func (n *Node) receive(ctx context.Context, sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(ctx)
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
	switch {
	case topic == validator.BlockTopic:
		blk := new(pb.Block)
		if err := proto.Unmarshal(data, blk); err != nil {
			return Received{}, err
		}
		return Received{Kind: KindBlock, Obj: blk, At: at}, nil
	case strings.HasPrefix(topic, validator.AttestationTopicPrefix):
		att := new(pb.Attestation)
		if err := proto.Unmarshal(data, att); err != nil {
			return Received{}, err
		}
		return Received{Kind: KindAttestation, Obj: att, At: at}, nil
	default:
		return Received{}, fmt.Errorf("unknown topic %q", topic)
	}
}
