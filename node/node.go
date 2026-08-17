// Package node is the passive half of the simulator: it owns the libp2p host,
// gossipsub, topic membership, peer connections, and send/receive. It responds
// to requests (Connect, JoinTopics, Join, Subscribe, Publish) and reports what
// it receives outward via OnReceive; it knows nothing about slots, duties, or
// timing — a Driver supplies the orchestration. It does own the per-kind message
// registry (registry.go): topic→kind dispatch, verify-queue routing, and the
// wire-identity policy whose publish-side twins are the metrics.*ID constructors.
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

	"github.com/ethp2p/slot-sim/validator"
)

// defaultBatchWindow is the attestation verifier's batch window when unset; it must
// be > 0 so the verifier's idle timer parks (a zero window busy-spins, §8.1).
const defaultBatchWindow = 50 * time.Millisecond

// Kind tags a received, decoded message so a Driver can dispatch on it without
// type-asserting to discover what it got. Values are predefined per message type.
type Kind int

const (
	KindBlock             Kind = 1
	KindAttestation       Kind = 2
	KindAggregate         Kind = 3
	KindColumn            Kind = 4
	KindSyncMessage       Kind = 5
	KindSyncContribution  Kind = 6
	KindACVote            Kind = 7
	KindFinalityVote      Kind = 8
	KindFinalityAggregate Kind = 9
	KindConsensusBlock    Kind = 10
	KindExecutionPayload  Kind = 11
	KindPTCVote           Kind = 12
)

// Received is the node's outward hand-off for one decoded message: the node
// infers the type from the gossipsub topic, unmarshals, and tags Obj with Kind.
// ID and Origin come from the registry's per-kind extractors, so a consumer can
// trace and loopback-skip without type-asserting Obj. Origin is the publishing
// node — distinct from ID.Origin, which is -1 for the aggregate-like kinds whose
// publisher rides ID.Attester instead (see Identity).
type Received struct {
	Kind   Kind
	Obj    any
	ID     Identity
	Origin int
	At     time.Time
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
	AttestBatchMax    int // max attestations per verify batch; 0 = uncapped

	// Column verify-hook (width-P semaphore): models the t=0 column burst as a P-server
	// per-core queue. ColVerifyService is the per-column cost; ColVerifyParallelism is P (16
	// for a full-custody node, 4 otherwise). Both unset ⇒ a 1-server, zero-cost verifier
	// (harmless when the column phase is off).
	ColVerifyService     func() time.Duration
	ColVerifyParallelism int

	// Partial switches the attestation-class floods (standard attestations + finality votes)
	// to the gossipsub partial-messages transport (partial-attestation-spec.md). Nil ⇒ classic.
	// Set before Start; the Resolver before JoinTopics.
	Partial *PartialOpts

	// RPCLogger, when non-nil, enables gossipsub's built-in debug RPC logger (every
	// RPC sent/received, with topics + data length) — a diagnostic for how many block
	// copies a node sources via mesh push vs gossip IWANT pull. Off by default.
	RPCLogger *slog.Logger

	ps          *pubsub.PubSub
	partial     *partialManager           // non-nil iff Partial is set (built at Start)
	verifiers   map[string]*batchVerifier // flood class → its single-server queue (lazy; see batchClass)
	colVerifier *columnVerifier

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
	opts := []pubsub.Option{
		pubsub.WithGossipSubParams(params),
		pubsub.WithMessageIdFn(MessageIDFunc),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		// 20k: per-peer, drop-on-overflow. At the burst instant a multi-key supernode
		// pushes its own fan-out burst PLUS the whole re-forwarded flood through each
		// peer queue; at 10k the n2000 run lost 55 votes at their publisher (every copy
		// of a vote hit a full queue — contiguous validator-id runs at big hosts). The
		// wire models bandwidth; queues should buffer, not silently drop.
		pubsub.WithPeerOutboundQueueSize(20000),
		// 20k: the inbound validate queue is ONE global queue per node, and aggregator
		// hosts subscribe several finality subnets at once — at 4096, k simultaneous
		// ~10k-vote bursts dropped 11% of finality attestations in the n500 run, hitting
		// the multi-subnet aggregator hosts hardest. 20k holds ~2 full subnet bursts.
		pubsub.WithValidateQueueSize(20000),
		// 50k: the throttle caps CONCURRENT validations and silently drops past it; our
		// validators sleep in the batch verifier, so a ~10k burst parks ~10k goroutines
		// there and every message popped beyond 8192 (the default) died — measured n500
		// loss was burst−throttle ≈ 1.1k per member per finality slot. Queueing must be
		// modeled by the verifier, not this cap, so keep it above any realistic backlog.
		pubsub.WithValidateThrottle(50000),
		pubsub.WithMaxMessageSize(maxMessageSize),
	}
	if n.RPCLogger != nil {
		opts = append(opts, pubsub.WithRPCLogger(n.RPCLogger))
	}
	if n.Partial != nil {
		n.partial = newPartialManager(n)
		opts = append(opts, pubsub.WithPartialMessagesExtension(n.partial.newExtension()))
	}
	ps, err := pubsub.NewGossipSub(ctx, n.Host, opts...)
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
	n.verifiers = make(map[string]*batchVerifier)
	n.rctx, n.cancel = context.WithCancel(ctx)

	colService := n.ColVerifyService
	if colService == nil {
		colService = func() time.Duration { return 0 }
	}
	n.colVerifier = newColumnVerifier(n.ColVerifyParallelism, colService)

	// The partial transport's tick loop, under the receive context so Close stops it.
	if n.partial != nil {
		n.wg.Go(func() { n.partial.run(n.rctx, n.ps) })
	}

	// The block-family global topics: Join + Subscribe with the fixed verify hook. All three
	// are always joined — the node is phase-ignorant, and an idle mesh carries no traffic
	// (legacy runs publish only the Block, ePBS runs only the consensus block + payload).
	for _, topic := range []string{
		validator.BlockTopic, validator.ConsensusBlockTopic, validator.ExecutionPayloadTopic,
	} {
		if err := n.Subscribe(topic); err != nil {
			return err
		}
	}
	return nil
}

// Join makes the node a publisher on topic without joining its mesh (fan-out
// publish). Idempotent; registers the topic's verify hook on first join.
func (n *Node) Join(topic string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.joinLocked(topic)
}

// joinLocked joins topic and registers its verify hook; caller holds n.mu. A partial-kind
// topic under the partial transport instead opts into partial messages and registers NO hook:
// no normal gossipsub messages flow there (verification enters from the extension's RPC
// handler), so a subscription's receive goroutine simply stays idle.
func (n *Node) joinLocked(topic string) error {
	if _, ok := n.topics[topic]; ok {
		return nil
	}
	var topicOpts []pubsub.TopicOpt
	if _, _, ok := partialKindFor(topic); ok && n.partial != nil {
		topicOpts = append(topicOpts, pubsub.RequestPartialMessages())
	} else if err := n.registerVerifyHook(topic); err != nil {
		return err
	}
	t, err := n.ps.Join(topic, topicOpts...)
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

// Unsubscribe leaves topic's mesh and stops receiving — the inverse of Subscribe (the
// finality aggregator's drop after its aggregate publishes). The topic stays joined, so
// the node can still fan-out publish on it. Idempotent; a never-subscribed topic is a no-op.
func (n *Node) Unsubscribe(topic string) {
	n.mu.Lock()
	sub, ok := n.subs[topic]
	delete(n.subs, topic)
	n.mu.Unlock()
	if ok {
		sub.Cancel() // the receive goroutine exits on the cancelled subscription
	}
}

// batchVerifierFor returns the batched verifier for a flood class, creating it (and starting its
// run loop) on first use, so a node spins up only the queues it actually joins. All classes use the
// same validation-as-sleep knobs. Caller holds n.mu.
func (n *Node) batchVerifierFor(class string) *batchVerifier {
	if v, ok := n.verifiers[class]; ok {
		return v
	}
	base := n.AttestVerifyDelay
	if base == nil {
		base = n.VerifyDelay
	}
	window := n.AttestBatchWindow
	if window <= 0 {
		window = defaultBatchWindow
	}
	v := newBatchVerifier(base, n.AttestPerItem, window, n.AttestBatchMax, slog.Default())
	go v.run()
	n.verifiers[class] = v
	return v
}

// verifierFor returns topic's class queue, creating it on first use — the locked twin of
// batchVerifierFor for callers outside n.mu (the partial manager's RPC handler).
func (n *Node) verifierFor(topic string) *batchVerifier {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.batchVerifierFor(string(verifyClassFor(topic)))
}

// registerVerifyHook registers topic's validation-as-sleep hook once, three-way by the
// topic's registry class: the column burst goes through the per-node P-server column
// verifier; a batched flood goes through its class's single-server queue; the block (and
// any unregistered topic) gets the fixed per-hop delay. Caller holds n.mu.
func (n *Node) registerVerifyHook(topic string) error {
	if n.validated[topic] {
		return nil
	}
	var hook pubsub.ValidatorEx
	switch class := verifyClassFor(topic); class {
	case vcColumn:
		hook = func(context.Context, peer.ID, *pubsub.Message) pubsub.ValidationResult {
			n.colVerifier.verify()
			return pubsub.ValidationAccept
		}
	case vcFixed:
		hook = func(context.Context, peer.ID, *pubsub.Message) pubsub.ValidationResult {
			time.Sleep(n.VerifyDelay())
			return pubsub.ValidationAccept
		}
	default:
		v := n.batchVerifierFor(string(class))
		hook = func(context.Context, peer.ID, *pubsub.Message) pubsub.ValidationResult {
			v.submitAndWait(verificationItem{Attestations: []any{nil}})
			return pubsub.ValidationAccept
		}
	}
	// Own publishes bypass the hook: a node never re-verifies a signature it just
	// produced, and gossipsub validates LOCAL messages too — without this skip,
	// Publish blocks one batch cycle per message and a multi-key host's burst
	// serializes (one finality attestation per window; the n100 37s tail).
	self, verify := n.Host.ID(), hook
	hook = func(ctx context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		if pid == self {
			return pubsub.ValidationAccept
		}
		return verify(ctx, pid, msg)
	}
	// Lift the per-topic concurrency cap (default 1024, drop-on-overflow, markSeen
	// already done ⇒ the drop is permanent — no gossip recovery). Validation-as-sleep
	// parks a whole subnet burst in the verifier, so in-flight counts reach the burst
	// size; the n500 run lost 15% of finality attestations at multi-subnet supernodes
	// to this cap. Like the global throttle, it must sit above any modeled backlog.
	if err := n.ps.RegisterTopicValidator(topic, hook,
		pubsub.WithValidatorConcurrency(50000)); err != nil {
		return err
	}
	n.validated[topic] = true
	return nil
}

// Close stops the receive loops and every batched verifier and waits for them to exit. A
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
	verifiers := make([]*batchVerifier, 0, len(n.verifiers))
	for _, v := range n.verifiers {
		verifiers = append(verifiers, v)
	}
	n.mu.Unlock()
	for _, v := range verifiers { // stop() blocks on drain, so release n.mu first
		v.stop()
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

// PublishLocalPartial stores one of this node's own votes for the partial transport,
// validated immediately; the next tick pushes it to topic's mesh. The member-duty publish.
func (n *Node) PublishLocalPartial(topic string, group, position int, sig, data []byte) {
	n.partial.publishLocal(topic, group, position, sig, data)
}

// FanoutPartial eagerly sends one batch of this node's votes to every current peer of topic —
// the non-member duty publish (no tick-loop membership for foreign topics). The topic must be
// joined and its subscribers dialed (the runner's warmup) so the fanout has somewhere to land.
func (n *Node) FanoutPartial(topic string, group int, positions []int, sigs [][]byte, data []byte) error {
	return n.partial.fanoutPublish(n.ps, topic, group, positions, sigs, data)
}

// SealPartial closes the serving window of the partial transport's kind-groups with key <=
// group: ticks stop pushing and advertising them (a late mesh joiner gets no backlog, matching
// classic's no-mcache-replay), but stragglers still verify and count until the prune. The
// runner calls it at the flood's semantic end (the finality aggregation deadline). A no-op
// when classic.
func (n *Node) SealPartial(kind Kind, group int) {
	if n.partial != nil {
		n.partial.seal(kind, group)
	}
}

// PrunePartial drops the partial transport's kind-groups with key <= group on every topic —
// the runner-driven GC, called one slot after the group ends; the pruned floor keeps stale
// stragglers from resurrecting them. A no-op when classic.
func (n *Node) PrunePartial(kind Kind, group int) {
	if n.partial != nil {
		n.partial.prune(kind, group)
	}
}

// LivePartialGroups counts the partial transport's live (topic, group) states — for tests
// asserting the bucket GC. 0 when classic.
func (n *Node) LivePartialGroups() int {
	if n.partial == nil {
		return 0
	}
	return n.partial.liveGroups()
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
		rec, err := Decode(msg.GetTopic(), msg.Data, time.Now())
		if err != nil {
			slog.Error("decode failed", "node", n.Num, "err", err)
			continue
		}
		if n.OnReceive != nil {
			n.OnReceive(rec)
		}
	}
}

// Decode resolves topic to its registry entry, unmarshals the payload, and hands back a
// Received tagged with the kind, identity, and publishing node.
func Decode(topic string, data []byte, at time.Time) (Received, error) {
	d := descriptorFor(topic)
	if d == nil {
		return Received{}, fmt.Errorf("unknown topic %q", topic)
	}
	obj, id, origin, err := d.decode(data)
	if err != nil {
		return Received{}, err
	}
	return Received{Kind: d.kind, Obj: obj, ID: id, Origin: origin, At: at}, nil
}
