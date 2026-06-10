package node

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
)

// This file is the partial-message transport for the attestation-class floods
// (partial-attestation-spec.md), a port of batched-attestation-sim's partialAttestationManager
// generalized to this repo's per-kind registry: one manager per node owns every partial-kind
// topic, with the committee width and identity resolution supplied per (kind, subnet, group)
// by the driver-injected resolver. The unit is one committee member's vote, identified on the
// wire by its position; votes sharing attestation_data batch into one BatchedAttestation. Mesh
// peers get eager data pushes every publish tick; gossip (non-mesh) peers get bitmap metadata
// at heartbeat cadence and non-persistent IWANTs back. Partial messages never enter mcache, so
// this metadata layer IS their gossip recovery.

// PartialResolver supplies the schedule-derived facts the manager needs, injected by the
// driver so node/ stays schedule-free. CommitteeSize is the position-space width (bitmap
// width) of one (kind, subnet, group); Identity recovers the metrics identity of one position
// — the validator and its hosting node (the publisher origin).
type PartialResolver interface {
	CommitteeSize(kind Kind, subnet, group int) int
	Identity(kind Kind, subnet, group, position int) (val, origin int)
}

// PartialOpts switches the node's attestation-class floods to the partial-message transport
// (nil Node.Partial ⇒ classic). Set before Start.
type PartialOpts struct {
	PublishInterval        time.Duration // tick period; 0 ⇒ 20ms
	MaxPeersPerAttestation int           // per-position forward cap; 0 ⇒ 2·D
	MaxIWantPerPosition    int           // per-position request cap across gossip peers; 0 ⇒ 10
	DisableMetadataGossip  bool          // no-gossip variant: mesh push only
	Seed                   uint64        // with Node.Num, seeds the tick loop's initial jitter
	Resolver               PartialResolver
}

// partialEntry is one held vote: a committee position and its signature (the shared
// attestation_data lives on the bucket).
type partialEntry struct {
	Position  int
	Signature []byte
}

// bucketPeerState is what one peer has and wants, scoped to one bucket. available bits are
// positions the peer holds (advertised, or inferred from what it sent us); pendingWant bits
// are positions it just requested — cleared on the next tick to the peer whether or not we
// satisfied them (requests are non-persistent per spec).
type bucketPeerState struct {
	available   bitmap.Bitmap
	pendingWant bitmap.Bitmap
}

// partialBucket is the per-(topic, group, attestation_data) state. Forks at one group key get
// independent buckets — nodes MUST NOT dedupe by (group, position) across buckets.
type partialBucket struct {
	data         []byte
	attestations map[int]*partialEntry
	validating   map[int]struct{} // accepted into the verifier, not yet forwardable
	validated    map[int]struct{} // verifier-promoted (or our own) — the only ones that flow
	sendCount    map[int]int      // per-position forward count (the MaxPeersPerAttestation cap)
	requestCount map[int]int      // per-position IWANT count (the MaxIWantPerPosition cap)
	peers        map[peer.ID]*bucketPeerState
}

func newPartialBucket(data []byte) *partialBucket {
	return &partialBucket{
		data:         slices.Clone(data),
		attestations: make(map[int]*partialEntry),
		validating:   make(map[int]struct{}),
		validated:    make(map[int]struct{}),
		sendCount:    make(map[int]int),
		requestCount: make(map[int]int),
		peers:        make(map[peer.ID]*bucketPeerState),
	}
}

// peer returns (creating as needed) the per-bucket state for p; bitmaps are fixed-width over
// the bucket's committee index space. Caller holds the manager lock.
func (b *partialBucket) peer(p peer.ID, committeeSize int) *bucketPeerState {
	s, ok := b.peers[p]
	if !ok {
		s = &bucketPeerState{
			available:   newCommitteeBitmap(committeeSize),
			pendingWant: newCommitteeBitmap(committeeSize),
		}
		b.peers[p] = s
	}
	return s
}

// addReceived stores positions received from a peer as validating and returns the
// genuinely-new ones (redeliveries — including of validated positions — are dropped, never
// overwritten). Caller holds the manager lock.
func (b *partialBucket) addReceived(positions []int, signatures [][]byte) []int {
	var fresh []int
	for i, pos := range positions {
		if _, ok := b.attestations[pos]; ok {
			continue
		}
		b.attestations[pos] = &partialEntry{Position: pos, Signature: signatures[i]}
		b.validating[pos] = struct{}{}
		fresh = append(fresh, pos)
	}
	return fresh
}

// partialGroup holds one (topic, group key)'s buckets plus the per-kind scalars resolved at
// creation: the kind, the topic's subnet, and the committee width (fixed for the group — every
// bucket's bitmaps share it).
type partialGroup struct {
	kind          Kind
	subnet        int
	committeeSize int
	buckets       map[string]*partialBucket // string(attestation_data) → bucket
	// sealed: the flood's window closed (the runner's aggregation deadline). A sealed group
	// serves and advertises nothing — a late mesh joiner gets no backlog push, matching
	// classic's no-mcache-replay behavior — but still accepts and counts stragglers until the
	// prune.
	sealed bool
}

// bucket returns (creating as needed) the bucket for data. Caller holds the manager lock.
func (g *partialGroup) bucket(data []byte) *partialBucket {
	key := string(data)
	b, ok := g.buckets[key]
	if !ok {
		b = newPartialBucket(data)
		g.buckets[key] = b
	}
	return b
}

// partialPeerState is the extension's per-peer state, spanning all buckets of a (topic,
// group). gossipPeer marks a peer acting non-mesh (EmitGossip selected it, or it sent
// metadata); sendAvailableList arms a once-per-heartbeat Available advertisement — set only by
// onEmitGossip, so Available re-advertisement runs at heartbeat cadence, not every tick.
type partialPeerState struct {
	gossipPeer        bool
	sendAvailableList bool
}

// partialManager is the application half of the extension: all partial-kind state of one node.
type partialManager struct {
	node          *Node
	logger        *slog.Logger
	interval      time.Duration
	maxPeers      int
	maxIWant      int
	disableGossip bool

	mu     sync.Mutex
	groups map[string]map[int]*partialGroup // topic → group key → state
	// floor is the per-kind pruned watermark: group keys are time-ordered (slots / round
	// keys), so a pruned group must STAY pruned — a straggler RPC in flight across the prune
	// instant must not resurrect a bucket nothing will ever GC again.
	floor map[Kind]int
}

// newPartialManager resolves the option defaults (20ms tick, 2·D forward cap, 10-request cap)
// against n.Partial, which must be set.
func newPartialManager(n *Node) *partialManager {
	o := n.Partial
	if o.Resolver == nil {
		panic("partial transport: PartialOpts.Resolver must be set")
	}
	m := &partialManager{
		node:          n,
		logger:        slog.With("node", n.Num, "component", "partial"),
		interval:      o.PublishInterval,
		maxPeers:      o.MaxPeersPerAttestation,
		maxIWant:      o.MaxIWantPerPosition,
		disableGossip: o.DisableMetadataGossip,
		groups:        make(map[string]map[int]*partialGroup),
		floor:         make(map[Kind]int),
	}
	if m.interval <= 0 {
		m.interval = 20 * time.Millisecond
	}
	if m.maxPeers <= 0 {
		m.maxPeers = 2 * n.D
	}
	if m.maxIWant <= 0 {
		m.maxIWant = 10
	}
	return m
}

// group returns the (topic, group key) state, nil if absent. Caller holds m.mu or is a test.
func (m *partialManager) group(topic string, group int) *partialGroup {
	return m.groups[topic][group]
}

// getOrCreateGroup resolves topic to its partial kind and committee width on first use. It
// errors on a non-partial topic or an empty cell (width <= 0) — remote input must not create
// state the resolver can't size.
func (m *partialManager) getOrCreateGroup(topic string, group int) (*partialGroup, error) {
	byGroup, ok := m.groups[topic]
	if !ok {
		byGroup = make(map[int]*partialGroup)
		m.groups[topic] = byGroup
	}
	if g, ok := byGroup[group]; ok {
		return g, nil
	}
	kind, subnet, ok := partialKindFor(topic)
	if !ok {
		return nil, fmt.Errorf("topic %q is not a partial kind", topic)
	}
	size := m.node.Partial.Resolver.CommitteeSize(kind, subnet, group)
	if size <= 0 {
		return nil, fmt.Errorf("topic %q group %d: committee size %d", topic, group, size)
	}
	g := &partialGroup{kind: kind, subnet: subnet, committeeSize: size,
		buckets: make(map[string]*partialBucket)}
	byGroup[group] = g
	return g, nil
}

// publishLocal stores one of the node's own votes, validated immediately (a node never
// re-verifies a signature it just produced — classic's own-publish bypass); the next tick
// pushes it to mesh peers. Duplicate positions are no-ops.
func (m *partialManager) publishLocal(topic string, group, position int, sig, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, err := m.getOrCreateGroup(topic, group)
	if err != nil {
		panic("publishLocal: " + err.Error()) // our own emit path — a bad topic is a bug
	}
	b := g.bucket(data)
	if _, ok := b.attestations[position]; ok {
		return
	}
	b.attestations[position] = &partialEntry{Position: position, Signature: sig}
	b.validated[position] = struct{}{}
}

// claimToSend returns the sorted positions to send a peer for one bucket and commits the
// bookkeeping (per-peer available, sendCount). Mesh peers get every validated position they
// lack under the forward cap; gossip peers only what they asked for. Caller holds m.mu.
func (m *partialManager) claimToSend(g *partialGroup, b *partialBucket,
	bps *bucketPeerState, gossipPeer bool) []int {
	if gossipPeer && bps.pendingWant.OnesCount() <= 0 {
		return nil
	}
	var claimed []int
	for pos := range b.validated {
		if b.sendCount[pos] >= m.maxPeers {
			continue
		}
		if bps.available.Get(pos) {
			continue
		}
		if gossipPeer && !bps.pendingWant.Get(pos) {
			continue
		}
		claimed = append(claimed, pos)
	}
	slices.Sort(claimed)
	for _, pos := range claimed {
		bps.available.Set(pos)
		b.sendCount[pos]++
	}
	return claimed
}

// selectIWantTargets picks, for each missing position some gossip peer advertises, up to
// (maxIWant − already-asked) peers to request it from, bumping requestCount. Caller holds m.mu.
func (m *partialManager) selectIWantTargets(g *partialGroup, b *partialBucket,
	peerStates map[peer.ID]partialPeerState) map[peer.ID][]int {
	wantPerPeer := make(map[peer.ID][]int)
	for p, ps := range peerStates {
		if !ps.gossipPeer {
			continue
		}
		bps := b.peer(p, g.committeeSize)
		for pos := range g.committeeSize {
			if !bps.available.Get(pos) {
				continue
			}
			if _, have := b.attestations[pos]; have {
				continue
			}
			if b.requestCount[pos] >= m.maxIWant {
				continue
			}
			b.requestCount[pos]++
			wantPerPeer[p] = append(wantPerPeer[p], pos)
		}
	}
	for p := range wantPerPeer {
		slices.Sort(wantPerPeer[p])
	}
	return wantPerPeer
}

// bucketMetadata assembles one bucket's PartsMetadata: Available (validated positions) only
// when the heartbeat armed it, Requests (wantList) always. Nil when both would be empty.
func bucketMetadata(g *partialGroup, b *partialBucket, group int,
	wantList []int, includeAvailable bool) *pb.PartsMetadata {
	md := &pb.PartsMetadata{Slot: uint32(group), AttestationData: b.data}
	if includeAvailable && len(b.validated) > 0 {
		avail := newCommitteeBitmap(g.committeeSize)
		for pos := range b.validated {
			avail.Set(pos)
		}
		md.Available = []byte(avail)
	}
	if len(wantList) > 0 {
		req := newCommitteeBitmap(g.committeeSize)
		for _, pos := range wantList {
			req.Set(pos)
		}
		md.Requests = []byte(req)
	}
	if len(md.Available) == 0 && len(md.Requests) == 0 {
		return nil
	}
	return md
}

// encodeBatch builds one bucket's BatchedAttestation for positions (which must all be held),
// indices and signatures in the given order.
func encodeBatch(b *partialBucket, positions []int) *pb.BatchedAttestation {
	idxs := make([]uint32, 0, len(positions))
	sigs := make([][]byte, 0, len(positions))
	for _, pos := range positions {
		idxs = append(idxs, uint32(pos))
		sigs = append(sigs, b.attestations[pos].Signature)
	}
	return &pb.BatchedAttestation{AttestationData: b.data, AttestorIndices: idxs, Signatures: sigs}
}

// publishActions is one (topic, group) tick: per peer, one data envelope (mesh: everything
// validated and unsent under the cap; gossip: only their pending wants) and — gossip peers
// only — one control envelope (Available gated to the heartbeat flag, Requests from this
// tick's IWANT selection). pendingWant clears unconditionally; a served gossip peer is dropped
// from peerStates so it's re-gossiped only by the next heartbeat (an incoming want re-adds it
// sooner), while mesh peers are re-added every tick by the extension.
func (m *partialManager) publishActions(topic string, group int) partialmessages.PublishActionsFn[partialPeerState] {
	return func(peerStates map[peer.ID]partialPeerState,
		peerRequestsPartial func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
			m.mu.Lock()
			defer m.mu.Unlock()

			g := m.group(topic, group)
			if g == nil || g.sealed || len(g.buckets) == 0 {
				return
			}

			// Select this tick's IWANTs per bucket (bumps requestCount) before serving peers.
			wantPerPeerPerData := make(map[peer.ID]map[string][]int)
			for key, b := range g.buckets {
				for p, positions := range m.selectIWantTargets(g, b, peerStates) {
					if wantPerPeerPerData[p] == nil {
						wantPerPeerPerData[p] = make(map[string][]int)
					}
					wantPerPeerPerData[p][key] = positions
				}
			}

			for p, ps := range peerStates {
				ctrl := &pb.ControlEnvelope{}
				data := &pb.BatchedAttestationEnvelope{}

				for key, b := range g.buckets {
					bps := b.peer(p, g.committeeSize)
					if peerRequestsPartial(p) {
						if positions := m.claimToSend(g, b, bps, ps.gossipPeer); len(positions) > 0 {
							data.Batches = append(data.Batches, encodeBatch(b, positions))
						}
					}
					// Requests are non-persistent: clear even what we didn't satisfy.
					bps.pendingWant = newCommitteeBitmap(g.committeeSize)
					if ps.gossipPeer {
						if md := bucketMetadata(g, b, group, wantPerPeerPerData[p][key], ps.sendAvailableList); md != nil {
							ctrl.Metadatas = append(ctrl.Metadatas, md)
						}
					}
				}

				// Served this heartbeat round — the next EmitGossip re-adds it.
				if ps.gossipPeer {
					delete(peerStates, p)
				}

				var action partialmessages.PublishAction
				if len(ctrl.Metadatas) > 0 {
					enc, err := proto.Marshal(ctrl)
					if err != nil {
						m.logger.Error("marshal control envelope", "err", err)
					} else {
						action.EncodedPartsMetadata = enc
					}
				}
				if len(data.Batches) > 0 {
					enc, err := proto.Marshal(data)
					if err != nil {
						m.logger.Error("marshal data envelope", "err", err)
					} else {
						action.EncodedPartialMessage = enc
					}
				}
				if action.EncodedPartsMetadata == nil && action.EncodedPartialMessage == nil {
					continue
				}
				if !yield(p, action) {
					return
				}
			}
		}
	}
}

// onIncomingRPC (extension hook) is the partial topics' entire receive path — no normal
// gossipsub messages flow there, so no topic validator parks a goroutine per message.
// Metadata first (so available/pendingWant is current before batches infer from them), then
// data batches: malformed input errors out; genuinely-new positions park as validating and
// enter the kind's class queue as ONE item whose Attestations length is the new-position count
// — a 1000-vote batch costs base + 1000·perItem once, the batching win priced honestly in the
// same M/D/1 model. The verifier callback (markValidated) promotes and fires the arrival.
func (m *partialManager) onIncomingRPC(from peer.ID, peerStates map[peer.ID]partialPeerState,
	rpc *pubsub_pb.PartialMessagesExtension) error {
	topic := rpc.GetTopicID()
	group := groupKeyFromID(rpc.GroupID)
	kind, _, ok := partialKindFor(topic)
	if !ok {
		return fmt.Errorf("topic %q is not a partial kind", topic)
	}

	var ctrl pb.ControlEnvelope
	if len(rpc.PartsMetadata) > 0 {
		if err := proto.Unmarshal(rpc.PartsMetadata, &ctrl); err != nil {
			return fmt.Errorf("unmarshal control envelope: %w", err)
		}
	}
	var dataEnv pb.BatchedAttestationEnvelope
	if len(rpc.PartialMessage) > 0 {
		if err := proto.Unmarshal(rpc.PartialMessage, &dataEnv); err != nil {
			return fmt.Errorf("unmarshal data envelope: %w", err)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// A straggler for a pruned (reaped) group: not a protocol error, just stale — drop it
	// rather than resurrect a bucket nothing will GC again.
	if fl, ok := m.floor[kind]; ok && group <= fl {
		return nil
	}

	for _, md := range ctrl.Metadatas {
		g, err := m.getOrCreateGroup(topic, group)
		if err != nil {
			return err
		}
		b := g.bucket(md.AttestationData)
		bps := b.peer(from, g.committeeSize)
		bps.available.Or(md.Available) // Or truncates to our fixed width — remote can't grow it
		bps.pendingWant.Or(md.Requests)
		// A peer issuing metadata is by definition a gossip peer; marking it here also re-adds
		// it to peerStates after a publish tick dropped it, so its want is served next tick.
		if bitmap.Bitmap(md.Available).OnesCount() > 0 || bitmap.Bitmap(md.Requests).OnesCount() > 0 {
			ps := peerStates[from]
			ps.gossipPeer = true
			peerStates[from] = ps
		}
	}

	for _, batch := range dataEnv.Batches {
		g, err := m.getOrCreateGroup(topic, group)
		if err != nil {
			return err
		}
		// Validate the whole batch before storing anything.
		if len(batch.AttestorIndices) != len(batch.Signatures) {
			return fmt.Errorf("attestor_indices=%d != signatures=%d",
				len(batch.AttestorIndices), len(batch.Signatures))
		}
		positions := make([]int, len(batch.AttestorIndices))
		for i, p := range batch.AttestorIndices {
			if int(p) >= g.committeeSize {
				return fmt.Errorf("attestor index %d >= committee size %d", p, g.committeeSize)
			}
			positions[i] = int(p)
		}

		b := g.bucket(batch.AttestationData)
		bps := b.peer(from, g.committeeSize)
		fresh := b.addReceived(positions, batch.Signatures)
		for _, pos := range positions { // the sender holds what it sent
			bps.available.Set(pos)
		}
		if len(fresh) > 0 {
			data := b.data
			m.node.verifierFor(topic).submit(
				verificationItem{Slot: group, Topic: topic, Attestations: make([]any, len(fresh))},
				func(verificationItem) { m.markValidated(topic, group, data, fresh) })
		}
	}
	return nil
}

// markValidated (the verifier callback) promotes positions validating → validated and fires
// one synthesized arrival per position — post-validation, the same moment classic's
// validator-gated delivery implies — with the identity the resolver recovers from
// (kind, subnet, group, position). OnReceive runs outside m.mu (the sink takes its own locks
// and calls back into the Node).
func (m *partialManager) markValidated(topic string, group int, data []byte, positions []int) {
	at := time.Now()
	m.mu.Lock()
	var promoted []int
	g := m.group(topic, group)
	if g != nil {
		if b, ok := g.buckets[string(data)]; ok {
			for _, pos := range positions {
				if _, ok := b.validating[pos]; !ok {
					continue
				}
				delete(b.validating, pos)
				b.validated[pos] = struct{}{}
				promoted = append(promoted, pos)
			}
		}
	}
	m.mu.Unlock()

	if m.node.OnReceive == nil || g == nil {
		return
	}
	for _, pos := range promoted {
		val, origin := m.node.Partial.Resolver.Identity(g.kind, g.subnet, group, pos)
		m.node.OnReceive(Received{
			Kind:   g.kind,
			ID:     Identity{Slot: group, Subnet: g.subnet, Attester: val, Origin: origin},
			Origin: origin,
			At:     at,
		})
	}
}

// onEmitGossip (extension hook, heartbeat cadence): mark the selected non-mesh peers as gossip
// peers and arm their once-per-heartbeat Available advertisement. The disable knob turns the
// whole metadata layer off (the no-gossip variant).
func (m *partialManager) onEmitGossip(topic string, groupID []byte,
	gossipPeers []peer.ID, peerStates map[peer.ID]partialPeerState) {
	if m.disableGossip {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range gossipPeers {
		ps := peerStates[p]
		ps.gossipPeer = true
		ps.sendAvailableList = true
		peerStates[p] = ps
	}
}

// newExtension wires the manager as the gossipsub extension (pass to
// pubsub.WithPartialMessagesExtension at Start). GroupTTL 10 heartbeats — the pubsub-side
// state self-expires; the manager's own buckets are pruned by the runner (PrunePartial).
func (m *partialManager) newExtension() *partialmessages.PartialMessagesExtension[partialPeerState] {
	return &partialmessages.PartialMessagesExtension[partialPeerState]{
		Logger:             m.logger,
		OnEmitGossip:       m.onEmitGossip,
		OnIncomingRPC:      m.onIncomingRPC,
		GroupTTLByHeatbeat: 10,
	}
}

// run is the tick loop: every PublishInterval, one PublishPartial per live (topic, group).
// The initial offset is seeded (partialJitter) so staggering is reproducible across runs and
// backends — the prior art's bare rand would break synctest determinism. Started by JoinTopics
// under the node's receive context; Close cancels it.
func (m *partialManager) run(ctx context.Context, ps *pubsub.PubSub) {
	time.Sleep(partialJitter(m.node.Partial.Seed, m.node.Num, m.interval))
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		m.tick(ps)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// tick publishes every live (topic, group) once, in sorted order (map iteration order must
// not leak into the send schedule — the determinism guard).
func (m *partialManager) tick(ps *pubsub.PubSub) {
	type liveGroup struct {
		topic string
		group int
	}
	m.mu.Lock()
	var live []liveGroup
	for topic, byGroup := range m.groups {
		for group := range byGroup {
			live = append(live, liveGroup{topic, group})
		}
	}
	m.mu.Unlock()
	slices.SortFunc(live, func(a, b liveGroup) int {
		if c := strings.Compare(a.topic, b.topic); c != 0 {
			return c
		}
		return a.group - b.group
	})
	for _, lg := range live {
		err := pubsub.PublishPartial(ps, lg.topic, groupKeyBytes(lg.group),
			m.publishActions(lg.topic, lg.group))
		if err != nil {
			m.logger.Error("publish partial", "topic", lg.topic, "group", lg.group, "err", err)
		}
	}
}

// fanoutPublish eagerly sends one batch to every current peer of topic — the non-member duty
// publish. The extension's MeshPeers falls back to the fanout peer set, which the runner's
// Join + dial-2 warmup feeds; eager-once, no manager state (a fanout publisher is not a member
// and will not receive, so there is nothing to tick).
func (m *partialManager) fanoutPublish(ps *pubsub.PubSub, topic string, group int,
	positions []int, sigs [][]byte, data []byte) error {
	idxs := make([]uint32, len(positions))
	for i, pos := range positions {
		idxs[i] = uint32(pos)
	}
	env := &pb.BatchedAttestationEnvelope{Batches: []*pb.BatchedAttestation{
		{AttestationData: data, AttestorIndices: idxs, Signatures: sigs},
	}}
	enc, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal fanout envelope: %w", err)
	}
	actions := func(peerStates map[peer.ID]partialPeerState,
		_ func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
			for p := range peerStates {
				if !yield(p, partialmessages.PublishAction{EncodedPartialMessage: enc}) {
					return
				}
			}
		}
	}
	return pubsub.PublishPartial(ps, topic, groupKeyBytes(group), actions)
}

// seal marks every topic's kind-groups with key <= group as sealed (monotone, like prune, over
// the time-ordered keys). Receiving continues; serving stops.
func (m *partialManager) seal(kind Kind, group int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, byGroup := range m.groups {
		for key, g := range byGroup {
			if g.kind == kind && key <= group {
				g.sealed = true
			}
		}
	}
}

// prune drops every topic's kind-groups with key <= group and raises the kind's pruned floor —
// the runner-driven GC (called a grace slot behind the group's end, so late stragglers still
// count instead of re-creating buckets; anything later than that is dropped by the floor).
func (m *partialManager) prune(kind Kind, group int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fl, ok := m.floor[kind]; !ok || group > fl {
		m.floor[kind] = group
	}
	for topic, byGroup := range m.groups {
		for key, g := range byGroup {
			if g.kind == kind && key <= group {
				delete(byGroup, key)
			}
		}
		if len(byGroup) == 0 {
			delete(m.groups, topic)
		}
	}
}

// liveGroups counts (topic, group) states — a test accessor (bucket GC assertions).
func (m *partialManager) liveGroups() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, byGroup := range m.groups {
		n += len(byGroup)
	}
	return n
}

// partialJitter is the tick loop's seeded initial offset in [0, interval).
func partialJitter(seed uint64, num int, interval time.Duration) time.Duration {
	rng := rand.New(rand.NewPCG(seed, uint64(num)))
	return time.Duration(rng.Int64N(int64(interval)))
}

// groupKeyBytes encodes a group key (slot / round key) as the extension's groupID.
func groupKeyBytes(group int) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(group))
}

// groupKeyFromID decodes a groupID back to its group key, zero-padding short inputs.
func groupKeyFromID(groupID []byte) int {
	var buf [4]byte
	if len(groupID) > 4 {
		groupID = groupID[len(groupID)-4:]
	}
	copy(buf[4-len(groupID):], groupID)
	return int(binary.BigEndian.Uint32(buf[:]))
}

// newCommitteeBitmap returns a zero bitmap over committeeSize positions.
func newCommitteeBitmap(committeeSize int) bitmap.Bitmap {
	return make(bitmap.Bitmap, (committeeSize+7)/8)
}
