package node

import (
	"encoding/binary"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
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
			if g == nil || len(g.buckets) == 0 {
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
