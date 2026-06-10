package node

import (
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// stubResolver gives each kind a distinct committee width so per-kind sizing is observable:
// standard committees are stubSc wide; finality cells are stubFcBase+subnet wide. Identities
// are synthesized arithmetically so tests can predict them.
type stubResolver struct{}

const (
	stubSc     = 16
	stubFcBase = 4
)

func (stubResolver) CommitteeSize(kind Kind, subnet, group int) int {
	if kind == KindFinalityVote {
		return stubFcBase + subnet
	}
	return stubSc
}

func (stubResolver) Identity(kind Kind, subnet, group, position int) (val, origin int) {
	return 1000*subnet + position, position % 5
}

// newTestPartialManager builds a manager over a bare Node (no pubsub, no network) with explicit
// knobs — the unit seam: tests drive publishLocal / publishActions / the extension hooks
// directly with fake peer-state maps.
func newTestPartialManager(t *testing.T) *partialManager {
	t.Helper()
	n := &Node{
		Num: 1,
		D:   8,
		Partial: &PartialOpts{
			PublishInterval:        20 * time.Millisecond,
			MaxPeersPerAttestation: 16,
			MaxIWantPerPosition:    10,
			Resolver:               stubResolver{},
		},
		AttestVerifyDelay: func() time.Duration { return 5 * time.Millisecond },
		AttestBatchWindow: 2 * time.Millisecond,
	}
	n.verifiers = make(map[string]*batchVerifier)
	t.Cleanup(func() {
		for _, v := range n.verifiers {
			v.stop()
		}
	})
	return newPartialManager(n)
}

// attTopic is the standard-attestation partial topic the unit tests exercise.
var attTopic = validator.AttestationTopic(0)

// collected captures one peer's decoded PublishAction.
type collected struct {
	ctrl    *pb.ControlEnvelope
	payload *pb.BatchedAttestationEnvelope
}

// runPublishActions invokes the manager's publishActions iterator and decodes every action.
func runPublishActions(t *testing.T, m *partialManager, topic string, group int,
	peers map[peer.ID]partialPeerState, requestsPartial func(peer.ID) bool) map[peer.ID]collected {
	t.Helper()
	fn := m.publishActions(topic, group)
	out := map[peer.ID]collected{}
	for p, action := range fn(peers, requestsPartial) {
		var c collected
		if len(action.EncodedPartsMetadata) > 0 {
			c.ctrl = &pb.ControlEnvelope{}
			if err := proto.Unmarshal(action.EncodedPartsMetadata, c.ctrl); err != nil {
				t.Fatalf("unmarshal control: %v", err)
			}
		}
		if len(action.EncodedPartialMessage) > 0 {
			c.payload = &pb.BatchedAttestationEnvelope{}
			if err := proto.Unmarshal(action.EncodedPartialMessage, c.payload); err != nil {
				t.Fatalf("unmarshal data: %v", err)
			}
		}
		out[p] = c
	}
	return out
}

func acceptsPartial(peer.ID) bool  { return true }
func declinesPartial(peer.ID) bool { return false }

// makePeers returns n peer states "p0".."p<n-1>". Gossip peers are modeled as freshly selected
// by an EmitGossip heartbeat (primed to receive the Available list this tick).
func makePeers(n int, gossip bool) map[peer.ID]partialPeerState {
	peers := make(map[peer.ID]partialPeerState, n)
	for i := range n {
		peers[peer.ID(fmt.Sprintf("p%d", i))] = partialPeerState{gossipPeer: gossip, sendAvailableList: gossip}
	}
	return peers
}

func TestGroupKeyRoundtrip(t *testing.T) {
	for _, group := range []int{0, 1, 255, 256, 65535, 1 << 24} {
		if got := groupKeyFromID(groupKeyBytes(group)); got != group {
			t.Fatalf("groupKeyFromID(groupKeyBytes(%d)) = %d", group, got)
		}
	}
	// Short inputs zero-pad on the left.
	if got := groupKeyFromID([]byte{1}); got != 1 {
		t.Fatalf("groupKeyFromID([1]) = %d, want 1", got)
	}
	if got := groupKeyFromID([]byte{1, 2}); got != 0x0102 {
		t.Fatalf("groupKeyFromID([1 2]) = %d, want 0x0102", got)
	}
}

// Bitmap width is ceil(committeeSize/8), per kind: the stub's standard width is stubSc bits,
// its finality width stubFcBase+subnet bits — two kinds (and two finality subnets) coexist in
// one manager with independent widths.
func TestPerKindCommitteeWidth(t *testing.T) {
	m := newTestPartialManager(t)
	m.publishLocal(attTopic, 1, 0, []byte("s"), []byte("d"))
	m.publishLocal(validator.FinalityVoteTopic(0), 1, 0, []byte("s"), []byte("d"))
	m.publishLocal(validator.FinalityVoteTopic(5), 1, 0, []byte("s"), []byte("d"))

	for _, tc := range []struct {
		topic string
		size  int
	}{
		{attTopic, stubSc},
		{validator.FinalityVoteTopic(0), stubFcBase},
		{validator.FinalityVoteTopic(5), stubFcBase + 5},
	} {
		g := m.group(tc.topic, 1)
		if g == nil {
			t.Fatalf("no group state for %q", tc.topic)
		}
		if g.committeeSize != tc.size {
			t.Fatalf("%q committeeSize = %d, want %d", tc.topic, g.committeeSize, tc.size)
		}
		bps := g.buckets["d"].peer(peer.ID("p0"), g.committeeSize)
		if want := (tc.size + 7) / 8; len(bps.available) != want {
			t.Fatalf("%q bitmap = %d bytes, want %d", tc.topic, len(bps.available), want)
		}
	}
}

// publishLocal stores the node's own vote validated immediately (never re-verify your own
// signature); duplicates are no-ops; different attestation_data at one group key form
// independent fork buckets.
func TestPublishLocalValidatedImmediately(t *testing.T) {
	m := newTestPartialManager(t)
	m.publishLocal(attTopic, 5, 3, []byte("sig"), []byte("dataA"))

	b := m.group(attTopic, 5).buckets["dataA"]
	if _, ok := b.validated[3]; !ok {
		t.Fatal("own publish not validated immediately")
	}
	if _, ok := b.validating[3]; ok {
		t.Fatal("own publish parked in validating")
	}
	m.publishLocal(attTopic, 5, 3, []byte("sig-dup"), []byte("dataA"))
	if got := string(b.attestations[3].Signature); got != "sig" {
		t.Fatalf("duplicate publishLocal overwrote signature: %q", got)
	}

	m.publishLocal(attTopic, 5, 3, []byte("sig"), []byte("dataB"))
	g := m.group(attTopic, 5)
	if len(g.buckets) != 2 {
		t.Fatalf("fork buckets = %d, want 2 (independent per attestation_data)", len(g.buckets))
	}
	if _, ok := g.buckets["dataB"].validated[3]; !ok {
		t.Fatal("fork bucket B missing the validated position")
	}
}

// addReceived stores only genuinely-new positions (validating, not validated) and returns them.
func TestAddReceivedNewPositionsOnly(t *testing.T) {
	b := newPartialBucket([]byte("shared"))
	got := b.addReceived([]int{2, 5}, [][]byte{[]byte("s2"), []byte("s5")})
	if !slices.Equal(got, []int{2, 5}) {
		t.Fatalf("addReceived = %v, want [2 5]", got)
	}
	if _, ok := b.validating[2]; !ok {
		t.Fatal("received position not validating")
	}
	if _, ok := b.validated[2]; ok {
		t.Fatal("received position validated without the verifier")
	}
	if got := b.addReceived([]int{5, 8}, [][]byte{[]byte("dup"), []byte("s8")}); !slices.Equal(got, []int{8}) {
		t.Fatalf("overlapping addReceived = %v, want [8]", got)
	}
	if got := string(b.attestations[5].Signature); got != "s5" {
		t.Fatalf("redelivery overwrote signature: %q", got)
	}
}

// claimToSend: a mesh peer gets every validated position it lacks; per-peer available and the
// MaxPeersPerAttestation budget trim; a gossip peer gets only its pendingWant. Validating
// positions are never claimed.
func TestClaimToSend(t *testing.T) {
	m := newTestPartialManager(t)
	m.publishLocal(attTopic, 1, 0, []byte("s0"), []byte("d"))
	m.publishLocal(attTopic, 1, 1, []byte("s1"), []byte("d"))
	m.publishLocal(attTopic, 1, 2, []byte("s2"), []byte("d"))
	g := m.group(attTopic, 1)
	b := g.buckets["d"]
	b.addReceived([]int{7}, [][]byte{[]byte("s7")}) // validating: must never be claimed

	// Mesh peer: all validated, none validating.
	bps := b.peer(peer.ID("mesh"), g.committeeSize)
	if got := m.claimToSend(g, b, bps, false); !slices.Equal(got, []int{0, 1, 2}) {
		t.Fatalf("mesh claim = %v, want [0 1 2]", got)
	}
	if !bps.available.Get(1) || b.sendCount[1] != 1 {
		t.Fatal("claim must mark per-peer available and bump sendCount")
	}
	// Re-claim: everything already available to this peer.
	if got := m.claimToSend(g, b, bps, false); got != nil {
		t.Fatalf("re-claim = %v, want nil", got)
	}

	// Gossip peer with no want: nothing. With a want: exactly the wanted position.
	gps := b.peer(peer.ID("gossip"), g.committeeSize)
	if got := m.claimToSend(g, b, gps, true); got != nil {
		t.Fatalf("gossip claim without want = %v, want nil", got)
	}
	gps.pendingWant.Set(1)
	if got := m.claimToSend(g, b, gps, true); !slices.Equal(got, []int{1}) {
		t.Fatalf("gossip claim = %v, want [1]", got)
	}

	// Budget: a position already forwarded MaxPeersPerAttestation times is not claimed again.
	m.maxPeers = 1
	fresh := b.peer(peer.ID("late"), g.committeeSize)
	if got := m.claimToSend(g, b, fresh, false); got != nil {
		t.Fatalf("over-budget claim = %v, want nil (sendCount at cap)", got)
	}
}

// selectIWantTargets caps requests per missing position, skips positions we already hold,
// ignores mesh peers, and returns sorted per-peer lists.
func TestSelectIWantTargets(t *testing.T) {
	m := newTestPartialManager(t)
	m.maxIWant = 10
	m.publishLocal(attTopic, 1, 0, []byte("s"), []byte("d"))
	g := m.group(attTopic, 1)
	b := g.buckets["d"]

	// 13 gossip peers all advertising position 5 (and one of them positions 8,1,4): the cap
	// binds the total requests for 5 at maxIWant.
	peers := map[peer.ID]partialPeerState{}
	for i := range m.maxIWant + 3 {
		id := peer.ID(fmt.Sprintf("p%d", i))
		bps := b.peer(id, g.committeeSize)
		bps.available.Set(5)
		bps.available.Set(0) // held: never requested
		peers[id] = partialPeerState{gossipPeer: true}
	}
	rich := b.peer(peer.ID("p0"), g.committeeSize)
	rich.available.Set(8)
	rich.available.Set(1)
	rich.available.Set(4)

	wants := m.selectIWantTargets(g, b, peers)
	count5 := 0
	for _, positions := range wants {
		if !slices.IsSorted(positions) {
			t.Fatalf("want list %v not sorted", positions)
		}
		if slices.Contains(positions, 0) {
			t.Fatal("requested a position we already hold")
		}
		if slices.Contains(positions, 5) {
			count5++
		}
	}
	if count5 != m.maxIWant {
		t.Fatalf("position 5 requested from %d peers, want %d (the cap)", count5, m.maxIWant)
	}
	if b.requestCount[5] != m.maxIWant {
		t.Fatalf("requestCount[5] = %d, want %d", b.requestCount[5], m.maxIWant)
	}

	// Mesh peers advertise nothing — they push eagerly; no IWANTs to them.
	meshOnly := map[peer.ID]partialPeerState{peer.ID("p0"): {gossipPeer: false}}
	if got := m.selectIWantTargets(g, b, meshOnly); len(got) != 0 {
		t.Fatalf("IWANTs to mesh peer: %v", got)
	}
}

// A tick to a mesh peer carries data only (no metadata); both fork buckets ride one envelope.
func TestPublishActionsMeshPeer(t *testing.T) {
	m := newTestPartialManager(t)
	m.publishLocal(attTopic, 1, 0, []byte("s"), []byte("forkA"))
	m.publishLocal(attTopic, 1, 1, []byte("s"), []byte("forkB"))

	out := runPublishActions(t, m, attTopic, 1, makePeers(1, false), acceptsPartial)
	c := out[peer.ID("p0")]
	if c.payload == nil || len(c.payload.Batches) != 2 {
		t.Fatalf("mesh payload = %+v, want both fork batches", c.payload)
	}
	if c.ctrl != nil {
		t.Fatal("mesh peer received metadata")
	}
	// A peer that didn't request partial messages gets nothing.
	if out := runPublishActions(t, m, attTopic, 99, makePeers(1, false), acceptsPartial); len(out) != 0 {
		t.Fatalf("tick for an absent group emitted %d actions", len(out))
	}
	m2 := newTestPartialManager(t)
	m2.publishLocal(attTopic, 1, 0, []byte("s"), []byte("d"))
	if out := runPublishActions(t, m2, attTopic, 1, makePeers(1, false), declinesPartial); len(out) != 0 {
		t.Fatal("peer without partial support received data")
	}
}

// A gossip peer gets Available once per heartbeat (the sendAvailableList flag), is dropped from
// peerStates after being served, and is re-added by the next EmitGossip; its pendingWant is
// cleared after every tick even when unsatisfied (requests are non-persistent).
func TestPublishActionsGossipPeerCadence(t *testing.T) {
	m := newTestPartialManager(t)
	m.publishLocal(attTopic, 1, 0, []byte("s"), []byte("d"))
	g := m.group(attTopic, 1)
	b := g.buckets["d"]

	peers := makePeers(1, true)
	out := runPublishActions(t, m, attTopic, 1, peers, declinesPartial)
	c := out[peer.ID("p0")]
	if c.ctrl == nil || len(c.ctrl.Metadatas) != 1 {
		t.Fatalf("gossip peer ctrl = %+v, want one metadata", c.ctrl)
	}
	md := c.ctrl.Metadatas[0]
	if !bitmap.Bitmap(md.Available).Get(0) {
		t.Fatal("metadata must advertise the validated position")
	}
	if len(md.Requests) != 0 {
		t.Fatal("nothing missing ⇒ no requests")
	}
	if c.payload != nil {
		t.Fatal("no pendingWant ⇒ no data to a gossip peer")
	}
	if _, tracked := peers[peer.ID("p0")]; tracked {
		t.Fatal("gossip peer must be dropped from peerStates after serving")
	}

	// Second tick with the same (now-empty) map: nothing emitted.
	if out := runPublishActions(t, m, attTopic, 1, peers, declinesPartial); len(out) != 0 {
		t.Fatal("dropped gossip peer served again without a heartbeat")
	}

	// The heartbeat re-adds it and re-arms the Available advertisement.
	m.onEmitGossip(attTopic, groupKeyBytes(1), []peer.ID{peer.ID("p0")}, peers)
	out = runPublishActions(t, m, attTopic, 1, peers, declinesPartial)
	if c := out[peer.ID("p0")]; c.ctrl == nil || len(c.ctrl.Metadatas[0].Available) == 0 {
		t.Fatal("re-gossiped peer must receive Available again")
	}

	// pendingWant is non-persistent: an unsatisfiable want is cleared by the next tick.
	bps := b.peer(peer.ID("p0"), g.committeeSize)
	bps.pendingWant.Set(9) // we don't hold 9
	peers[peer.ID("p0")] = partialPeerState{gossipPeer: true}
	runPublishActions(t, m, attTopic, 1, peers, declinesPartial)
	if bps.pendingWant.OnesCount() != 0 {
		t.Fatal("pendingWant must be cleared after one tick even when unsatisfied")
	}
}

// A gossip peer with a satisfiable pendingWant receives exactly the wanted data.
func TestPublishActionsGossipWantServed(t *testing.T) {
	m := newTestPartialManager(t)
	m.publishLocal(attTopic, 1, 0, []byte("s"), []byte("d"))
	g := m.group(attTopic, 1)
	b := g.buckets["d"]
	b.peer(peer.ID("p0"), g.committeeSize).pendingWant.Set(0)

	peers := map[peer.ID]partialPeerState{peer.ID("p0"): {gossipPeer: true}}
	out := runPublishActions(t, m, attTopic, 1, peers, acceptsPartial)
	c := out[peer.ID("p0")]
	if c.payload == nil || len(c.payload.Batches) != 1 ||
		!slices.Equal(c.payload.Batches[0].AttestorIndices, []uint32{0}) {
		t.Fatalf("gossip want payload = %+v, want position 0", c.payload)
	}
}

// Validating (unverified) positions are never advertised nor sent — only validated ones flow.
func TestValidatingPositionsHeld(t *testing.T) {
	m := newTestPartialManager(t)
	m.publishLocal(attTopic, 1, 0, []byte("s"), []byte("d"))
	g := m.group(attTopic, 1)
	b := g.buckets["d"]
	b.addReceived([]int{3}, [][]byte{[]byte("s3")}) // validating

	peers := makePeers(1, true)
	out := runPublishActions(t, m, attTopic, 1, peers, acceptsPartial)
	md := out[peer.ID("p0")].ctrl.Metadatas[0]
	if bitmap.Bitmap(md.Available).Get(3) {
		t.Fatal("validating position advertised")
	}
	mesh := makePeers(1, false)
	out = runPublishActions(t, m, attTopic, 1, mesh, acceptsPartial)
	for _, batch := range out[peer.ID("p0")].payload.Batches {
		if slices.Contains(batch.AttestorIndices, uint32(3)) {
			t.Fatal("validating position sent")
		}
	}
}

// onEmitGossip is the metadata gossip's on-switch; DisableMetadataGossip turns the whole
// gossip layer off (the no-gossip variant).
func TestOnEmitGossipDisableFlag(t *testing.T) {
	m := newTestPartialManager(t)
	m.disableGossip = true
	peers := map[peer.ID]partialPeerState{peer.ID("p0"): {}}
	m.onEmitGossip(attTopic, groupKeyBytes(1), []peer.ID{peer.ID("p0")}, peers)
	if peers[peer.ID("p0")].gossipPeer {
		t.Fatal("DisableMetadataGossip must suppress gossip-peer marking")
	}
}

// PrunePartial is the runner-driven GC: it drops exactly the named (kind, group)'s state on
// every topic of the kind, leaving other groups and the other kind untouched.
func TestPrunePartialClearsGroups(t *testing.T) {
	m := newTestPartialManager(t)
	m.node.partial = m
	m.publishLocal(attTopic, 1, 0, []byte("s"), []byte("d"))
	m.publishLocal(validator.AttestationTopic(2), 1, 0, []byte("s"), []byte("d"))
	m.publishLocal(attTopic, 2, 0, []byte("s"), []byte("d"))
	m.publishLocal(validator.FinalityVoteTopic(0), 1, 0, []byte("s"), []byte("d"))
	if got := m.node.LivePartialGroups(); got != 4 {
		t.Fatalf("live groups = %d, want 4", got)
	}
	m.node.PrunePartial(KindAttestation, 1) // both attestation topics' group 1
	if got := m.node.LivePartialGroups(); got != 2 {
		t.Fatalf("live groups after prune = %d, want 2", got)
	}
	if m.group(attTopic, 2) == nil || m.group(validator.FinalityVoteTopic(0), 1) == nil {
		t.Fatal("prune dropped an unrelated group")
	}
	m.node.PrunePartial(KindAttestation, 2)
	m.node.PrunePartial(KindFinalityVote, 1)
	if got := m.node.LivePartialGroups(); got != 0 {
		t.Fatalf("live groups after full prune = %d, want 0", got)
	}
	m.node.PrunePartial(KindAttestation, 9) // absent group: no-op
}

// The tick loop's initial jitter is seeded by (seed, node num) — identical inputs give the
// identical offset, the cross-run determinism guard (the prior art used a bare rand).
func TestPartialJitterSeeded(t *testing.T) {
	a := partialJitter(7, 3, 20*time.Millisecond)
	if b := partialJitter(7, 3, 20*time.Millisecond); a != b {
		t.Fatalf("same (seed, num) gave different jitter: %v vs %v", a, b)
	}
	if a < 0 || a >= 20*time.Millisecond {
		t.Fatalf("jitter %v outside [0, interval)", a)
	}
	if b := partialJitter(7, 4, 20*time.Millisecond); a == b {
		t.Fatal("different nodes share the same jitter (suspicious for a 20ms range)")
	}
}

// deliver hands one manager's tick output for `to` over to the receiving manager as an
// incoming RPC — the wire conversation, without gossipsub.
func deliver(t *testing.T, from *partialManager, fromID peer.ID, topic string, group int,
	fromPeers map[peer.ID]partialPeerState, to *partialManager, toPeers map[peer.ID]partialPeerState,
	toID peer.ID) bool {
	t.Helper()
	sent := false
	for p, action := range from.publishActions(topic, group)(fromPeers, acceptsPartial) {
		if p != toID {
			continue
		}
		rpc := &pubsub_pb.PartialMessagesExtension{TopicID: &topic, GroupID: groupKeyBytes(group)}
		rpc.PartsMetadata = action.EncodedPartsMetadata
		rpc.PartialMessage = action.EncodedPartialMessage
		if err := to.onIncomingRPC(fromID, toPeers, rpc); err != nil {
			t.Fatalf("deliver: %v", err)
		}
		sent = true
	}
	return sent
}

// The metadata-gossip conversation end to end (the partial topics' only non-mesh recovery —
// they never enter mcache): A's heartbeat advertises Available to gossip peer B; B's next tick
// answers with a Request; A's next tick serves the data; B verifies and fires the arrival.
// With DisableMetadataGossip nothing is ever advertised.
func TestPartialGossipConversation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mA, mB := newTestPartialManager(t), newTestPartialManager(t)
		idA, idB := peer.ID("A"), peer.ID("B")
		sink := &recvSink{}
		mB.node.OnReceive = sink.add

		data := []byte("d")
		mA.publishLocal(attTopic, 1, 4, []byte("sig4"), data)
		// B must know the group exists to tick for it (gossipsub's EmitGossip would create it;
		// here B has heard A's metadata below, which creates it on receipt).

		// Heartbeat at A: B becomes a gossip peer, primed for one Available advertisement.
		peersA := map[peer.ID]partialPeerState{idB: {}}
		mA.onEmitGossip(attTopic, groupKeyBytes(1), []peer.ID{idB}, peersA)

		// Tick A → B: metadata only (B asked for nothing yet).
		peersB := map[peer.ID]partialPeerState{}
		if !deliver(t, mA, idA, attTopic, 1, peersA, mB, peersB, idB) {
			t.Fatal("A's tick sent nothing to B")
		}
		if mB.group(attTopic, 1) == nil {
			t.Fatal("A's metadata did not create B's group state")
		}

		// Tick B → A: a Request for position 4 (B advertises nothing — it holds nothing).
		peersB[idA] = partialPeerState{gossipPeer: true}
		if !deliver(t, mB, idB, attTopic, 1, peersB, mA, peersA, idA) {
			t.Fatal("B's tick sent no request to A")
		}

		// Tick A → B: the data batch B asked for; B's verifier promotes it.
		if !deliver(t, mA, idA, attTopic, 1, peersA, mB, peersB, idB) {
			t.Fatal("A's tick did not serve B's request")
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		recs := sink.all()
		if len(recs) != 1 || recs[0].ID.Attester != 4 {
			t.Fatalf("B's arrivals = %+v, want exactly position 4", recs)
		}

		// The no-gossip variant: the heartbeat is inert, so nothing is ever advertised.
		mC := newTestPartialManager(t)
		mC.disableGossip = true
		mC.publishLocal(attTopic, 1, 4, []byte("sig4"), data)
		peersC := map[peer.ID]partialPeerState{idB: {}}
		mC.onEmitGossip(attTopic, groupKeyBytes(1), []peer.ID{idB}, peersC)
		out := runPublishActions(t, mC, attTopic, 1, peersC, declinesPartial)
		if len(out) != 0 {
			t.Fatalf("no-gossip variant still emitted %d actions", len(out))
		}
	})
}

// A sealed group (the flood's window closed — e.g. the finality aggregation deadline) serves
// and advertises nothing — a late mesh joiner gets no backlog push, classic's no-mcache-replay
// behavior — but still accepts and counts incoming stragglers until the prune.
func TestSealPartialStopsServingKeepsCounting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestPartialManager(t)
		m.node.partial = m
		sink := &recvSink{}
		m.node.OnReceive = sink.add

		m.publishLocal(attTopic, 1, 0, []byte("s"), []byte("d"))
		m.node.SealPartial(KindAttestation, 1)

		// No actions to anyone: neither data to a fresh mesh peer nor metadata to gossip peers.
		if out := runPublishActions(t, m, attTopic, 1, makePeers(2, false), acceptsPartial); len(out) != 0 {
			t.Fatalf("sealed group pushed %d actions to mesh peers", len(out))
		}
		if out := runPublishActions(t, m, attTopic, 1, makePeers(2, true), acceptsPartial); len(out) != 0 {
			t.Fatalf("sealed group gossiped %d actions", len(out))
		}

		// An incoming straggler still verifies and counts.
		batch := &pb.BatchedAttestation{AttestationData: []byte("d"),
			AttestorIndices: []uint32{3}, Signatures: [][]byte{[]byte("s3")}}
		peers := map[peer.ID]partialPeerState{peer.ID("p1"): {}}
		if err := m.onIncomingRPC(peer.ID("p1"), peers, rpcWith(t, attTopic, 1, batch)); err != nil {
			t.Fatalf("straggler into sealed group: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if recs := sink.all(); len(recs) != 1 || recs[0].ID.Attester != 3 {
			t.Fatalf("straggler arrivals = %+v, want exactly position 3", recs)
		}

		// The prune still ends the group for good (and floors stale recreations).
		m.node.PrunePartial(KindAttestation, 1)
		if got := m.node.LivePartialGroups(); got != 0 {
			t.Fatalf("live groups after prune = %d, want 0", got)
		}
		if err := m.onIncomingRPC(peer.ID("p1"), peers, rpcWith(t, attTopic, 1, batch)); err != nil {
			t.Fatalf("stale RPC after prune: %v", err)
		}
		if got := m.node.LivePartialGroups(); got != 0 {
			t.Fatal("stale RPC resurrected a pruned group")
		}
	})
}
