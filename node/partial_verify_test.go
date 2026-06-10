package node

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// rpcWith builds the extension RPC carrying the given batches (and no metadata).
func rpcWith(t *testing.T, topic string, group int, batches ...*pb.BatchedAttestation) *pubsub_pb.PartialMessagesExtension {
	t.Helper()
	env := &pb.BatchedAttestationEnvelope{Batches: batches}
	enc, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &pubsub_pb.PartialMessagesExtension{
		TopicID:        &topic,
		GroupID:        groupKeyBytes(group),
		PartialMessage: enc,
	}
}

// recvSink collects OnReceive events thread-safely.
type recvSink struct {
	mu   sync.Mutex
	recs []Received
}

func (s *recvSink) add(r Received) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs = append(s.recs, r)
}

func (s *recvSink) all() []Received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Received(nil), s.recs...)
}

// A malformed batch — indices/signatures length mismatch or a position outside the committee
// index space — is rejected with an error and stores nothing.
func TestOnIncomingRPCRejectsMalformed(t *testing.T) {
	m := newTestPartialManager(t)
	peers := map[peer.ID]partialPeerState{peer.ID("p1"): {}}

	for name, batch := range map[string]*pb.BatchedAttestation{
		"length mismatch": {AttestationData: []byte("d"),
			AttestorIndices: []uint32{1, 2}, Signatures: [][]byte{[]byte("s1")}},
		"position past committee size": {AttestationData: []byte("d"),
			AttestorIndices: []uint32{stubSc}, Signatures: [][]byte{[]byte("s")}},
	} {
		if err := m.onIncomingRPC(peer.ID("p1"), peers, rpcWith(t, attTopic, 1, batch)); err == nil {
			t.Fatalf("%s: onIncomingRPC = nil, want error", name)
		}
		if g := m.group(attTopic, 1); g != nil && len(g.buckets) != 0 {
			t.Fatalf("%s: malformed batch stored a bucket", name)
		}
	}
	// A non-partial topic and a garbled envelope are errors too.
	if err := m.onIncomingRPC(peer.ID("p1"), peers, rpcWith(t, validator.BlockTopic, 1,
		&pb.BatchedAttestation{AttestationData: []byte("d")})); err == nil {
		t.Fatal("non-partial topic accepted")
	}
	topic := attTopic
	garbled := &pubsub_pb.PartialMessagesExtension{
		TopicID: &topic, GroupID: groupKeyBytes(1), PartsMetadata: []byte{0xFF, 0xFF, 0xFF}}
	if err := m.onIncomingRPC(peer.ID("p1"), peers, garbled); err == nil {
		t.Fatal("garbled metadata accepted")
	}
}

// A received batch enters the kind's existing class queue as ONE item whose length is the
// new-position count — a 100-vote batch costs base + 100·perItem once — and the verifier
// callback promotes validating → validated and fires OnReceive exactly once per position with
// the resolver's identity, at the promotion instant.
func TestOnIncomingRPCVerifiesAndSynthesizesArrivals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestPartialManager(t)
		m.node.AttestVerifyDelay = func() time.Duration { return 10 * time.Millisecond }
		m.node.AttestPerItem = time.Millisecond
		sink := &recvSink{}
		m.node.OnReceive = sink.add

		const n = 10
		positions := make([]uint32, n)
		sigs := make([][]byte, n)
		for i := range n {
			positions[i] = uint32(i)
			sigs[i] = []byte("s")
		}
		batch := &pb.BatchedAttestation{
			AttestationData: []byte("d"), AttestorIndices: positions, Signatures: sigs}
		peers := map[peer.ID]partialPeerState{peer.ID("p1"): {}}
		start := time.Now()
		if err := m.onIncomingRPC(peer.ID("p1"), peers, rpcWith(t, attTopic, 3, batch)); err != nil {
			t.Fatalf("onIncomingRPC: %v", err)
		}

		// Just before base + n·perItem: still validating, no arrivals.
		time.Sleep(19 * time.Millisecond)
		synctest.Wait()
		if got := len(sink.all()); got != 0 {
			t.Fatalf("%d arrivals before the verification sleep elapsed", got)
		}
		time.Sleep(2 * time.Millisecond) // past 20ms
		synctest.Wait()

		recs := sink.all()
		if len(recs) != n {
			t.Fatalf("arrivals = %d, want %d (exactly once per position)", len(recs), n)
		}
		seen := map[int]bool{}
		for _, r := range recs {
			pos := r.ID.Attester // stub: val = 1000*subnet + position; subnet 0 ⇒ val == position
			wantVal, wantOrigin := pos, pos%5
			if r.Kind != KindAttestation || r.ID.Slot != 3 || r.ID.Subnet != 0 ||
				r.ID.Attester != wantVal || r.ID.Origin != wantOrigin || r.Origin != wantOrigin {
				t.Fatalf("arrival %+v inconsistent with the stub resolver", r)
			}
			if r.Obj != nil {
				t.Fatalf("arrival Obj = %v, want nil", r.Obj)
			}
			if got := time.Since(start) - time.Since(r.At); got != 20*time.Millisecond {
				t.Fatalf("arrival at +%v, want +20ms (base 10ms + 10·1ms)", got)
			}
			if seen[pos] {
				t.Fatalf("position %d arrived twice", pos)
			}
			seen[pos] = true
		}

		// The bucket promoted: validated, nothing validating; only validated positions flow.
		b := m.group(attTopic, 3).buckets["d"]
		m.mu.Lock()
		validated, validating := len(b.validated), len(b.validating)
		m.mu.Unlock()
		if validated != n || validating != 0 {
			t.Fatalf("bucket validated=%d validating=%d, want %d/0", validated, validating, n)
		}

		// Redelivery of the same positions: no new submit, no second arrival.
		if err := m.onIncomingRPC(peer.ID("p2"), peers, rpcWith(t, attTopic, 3, batch)); err != nil {
			t.Fatalf("redelivery: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if got := len(sink.all()); got != n {
			t.Fatalf("arrivals after redelivery = %d, want %d", got, n)
		}
	})
}

// Each partial kind routes through its own class queue (vcConsensus for standard attestations,
// vcFCVote for finality votes) — the queues must not contend.
func TestOnIncomingRPCRoutesPerClassQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestPartialManager(t)
		batch := &pb.BatchedAttestation{AttestationData: []byte("d"),
			AttestorIndices: []uint32{0}, Signatures: [][]byte{[]byte("s")}}
		peers := map[peer.ID]partialPeerState{peer.ID("p1"): {}}
		if err := m.onIncomingRPC(peer.ID("p1"), peers, rpcWith(t, attTopic, 1, batch)); err != nil {
			t.Fatalf("attestation rpc: %v", err)
		}
		if err := m.onIncomingRPC(peer.ID("p1"), peers,
			rpcWith(t, validator.FinalityVoteTopic(1), 1, batch)); err != nil {
			t.Fatalf("finality rpc: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		m.node.mu.Lock()
		_, consensus := m.node.verifiers[string(vcConsensus)]
		_, fcvote := m.node.verifiers[string(vcFCVote)]
		m.node.mu.Unlock()
		if !consensus || !fcvote {
			t.Fatalf("verifier queues consensus=%v fcvote=%v, want both", consensus, fcvote)
		}
	})
}

// publishLocal is the own-publish bypass: no verifier submission, no arrival event.
func TestPublishLocalNoVerifierNoArrival(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestPartialManager(t)
		sink := &recvSink{}
		m.node.OnReceive = sink.add

		m.publishLocal(attTopic, 1, 4, []byte("s"), []byte("d"))
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		if got := len(sink.all()); got != 0 {
			t.Fatalf("own publish produced %d arrivals, want 0", got)
		}
		m.node.mu.Lock()
		queues := len(m.node.verifiers)
		m.node.mu.Unlock()
		if queues != 0 {
			t.Fatalf("own publish spun up %d verifier queues, want 0", queues)
		}
	})
}

// A peer's metadata feeds its per-bucket state: Available ORs in, Requests arm pendingWant,
// and the sender is marked (re-added) as a gossip peer so its want is served next tick.
func TestOnIncomingRPCMetadata(t *testing.T) {
	m := newTestPartialManager(t)
	avail := newCommitteeBitmap(stubSc)
	avail.Set(1)
	avail.Set(2)
	req := newCommitteeBitmap(stubSc)
	req.Set(3)
	ctrl := &pb.ControlEnvelope{Metadatas: []*pb.PartsMetadata{
		{Slot: 1, AttestationData: []byte("d"), Available: avail, Requests: req},
	}}
	enc, err := proto.Marshal(ctrl)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	topic := attTopic
	rpc := &pubsub_pb.PartialMessagesExtension{
		TopicID: &topic, GroupID: groupKeyBytes(1), PartsMetadata: enc}
	peers := map[peer.ID]partialPeerState{}
	if err := m.onIncomingRPC(peer.ID("p1"), peers, rpc); err != nil {
		t.Fatalf("onIncomingRPC: %v", err)
	}

	if !peers[peer.ID("p1")].gossipPeer {
		t.Fatal("metadata sender not marked as a gossip peer")
	}
	g := m.group(attTopic, 1)
	bps := g.buckets["d"].peer(peer.ID("p1"), g.committeeSize)
	if !bps.available.Get(1) || !bps.available.Get(2) || !bps.pendingWant.Get(3) {
		t.Fatalf("peer state available/pendingWant not updated: %v %v", bps.available, bps.pendingWant)
	}
}

// A received batch infers the sender's available bits (it holds what it sent), including for
// positions we already had.
func TestOnIncomingRPCInfersAvailable(t *testing.T) {
	m := newTestPartialManager(t)
	m.publishLocal(attTopic, 1, 4, []byte("s"), []byte("d"))
	batch := &pb.BatchedAttestation{AttestationData: []byte("d"),
		AttestorIndices: []uint32{4, 7}, Signatures: [][]byte{[]byte("s4"), []byte("s7")}}
	peers := map[peer.ID]partialPeerState{peer.ID("p1"): {}}
	if err := m.onIncomingRPC(peer.ID("p1"), peers, rpcWith(t, attTopic, 1, batch)); err != nil {
		t.Fatalf("onIncomingRPC: %v", err)
	}
	g := m.group(attTopic, 1)
	bps := g.buckets["d"].peer(peer.ID("p1"), g.committeeSize)
	if !bps.available.Get(4) || !bps.available.Get(7) {
		t.Fatal("sender's available not inferred from the batch")
	}
}
