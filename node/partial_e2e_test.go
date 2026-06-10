package node_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/validator"
)

// e2eResolver: every committee is `size` wide; position p is validator p hosted on node p —
// so a synthesized arrival's Origin names the publishing node directly.
type e2eResolver struct{ size int }

func (r e2eResolver) CommitteeSize(kind node.Kind, subnet, group int) int { return r.size }
func (r e2eResolver) Identity(kind node.Kind, subnet, group, position int) (int, int) {
	return position, position
}

// partialSink records, per (group, val), the receiving nodes of synthesized partial arrivals.
type partialSink struct {
	mu   sync.Mutex
	recv map[[2]int][]int // (group, val) → receiving nodes
}

func newPartialSink() *partialSink { return &partialSink{recv: map[[2]int][]int{}} }

func (s *partialSink) on(nodeNum int) func(node.Received) {
	return func(r node.Received) {
		if r.Kind != node.KindAttestation && r.Kind != node.KindFinalityVote {
			return
		}
		s.mu.Lock()
		k := [2]int{r.ID.Slot, r.ID.Attester}
		s.recv[k] = append(s.recv[k], nodeNum)
		s.mu.Unlock()
	}
}

func (s *partialSink) receivers(group, val int) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]int(nil), s.recv[[2]int{group, val}]...)
	slices.Sort(out)
	return out
}

// buildPartialNodes builds n started nodes with the partial transport on (committee width
// `size`) and the sink attached. The caller dials/joins/subscribes per scenario.
func buildPartialNodes(t *testing.T, ctx context.Context, nw *netsim.Netsim, n, size int,
	sink *partialSink, disableGossip bool, d int) []*node.Node {
	t.Helper()
	nodes := make([]*node.Node, n)
	for i := range nodes {
		nodes[i] = &node.Node{
			Num: i, Host: nw.Host(i), Network: nw,
			VerifyDelay:       func() time.Duration { return 0 },
			AttestVerifyDelay: func() time.Duration { return 0 },
			AttestBatchWindow: 10 * time.Millisecond,
			D:                 d, Dlo: d, Dhi: d,
			OnReceive:         sink.on(i),
			Partial: &node.PartialOpts{
				Seed:                  1,
				DisableMetadataGossip: disableGossip,
				Resolver:              e2eResolver{size: size},
			},
		}
		if err := nodes[i].Start(ctx); err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.Close()
		}
	})
	return nodes
}

// Two mesh members exchange their own votes through the tick loop: each receives exactly the
// other's position, with the resolver identity, and never its own (no loopback).
func TestPartialMeshExchange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{N: 2, P: 1, Seed: 1,
			MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		sink := newPartialSink()
		nodes := buildPartialNodes(t, ctx, nw, 2, 8, sink, false, 8)
		time.Sleep(time.Second)
		nodes[0].ConnectToPeers([]int{1})
		for _, nd := range nodes {
			if err := nd.JoinTopics(ctx); err != nil {
				t.Fatalf("join %d: %v", nd.Num, err)
			}
		}
		topic := validator.AttestationTopic(0)
		for _, nd := range nodes {
			mustDo(t, nd.Subscribe(topic))
		}
		time.Sleep(2 * time.Second) // mesh forms

		data := validator.MakePartialAttData(3, 0, 0, validator.PartialAttDataSize)
		sig := validator.MakePartialSignature(validator.PartialSignatureSize)
		nodes[0].PublishLocalPartial(topic, 3, 0, sig, data)
		nodes[1].PublishLocalPartial(topic, 3, 1, sig, data)
		time.Sleep(time.Second)
		synctest.Wait()

		if got := sink.receivers(3, 0); !slices.Equal(got, []int{1}) {
			t.Fatalf("position 0 receivers = %v, want [1]", got)
		}
		if got := sink.receivers(3, 1); !slices.Equal(got, []int{0}) {
			t.Fatalf("position 1 receivers = %v, want [0]", got)
		}
	})
}

// The non-member duty publish: a Join-only publisher that dialed two subscribers reaches every
// subscriber via one eager FanoutPartial batch (relayed over the subnet mesh), and an
// uninvolved node receives nothing — got == want both ways, the leakage check.
func TestPartialFanoutReachesSubscribersOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{N: 4, P: 2, Seed: 1,
			MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		sink := newPartialSink()
		nodes := buildPartialNodes(t, ctx, nw, 4, 8, sink, false, 8)
		time.Sleep(time.Second)
		for _, nd := range nodes {
			nd.ConnectToPeers(nw.Peers(nd.Num))
		}
		for _, nd := range nodes {
			if err := nd.JoinTopics(ctx); err != nil {
				t.Fatalf("join %d: %v", nd.Num, err)
			}
		}
		topic := validator.AttestationTopic(0)
		mustDo(t, nodes[1].Subscribe(topic))
		mustDo(t, nodes[2].Subscribe(topic))
		mustDo(t, nodes[0].Join(topic)) // publisher: fan-out only
		nodes[0].Dial([]int{1, 2})      // the runner's dial-2 warmup
		time.Sleep(2 * time.Second)

		// Two duties on the foreign subnet ride ONE eager batch.
		data := validator.MakePartialAttData(0, 0, 0, validator.PartialAttDataSize)
		sig := validator.MakePartialSignature(validator.PartialSignatureSize)
		if err := nodes[0].FanoutPartial(topic, 0, []int{2, 5},
			[][]byte{sig, sig}, data); err != nil {
			t.Fatalf("fanout: %v", err)
		}
		time.Sleep(time.Second)
		synctest.Wait()

		for _, pos := range []int{2, 5} {
			if got := sink.receivers(0, pos); !slices.Equal(got, []int{1, 2}) {
				t.Fatalf("position %d receivers = %v, want [1 2]", pos, got)
			}
		}
	})
}

// Fork buckets coexist end-to-end: two publishers voting differently (two attestation_data
// byte strings) both reach the subscriber — no cross-bucket dedup.
func TestPartialForkBucketsCoexist(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.New(netsim.Config{N: 3, P: 2, Seed: 1,
			MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		sink := newPartialSink()
		nodes := buildPartialNodes(t, ctx, nw, 3, 8, sink, false, 8)
		time.Sleep(time.Second)
		for _, nd := range nodes {
			nd.ConnectToPeers(nw.Peers(nd.Num))
		}
		for _, nd := range nodes {
			if err := nd.JoinTopics(ctx); err != nil {
				t.Fatalf("join %d: %v", nd.Num, err)
			}
		}
		topic := validator.AttestationTopic(0)
		for _, nd := range nodes {
			mustDo(t, nd.Subscribe(topic))
		}
		time.Sleep(2 * time.Second)

		sig := validator.MakePartialSignature(validator.PartialSignatureSize)
		blockVote := validator.MakePartialAttData(0, 0, 7, validator.PartialAttDataSize)
		priorVote := validator.MakePartialAttData(0, 0, -1, validator.PartialAttDataSize)
		nodes[0].PublishLocalPartial(topic, 0, 0, sig, blockVote)
		nodes[1].PublishLocalPartial(topic, 0, 1, sig, priorVote)
		time.Sleep(time.Second)
		synctest.Wait()

		if got := sink.receivers(0, 0); !slices.Equal(got, []int{1, 2}) {
			t.Fatalf("block-vote receivers = %v, want [1 2]", got)
		}
		if got := sink.receivers(0, 1); !slices.Equal(got, []int{0, 2}) {
			t.Fatalf("prior-vote receivers = %v, want [0 2]", got)
		}
	})
}
