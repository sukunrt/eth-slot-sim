package node_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// attSink records, per receiving node, the attestations it got (keyed by identity).
type attSink struct {
	mu   sync.Mutex
	recv map[attKey][]int // (subnet,val,origin) → receiving nodes
}

type attKey struct{ subnet, val, origin int }

func newAttSink() *attSink { return &attSink{recv: map[attKey][]int{}} }

func (s *attSink) on(nodeNum int) func(node.Received) {
	return func(r node.Received) {
		if r.Kind != node.KindAttestation {
			return
		}
		a := r.Obj.(*pb.Attestation)
		s.mu.Lock()
		k := attKey{int(a.Subnet), int(a.Val), int(a.Origin)}
		s.recv[k] = append(s.recv[k], nodeNum)
		s.mu.Unlock()
	}
}

func (s *attSink) receivers(k attKey) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]int(nil), s.recv[k]...)
	slicesSortInts(out)
	return out
}

// M2 / §8.2: a publisher that is NOT a subscriber of subnet S still reaches every
// subscriber (fan-out), and a non-subscriber receives nothing — got == want both ways.
// The subnet-aware graph guarantees the publisher connects to S's subscribers (the
// generator "plays discv5"); reachability must come from construction, not chance.
func TestSubnetFanOutReachesSubscribersOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// subnet 0's backbone subscribers = {1,2}; node 0 attests subnet 0 (publisher,
		// not a subscriber); node 3 is uninvolved in subnet 0.
		a := &committee.Assignment{
			Params:   committee.Params{N: 4, V: 4, C: 1, Sc: 1, SubnetCount: 64, BackbonePerNode: 1, NumSlots: 1},
			Backbone: [][]int{{10}, {0}, {0}, {11}},
			Slots: []committee.SlotPlan{{
				Slot:        0,
				Committees:  [][]committee.AttesterRef{{{Node: 0, Val: 0, Subnet: 0, Position: 0}}},
				SubnetOf:    []int{0},
				Aggregators: [][]committee.AttesterRef{{}},
				Subscribers: [][]int{{1, 2}},
			}},
		}
		nw, err := netsim.NewWithCommittee(a, netsim.Config{
			N: 4, P: 2, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("netsim: %v", err)
		}
		t.Cleanup(nw.Close)

		sink := newAttSink()
		nodes := make([]*node.Node, 4)
		for i := range nodes {
			nodes[i] = &node.Node{
				Num: i, Host: nw.Host(i), Network: nw,
				VerifyDelay:       func() time.Duration { return 0 },
				AttestVerifyDelay: func() time.Duration { return 0 },
				AttestBatchWindow: 10 * time.Millisecond,
				D:                 8, Dlo: 6, Dhi: 12,
				OnReceive: sink.on(i),
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
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
		defer func() {
			for _, nd := range nodes {
				nd.Close()
			}
		}()

		topic := validator.AttestationTopic(0)
		mustDo(t, nodes[1].Subscribe(topic)) // subscribers join the mesh
		mustDo(t, nodes[2].Subscribe(topic))
		mustDo(t, nodes[0].Join(topic)) // publisher fans out without subscribing
		time.Sleep(2 * time.Second)  // let subscriptions propagate + mesh form

		msg := validator.MakeAttestation(0, 0, 0, 0, -1) // slot0 subnet0 val0 origin0, prior vote
		if err := nodes[0].Publish(ctx, topic, msg.Payload); err != nil {
			t.Fatalf("publish: %v", err)
		}
		time.Sleep(time.Second) // let the attestation disseminate (drain window)
		synctest.Wait()

		// MUST receive (exactly): {1,2}. MUST NOT: {0 (loopback), 3 (non-subscriber)}.
		got := sink.receivers(attKey{subnet: 0, val: 0, origin: 0})
		want := []int{1, 2}
		if len(got) != len(want) {
			t.Fatalf("receivers = %v, want %v (fan-out reaches subscribers only)", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("receivers = %v, want %v", got, want)
			}
		}
	})
}

func mustDo(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func slicesSortInts(s []int) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
