package driver_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/validator"
)

// scenario builds N nodes + NodeRunners over a committee assignment by hand (not via
// the Driver), so a test can override individual nodes — in particular suppress block
// delivery to chosen nodes (the test-only OnReceive filter the coupling tests need).
type scenario struct {
	a       *committee.Assignment
	nw      *netsim.Netsim
	nodes   []*node.Node
	runners []*driver.NodeRunner
	slotDur time.Duration
}

// buildScenario wires the nodes against tracer. suppressBlock[i] drops block receipts to
// node i's runner (it still relays via gossipsub) so node i never processes the block —
// forcing its attestation to the prior-head path. due is the attestation deadline offset.
func buildScenario(t *testing.T, a *committee.Assignment, due time.Duration, suppressBlock map[int]bool, tracer metrics.Tracer) *scenario {
	t.Helper()
	n := a.Params.N
	const slotDur = 12 * time.Second
	nw, err := netsim.NewWithCommittee(a, netsim.Config{
		N: n, P: n - 1, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("netsim: %v", err)
	}
	t.Cleanup(nw.Close)

	s := &scenario{a: a, nw: nw, slotDur: slotDur}
	for i := range n {
		val := validator.New(i, n, 1024, 0, 0, rand.New(rand.NewPCG(1, uint64(i))))
		nd := &node.Node{
			Num: i, Host: nw.Host(i), Network: nw,
			VerifyDelay:       func() time.Duration { return 0 },
			AttestVerifyDelay: func() time.Duration { return 0 },
			AttestBatchWindow: 10 * time.Millisecond,
			D:                 8, Dlo: 6, Dhi: 12,
		}
		r := driver.NewRunner(i, nd, val, a, tracer, slotDur, due, 0)
		r.Attach()
		if suppressBlock[i] {
			orig := nd.OnReceive
			nd.OnReceive = func(rec node.Received) {
				if rec.Kind == node.KindBlock {
					return // hold the block from this node past the deadline
				}
				orig(rec)
			}
		}
		s.nodes = append(s.nodes, nd)
		s.runners = append(s.runners, r)
	}
	return s
}

// run brings the fleet up and runs numSlots from runStart, returning once the run plus
// a drain window completes.
func (s *scenario) run(t *testing.T, ctx context.Context, runStart time.Time, numSlots int) {
	t.Helper()
	for _, nd := range s.nodes {
		if err := nd.Start(ctx); err != nil {
			t.Fatalf("start %d: %v", nd.Num, err)
		}
	}
	time.Sleep(time.Second)
	for _, nd := range s.nodes {
		nd.ConnectToPeers(s.nw.Peers(nd.Num))
	}
	for _, nd := range s.nodes {
		if err := nd.JoinTopics(ctx); err != nil {
			t.Fatalf("join %d: %v", nd.Num, err)
		}
	}
	for _, r := range s.runners {
		r.SubscribeBackbone()
	}
	time.Sleep(2 * time.Second) // settle block + backbone meshes

	var wg sync.WaitGroup
	for _, r := range s.runners {
		wg.Go(func() { r.Run(ctx, runStart, numSlots) })
	}
	wg.Wait()
	time.Sleep(s.slotDur) // drain
	for _, nd := range s.nodes {
		nd.Close()
	}
}

// oneCommittee builds an N-node assignment with a single committee of the given attester
// nodes on subnet 0, all of them backbone subscribers of subnet 0 (so they relay +
// receive). Nodes outside the committee back a different subnet. One slot.
func oneCommittee(n int, attesters []int) *committee.Assignment {
	backbone := make([][]int, n)
	for i := range backbone {
		backbone[i] = []int{1} // default: some other subnet
	}
	var com []committee.AttesterRef
	for pos, node := range attesters {
		backbone[node] = []int{0}
		com = append(com, committee.AttesterRef{Node: node, Val: node, Subnet: 0, Position: pos})
	}
	subs := append([]int(nil), attesters...)
	return &committee.Assignment{
		Params: committee.Params{
			N: n, V: n, C: 1, Sc: len(attesters), SubnetCount: 64, BackbonePerNode: 1, NumSlots: 1,
		},
		Backbone: backbone,
		Slots: []committee.SlotPlan{{
			Slot:        0,
			Committees:  [][]committee.AttesterRef{com},
			SubnetOf:    []int{0},
			Aggregators: [][]committee.AttesterRef{{}},
			Subscribers: [][]int{subs},
		}},
	}
}
