package driver_test

import (
	"context"
	"math/rand/v2"
	"slices"
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
		// A small publish offset keeps the proposer from publishing at the exact instant
		// the bring-up settle unparks every goroutine (which drops the first flood).
		val := validator.New(i, n, 1024, 200*time.Millisecond, 0, rand.New(rand.NewPCG(1, uint64(i))))
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

// run brings the fleet up, then runs numSlots, returning the runStart it used (captured
// AFTER bring-up so slot 0 starts in the future — a runStart in the past would fire the
// proposer's publish at the chaotic instant the settle returns). Returns once the run
// plus a drain window completes.
func (s *scenario) run(t *testing.T, ctx context.Context, numSlots int) time.Time {
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
		r.Prepare(numSlots)
	}
	time.Sleep(2 * time.Second) // settle block + backbone meshes + duty-subnet joins

	runStart := time.Now()
	var wg sync.WaitGroup
	for _, r := range s.runners {
		wg.Go(func() { r.Run(ctx, runStart, numSlots) })
	}
	wg.Wait()
	time.Sleep(s.slotDur) // drain
	for _, nd := range s.nodes {
		nd.Close()
	}
	return runStart
}

// genAssignment builds a sized assignment the way simctl/committee.py does (backbone
// from node-id, an independent per-slot draw of C committees of s_c, capped aggregators,
// subscribers = backbone subs ∪ aggregator nodes). A test fixture builder — it need not
// match Python's RNG, since the test asserts against this assignment's own derived sets.
func genAssignment(n, v, c, sc, backbonePerNode, aggsPerCommittee int, seed uint64) *committee.Assignment {
	const subnetCount = 64
	backbone := make([][]int, n)
	br := rand.New(rand.NewPCG(seed, 1))
	backboneSubs := map[int][]int{}
	for nd := range n {
		backbone[nd] = br.Perm(subnetCount)[:backbonePerNode]
		slices.Sort(backbone[nd])
		for _, s := range backbone[nd] {
			backboneSubs[s] = append(backboneSubs[s], nd)
		}
	}
	cr := rand.New(rand.NewPCG(seed, 2))
	vals := cr.Perm(v)[:c*sc]
	sp := committee.SlotPlan{Slot: 0}
	for ci := range c {
		com := make([]committee.AttesterRef, sc)
		for pos := range sc {
			val := vals[ci*sc+pos]
			com[pos] = committee.AttesterRef{Node: val % n, Val: val, Subnet: ci, Position: pos}
		}
		aggPos := rand.New(rand.NewPCG(seed, uint64(100+ci))).Perm(sc)[:min(aggsPerCommittee, sc)]
		subSet := map[int]bool{}
		for _, nd := range backboneSubs[ci] {
			subSet[nd] = true
		}
		var aggs []committee.AttesterRef
		for _, p := range aggPos {
			aggs = append(aggs, com[p])
			subSet[com[p].Node] = true
		}
		subs := make([]int, 0, len(subSet))
		for nd := range subSet {
			subs = append(subs, nd)
		}
		slices.Sort(subs)
		sp.Committees = append(sp.Committees, com)
		sp.SubnetOf = append(sp.SubnetOf, ci)
		sp.Aggregators = append(sp.Aggregators, aggs)
		sp.Subscribers = append(sp.Subscribers, subs)
	}
	return &committee.Assignment{
		Params: committee.Params{
			N: n, V: v, C: c, Sc: sc, SubnetCount: subnetCount,
			BackbonePerNode: backbonePerNode, AggsPerCommittee: aggsPerCommittee, Seed: seed, NumSlots: 1,
		},
		Backbone: backbone,
		Slots:    []committee.SlotPlan{sp},
	}
}

// attestKey identifies one published attestation across its arrivals.
type attestKey struct{ subnet, attester int }

// assertCoverageNoLeakage checks the strongest invariant (§8.2): every published
// attestation reaches exactly ExpectedSubscribers(subnet) \ {publisher} — no missing, no
// leaked (arrival at a non-subscriber), no duplicate — set-equality both directions.
func assertCoverageNoLeakage(t *testing.T, a *committee.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	got := map[attestKey]map[int]bool{}
	refKeys := map[attestKey]bool{}
	for _, com := range a.Slots[slot].Committees {
		for _, ref := range com {
			refKeys[attestKey{ref.Subnet, ref.Val}] = true
		}
	}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindAttestation || ar.ID.Slot != slot {
			continue
		}
		k := attestKey{ar.ID.Subnet, ar.ID.Attester}
		if !refKeys[k] {
			t.Fatalf("phantom attestation arrival %+v (no such committee seat)", ar.ID)
		}
		if got[k] == nil {
			got[k] = map[int]bool{}
		}
		if got[k][ar.Node] {
			t.Fatalf("duplicate arrival: subnet %d attester %d at node %d", k.subnet, k.attester, ar.Node)
		}
		got[k][ar.Node] = true
	}
	for _, com := range a.Slots[slot].Committees {
		for _, ref := range com {
			want := map[int]bool{}
			for _, nd := range a.ExpectedSubscribers(slot, ref.Subnet) {
				if nd != ref.Node {
					want[nd] = true
				}
			}
			g := got[attestKey{ref.Subnet, ref.Val}]
			if len(g) != len(want) {
				t.Fatalf("subnet %d attester %d: got %d receivers %v, want %d %v",
					ref.Subnet, ref.Val, len(g), keys(g), len(want), keys(want))
			}
			for nd := range want {
				if !g[nd] {
					t.Fatalf("subnet %d attester %d: missing receiver %d", ref.Subnet, ref.Val, nd)
				}
			}
		}
	}
}

func keys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
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
