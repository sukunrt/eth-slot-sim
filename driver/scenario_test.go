package driver_test

import (
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// scenario builds N nodes + NodeRunners over a committee assignment by hand (not via the
// Driver), so a test can override individual nodes — in particular suppress block delivery
// to chosen nodes (the test-only OnReceive filter the coupling tests need). peersP is the
// base-graph degree; set it below a subnet's subscriber count to force the per-slot dial +
// relay path (with P=n-1 a publisher reaches everyone directly and the dial is bypassed).
type scenario struct {
	a       *schedule.Assignment
	nw      *netsim.Netsim
	nodes   []*node.Node
	runners []*driver.NodeRunner
	slotDur time.Duration
}

func buildScenario(t *testing.T, a *schedule.Assignment, due time.Duration, suppressBlock map[int]bool, tracer metrics.Tracer, peersP int) *scenario {
	return buildScenarioWith(t, a, due, suppressBlock, tracer, peersP, true, a.SyncSubscribers != nil, nil, nil)
}

// buildDecoupledScenario is buildScenario's decoupled twin: attestation/sync emit off, the
// decoupled phase on (via dc). The AC vote rides the same block/column trigger, so the column-gate
// and block-suppression filters (suppressBlock, dropColumnTo) work unchanged.
func buildDecoupledScenario(t *testing.T, a *schedule.Assignment, due time.Duration, suppressBlock map[int]bool, tracer metrics.Tracer, peersP int, dc *driver.DecoupledParams) *scenario {
	return buildScenarioWith(t, a, due, suppressBlock, tracer, peersP, false, false, dc, nil)
}

// buildPartialScenario / buildPartialDecoupledScenario switch the attestation-class floods to
// the partial transport — same assignment, same timing, different wire (the parity seam).
func buildPartialScenario(t *testing.T, a *schedule.Assignment, due time.Duration, suppressBlock map[int]bool, tracer metrics.Tracer, peersP int) *scenario {
	return buildScenarioWith(t, a, due, suppressBlock, tracer, peersP, true, false, nil, &driver.PartialParams{})
}

func buildPartialDecoupledScenario(t *testing.T, a *schedule.Assignment, due time.Duration, suppressBlock map[int]bool, tracer metrics.Tracer, peersP int, dc *driver.DecoupledParams) *scenario {
	return buildScenarioWith(t, a, due, suppressBlock, tracer, peersP, false, false, dc, &driver.PartialParams{})
}

func buildScenarioRunners(t *testing.T, a *schedule.Assignment, due time.Duration, suppressBlock map[int]bool, tracer metrics.Tracer, peersP int, attest, sync bool, dc *driver.DecoupledParams) *scenario {
	return buildScenarioWith(t, a, due, suppressBlock, tracer, peersP, attest, sync, dc, nil)
}

func buildScenarioWith(t *testing.T, a *schedule.Assignment, due time.Duration, suppressBlock map[int]bool, tracer metrics.Tracer, peersP int, attest, sync bool, dc *driver.DecoupledParams, pp *driver.PartialParams) *scenario {
	t.Helper()
	n := a.Params.N
	const slotDur = 12 * time.Second
	nw, err := netsim.NewWithSchedule(a, netsim.Config{
		N: n, P: peersP, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("netsim: %v", err)
	}
	t.Cleanup(nw.Close)

	s := &scenario{a: a, nw: nw, slotDur: slotDur}
	for i := range n {
		nd := &node.Node{
			Num: i, Host: nw.Host(i), Network: nw,
			VerifyDelay:       func() time.Duration { return 0 },
			AttestVerifyDelay: func() time.Duration { return 0 },
			AttestBatchWindow: 10 * time.Millisecond,
			D:                 8, Dlo: 6, Dhi: 12,
		}
		if a.NumColumns > 0 { // size the column verifier from the node's full-custody role
			nd.ColVerifyParallelism = 4
			if a.Node(i).IsFullCustody() {
				nd.ColVerifyParallelism = 16
			}
		}
		if pp != nil {
			nd.Partial = &node.PartialOpts{Seed: 1, Resolver: driver.NewPartialResolver(a)}
		}
		// A small publish offset keeps the proposer off the exact instant the settle
		// unparks every goroutine (which drops the first flood).
		r := driver.NewRunner(i, nd, nw.Peers(i), tracer, driver.RunnerConfig{
			Schedule: a, Attest: attest, Sync: sync,
			NumNodes: n, BlockSize: 1024, Offset: 200 * time.Millisecond,
			SlotDuration: slotDur, AttestationDue: due, Seed: 1, Decoupled: dc, Partial: pp,
		})
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

// run brings the fleet up and runs numSlots, returning the runStart it used (captured
// AFTER bring-up so slot 0 starts in the future).
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
		r.Prepare()
	}
	time.Sleep(2 * time.Second) // settle block + subnet meshes

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

// genAssignment builds a sized assignment the way simctl/schedule.py does: each node
// subscribes `spn` random subnets, any subnet under `floor` is topped up, and each slot
// independently draws C committees of s_c. A test fixture builder — it need not match
// Python's RNG, since the test asserts against this assignment's own subscriber sets.
func genAssignment(n, v, c, sc, spn, floor int, seed uint64) *schedule.Assignment {
	subsSets := make([]map[int]bool, c)
	for i := range subsSets {
		subsSets[i] = map[int]bool{}
	}
	srng := rand.New(rand.NewPCG(seed, 1))
	per := min(spn, c)
	for node := range n {
		for _, s := range srng.Perm(c)[:per] {
			subsSets[s][node] = true
		}
	}
	f := min(floor, n)
	frng := rand.New(rand.NewPCG(seed, 11))
	for s := range c {
		for _, node := range frng.Perm(n) {
			if len(subsSets[s]) >= f {
				break
			}
			subsSets[s][node] = true
		}
	}
	subnetSubscribers := make([][]int, c)
	for s := range c {
		subnetSubscribers[s] = sortedKeys(subsSets[s])
	}
	sp := schedule.SlotPlan{Slot: 0}
	vals := rand.New(rand.NewPCG(seed, 2)).Perm(v)[:c*sc]
	for ci := range c {
		com := make([]schedule.AttesterRef, sc)
		for pos := range sc {
			val := vals[ci*sc+pos]
			com[pos] = schedule.AttesterRef{Node: val % n, Val: val, Subnet: ci, Position: pos}
		}
		sp.Committees = append(sp.Committees, com)
		sp.SubnetOf = append(sp.SubnetOf, ci)
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: v, C: c, Sc: sc, SubnetCount: 64,
			SubnetsPerNode: spn, SubscribeFloor: floor, Seed: seed, NumSlots: 1,
		},
		SubnetSubscribers: subnetSubscribers,
		Slots:             []schedule.SlotPlan{sp},
	}
}

func sortedKeys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// attestKey identifies one published attestation across its arrivals.
type attestKey struct{ subnet, attester int }

// assertCoverageNoLeakage checks the strongest invariant: every published attestation
// reaches exactly Subscribers(subnet) \ {publisher} — no missing, leaked (arrival at a
// non-subscriber), or duplicate.
func assertCoverageNoLeakage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, slot int) {
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
			for _, nd := range a.Subscribers(ref.Subnet) {
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
// nodes on subnet 0, where those same nodes are subnet 0's subscribers (so they mesh and
// receive each other's attestations). One slot.
func oneCommittee(n int, attesters []int) *schedule.Assignment {
	com := make([]schedule.AttesterRef, len(attesters))
	for pos, node := range attesters {
		com[pos] = schedule.AttesterRef{Node: node, Val: node, Subnet: 0, Position: pos}
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: n, C: 1, Sc: len(attesters), SubnetCount: 64,
			SubnetsPerNode: 1, SubscribeFloor: len(attesters), Seed: 1, NumSlots: 1,
		},
		SubnetSubscribers: [][]int{slices.Sorted(slices.Values(attesters))},
		Slots: []schedule.SlotPlan{{
			Slot: 0, Committees: [][]schedule.AttesterRef{com}, SubnetOf: []int{0},
		}},
	}
}
