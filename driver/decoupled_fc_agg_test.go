package driver_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/schedule"
	"github.com/ethp2p/slot-sim/validator"
)

// dialRecorder wraps a node's Network to record which peers it resolves (i.e. dials) and when, so a
// test can isolate the per-finality-slot pre-join dials (those at time ≥ runStart) from the base
// dials at bring-up.
type dialRecorder struct {
	inner node.Network
	mu    sync.Mutex
	dials []struct {
		peer int
		at   time.Time
	}
}

func (d *dialRecorder) PeerAddr(nodeNum int) ma.Multiaddr {
	d.mu.Lock()
	d.dials = append(d.dials, struct {
		peer int
		at   time.Time
	}{nodeNum, time.Now()})
	d.mu.Unlock()
	return d.inner.PeerAddr(nodeNum)
}

func (d *dialRecorder) after(runStart time.Time) []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []int
	for _, pd := range d.dials {
		if !pd.at.Before(runStart) {
			out = append(out, pd.peer)
		}
	}
	return out
}

// decoupledFCAggAssignment builds an N-node decoupled assignment focused on the finality aggregate:
// the given finality-subnet partition + per-subnet aggregator sets (drawn on every finality
// boundary slot), V validators (uniform v%N ⇒ validators_per_subnet). Node 0 proposes the
// (irrelevant) block.
func decoupledFCAggAssignment(n, v, k int, subnets, aggregators [][]int, numSlots int) *schedule.Assignment {
	vps := make([]int, len(subnets))
	for i, members := range subnets {
		for _, nd := range members {
			vps[i] += (v-1-nd)/n + 1 // validators node nd hosts (uniform)
		}
	}
	slots := make([]schedule.SlotPlan, numSlots)
	for s := range slots {
		sp := schedule.SlotPlan{Slot: s, Proposer: 0}
		if s%k == 0 { // finality boundary carries the aggregators
			sp.FinalityAggregators = aggregators
		}
		slots[s] = sp
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: v, AcSlotsPerFinalitySlot: k, FsSubnets: len(subnets), NumSlots: numSlots,
		},
		FinalitySubscribers: subnets,
		ValidatorsPerSubnet: vps,
		Slots:               slots,
	}
}

// assertFinalityAggregateCoverage: each aggregator publishes ONE distinct aggregate (the aggregator
// rides Attester) on the global topic, reaching every node except itself, exactly once — the
// aggregate twin, mirroring assertAggregateCoverage but read from the finality-boundary slot.
func assertFinalityAggregateCoverage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, fslot int) {
	t.Helper()
	sp := a.Slots[fslot*a.Params.AcSlotsPerFinalitySlot]
	got := map[aggKey]map[int]int{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindFinalityAggregate || ar.ID.Slot != fslot {
			continue
		}
		k := aggKey{ar.ID.Subnet, ar.ID.Attester}
		if got[k] == nil {
			got[k] = map[int]int{}
		}
		got[k][ar.Node]++
	}
	for subnet, aggs := range sp.FinalityAggregators {
		for _, agg := range aggs {
			recvd := got[aggKey{subnet, agg}]
			for nd := range a.Params.N {
				want := 1
				if nd == agg {
					want = 0 // the aggregator published it; its own loopback is skipped
				}
				if recvd[nd] != want {
					t.Fatalf("finality aggregate subnet %d agg %d node %d: got %d, want %d",
						subnet, agg, nd, recvd[nd], want)
				}
			}
		}
	}
}

// fs_aggregators per subnet each publish one distinct, population-scaled aggregate on the global
// topic; each reaches N−1, and its size matches FinalityAggregateSize(validators_per_subnet[subnet])
// — i.e. the aggregator sized it by its subnet's voting population. Measured on a settled finality
// slot (1), so the aggregator's pre-join (at AC slot k−1+...) and cross-slot timer have run.
func TestDecoupledFinalityAggregateCoverageAndSize(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		a := decoupledFCAggAssignment(8, 8, 2, [][]int{{0, 2, 4, 6}, {1, 3, 5, 7}}, [][]int{{0, 2}, {1, 3}}, 5)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, FCAggFraction: 50}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 4, dc)

		// Capture the wire size of each finality-slot-1 aggregate at node 4 (a non-aggregator that
		// receives all of them on the global topic). Re-marshal the decoded message: proto3 is
		// canonical, so this reproduces the published byte count (the decoded payload field alone
		// varies with origin/subnet — zero fields are omitted — but the marshaled total does not).
		var mu sync.Mutex
		wireLen := map[aggKey]int{} // (subnet, aggregator origin) → marshaled bytes
		orig := s.nodes[4].OnReceive
		s.nodes[4].OnReceive = func(r node.Received) {
			if fa, ok := r.Obj.(*pb.FinalityAggregate); ok && fa.FinalitySlot == 1 {
				b, err := proto.Marshal(fa)
				if err != nil {
					t.Errorf("marshal received aggregate: %v", err)
				}
				mu.Lock()
				wireLen[aggKey{int(fa.Subnet), int(fa.Origin)}] = len(b)
				mu.Unlock()
			}
			orig(r)
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 5)

		assertFinalityAggregateCoverage(t, a, rec, 1)

		// 4 aggregators, each reaching N−1 = 7 ⇒ 28 arrivals for finality slot 1.
		got := 0
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindFinalityAggregate && ar.ID.Slot == 1 {
				got++
			}
		}
		if want := 4 * (a.Params.N - 1); got != want {
			t.Fatalf("finality aggregate arrivals (fslot 1) = %d, want %d", got, want)
		}

		// Each aggregate is sized to its subnet's voting population: its wire bytes equal what
		// MakeFinalityAggregate produces for validators_per_subnet[subnet] (a wrong vps would differ).
		mu.Lock()
		defer mu.Unlock()
		for subnet, aggs := range a.Slots[2].FinalityAggregators { // boundary slot for finality slot 1
			for _, agg := range aggs {
				want := len(validator.MakeFinalityAggregate(1, subnet, agg, a.ValidatorsPerSubnet[subnet]).Payload)
				if got := wireLen[aggKey{subnet, agg}]; got != want {
					t.Fatalf("finality aggregate subnet %d agg %d wire size = %d bytes, want %d (vps=%d)",
						subnet, agg, got, want, a.ValidatorsPerSubnet[subnet])
				}
			}
		}
	})
}

// Each finality-slot-1 aggregate publishes at exactly finalitySlotStart(1) + fcAggFraction%·k·slotDur
// = runStart + (1·2·12s) + (50%·2·12s) = runStart + 36s — proving the deadline arithmetic and that
// the aggregate timer (armed at the boundary, k/2 slots earlier) survived the intervening endSlots.
func TestDecoupledFinalityAggregatePublishInstant(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		a := decoupledFCAggAssignment(8, 8, 2, [][]int{{0, 2, 4, 6}, {1, 3, 5, 7}}, [][]int{{0, 2}, {1, 3}}, 5)
		tr := &timeTracer{}
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, FCAggFraction: 50}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, tr, 4, dc)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		runStart := s.run(t, ctx, 5)

		wantAt := runStart.Add(36 * time.Second)
		aggs := 0
		tr.mu.Lock()
		defer tr.mu.Unlock()
		for _, p := range tr.pubs {
			if p.id.Kind != node.KindFinalityAggregate || p.id.Slot != 1 {
				continue
			}
			aggs++
			if !p.at.Equal(wantAt) {
				t.Fatalf("finality aggregate %+v published at %v, want %v", p.id, p.at, wantAt)
			}
		}
		if want := 4; aggs != want { // 2 subnets × 2 aggregators, one aggregate each
			t.Fatalf("finality aggregate publishes (fslot 1) = %d, want %d", aggs, want)
		}
	})
}

// An aggregator dials extra members of its subnet during the run (the previous-AC-slot pre-join);
// a non-aggregator dials nothing during the run. Subnets of 6 with peersP=4 guarantee at least one
// non-base subnet member to dial (a node has ≤4 base peers but 5 subnet mates).
func TestDecoupledFinalityAggregatePreJoin(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		// 12 nodes, 2 subnets of 6; aggregators {0} and {1}. V=N ⇒ vps = subnet size.
		subnets := [][]int{{0, 2, 4, 6, 8, 10}, {1, 3, 5, 7, 9, 11}}
		a := decoupledFCAggAssignment(12, 12, 2, subnets, [][]int{{0}, {1}}, 5)
		rec := metrics.NewRecorder()
		dc := &driver.DecoupledParams{K: 2, FCVoteOffset: time.Second, FCAggFraction: 50}
		s := buildDecoupledScenario(t, a, 4*time.Second, nil, rec, 4, dc)
		recorders := make([]*dialRecorder, len(s.nodes))
		for i, nd := range s.nodes {
			recorders[i] = &dialRecorder{inner: nd.Network}
			nd.Network = recorders[i]
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		runStart := s.run(t, ctx, 5)

		// Aggregator 0 pre-joined extra subnet-0 members during the run; node 2 (a non-aggregator
		// member of subnet 0) dialed nothing during the run.
		aggDials := recorders[0].after(runStart)
		if len(aggDials) == 0 {
			t.Fatal("aggregator 0 dialed no extra peers during the run — pre-join missing")
		}
		for _, peer := range aggDials {
			if !slices.Contains(subnets[0], peer) || peer == 0 {
				t.Fatalf("aggregator 0 pre-join dialed %d, want a subnet-0 member ≠ 0", peer)
			}
		}
		if d := recorders[2].after(runStart); len(d) != 0 {
			t.Fatalf("non-aggregator 2 dialed %v during the run, want none", d)
		}
	})
}
