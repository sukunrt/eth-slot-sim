package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// aggAssignment builds a one-slot assignment focused on the aggregate phase: C committees
// (one nominal attester each, so the attestation phase is well-formed but not the subject),
// the given per-subnet subscribers, and the given per-committee aggregator node sets.
func aggAssignment(n int, subscribers, aggregators [][]int) *schedule.Assignment {
	c := len(subscribers)
	sp := schedule.SlotPlan{Slot: 0}
	for ci := range c {
		sp.Committees = append(sp.Committees, []schedule.AttesterRef{
			{Node: subscribers[ci][0], Val: subscribers[ci][0], Subnet: ci, Position: 0},
		})
		sp.SubnetOf = append(sp.SubnetOf, ci)
		sp.Aggregators = append(sp.Aggregators, aggregators[ci])
	}
	return &schedule.Assignment{
		Params: schedule.Params{
			N: n, V: n, C: c, Sc: 1, SubnetCount: 64, SubnetsPerNode: 1,
			SubscribeFloor: 1, TargetAggregators: 16, Seed: 1, NumSlots: 1,
		},
		SubnetSubscribers: subscribers,
		Slots:             []schedule.SlotPlan{sp},
	}
}

// aggDriver builds the real Driver over a committee-aware netsim with the aggregate phase
// enabled (P=n-1 ⇒ the global aggregate topic is fully connected, so dissemination is
// one-hop and the test isolates the coverage logic from the dial path).
func aggDriver(t *testing.T, a *schedule.Assignment, aggDue time.Duration, tr metrics.Tracer) *driver.Driver {
	t.Helper()
	n := a.Params.N
	nw, err := netsim.NewWithSchedule(a, netsim.Config{
		N: n, P: n - 1, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("netsim: %v", err)
	}
	t.Cleanup(nw.Close)
	return driver.New(nw, driver.Config{
		BlockSize: 1024, SlotDuration: 12 * time.Second, Offset: 200 * time.Millisecond, Jitter: 0,
		VerifyDelay:       func() time.Duration { return 0 },
		AttestVerifyDelay: func() time.Duration { return 0 },
		AttestBatchWindow: 10 * time.Millisecond,
		D:                 8, Dlo: 6, Dhi: 12, Seed: 1,
		Schedule: a, Attest: true, AttestationDue: 4 * time.Second, AggregateDue: aggDue,
	}, tr)
}

type aggKey struct{ subnet, aggregator int }

// assertAggregateCoverage is the headline invariant: each aggregator publishes ONE distinct
// aggregate (the aggregator carried in the Attester field) on the global topic, which reaches
// every node EXCEPT that aggregator, exactly once. The aggregates are distinct per aggregator
// (the signature), so a committee's other aggregators DO receive it — only the publisher's own
// loopback is skipped.
func assertAggregateCoverage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	sp := a.Slots[slot]
	got := map[aggKey]map[int]int{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindAggregate || ar.ID.Slot != slot {
			continue
		}
		k := aggKey{ar.ID.Subnet, ar.ID.Attester}
		if got[k] == nil {
			got[k] = map[int]int{}
		}
		got[k][ar.Node]++
	}
	for ci, subnet := range sp.SubnetOf {
		for _, aggregator := range sp.Aggregators[ci] {
			recvd := got[aggKey{subnet, aggregator}]
			for nd := range a.Params.N {
				want := 1
				if nd == aggregator {
					want = 0 // the aggregator published it; its own loopback is skipped
				}
				if recvd[nd] != want {
					t.Fatalf("subnet %d aggregator %d node %d: got %d arrivals, want %d",
						subnet, aggregator, nd, recvd[nd], want)
				}
			}
		}
	}
}

func aggregateArrivalCount(rec *metrics.Recorder, slot int) int {
	n := 0
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind == node.KindAggregate && ar.ID.Slot == slot {
			n++
		}
	}
	return n
}

// The headline test. Schedule 0's aggregators {0,1} and committee 1's {0,3} (node 0
// aggregates both — a node in two committees). Each aggregator publishes one distinct
// aggregate that reaches every node except itself.
func TestAggregatesDistinctAndCover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 6
		a := aggAssignment(n, [][]int{{0, 1, 2}, {3, 4, 5}}, [][]int{{0, 1}, {0, 3}})
		rec := metrics.NewRecorder()
		d := aggDriver(t, a, 8*time.Second, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), 1)

		assertAggregateCoverage(t, a, rec, 0)
		// Σ_c |A_c|·(N−1) = 2·5 + 2·5 = 20 (4 distinct aggregates, each reaching N−1).
		if got, want := aggregateArrivalCount(rec, 0), (2+2)*(n-1); got != want {
			t.Fatalf("aggregate arrivals = %d, want %d", got, want)
		}
	})
}

// Each aggregator publishes exactly one aggregate per committee it aggregates, all at
// slotStart+AggregateDue. Total publishes = Σ_c |A_c| (no m multiplier).
func TestAggregatesPublishAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 6
		a := aggAssignment(n, [][]int{{0, 1, 2}, {3, 4, 5}}, [][]int{{0, 1}, {0, 3}})
		tr := &timeTracer{}
		d := aggDriver(t, a, 8*time.Second, tr)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		runStart := time.Now()
		d.Run(ctx, runStart, 1)

		tr.mu.Lock()
		defer tr.mu.Unlock()
		wantAt := runStart.Add(8 * time.Second)
		var aggPubs int
		for _, p := range tr.pubs {
			if p.id.Kind != node.KindAggregate {
				continue
			}
			aggPubs++
			if !p.at.Equal(wantAt) {
				t.Fatalf("aggregate %+v published at %v, want %v (slotStart+AggregateDue)", p.id, p.at, wantAt)
			}
		}
		if want := 2 + 2; aggPubs != want { // |A_0| + |A_1|, one aggregate each
			t.Fatalf("aggregate publishes = %d, want %d", aggPubs, want)
		}
	})
}

// With AggregateDue == 0 the aggregate phase is off: no aggregate is published or received
// (block + attestation behavior unchanged).
func TestAggregatesDisabled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := aggAssignment(6, [][]int{{0, 1, 2}, {3, 4, 5}}, [][]int{{0, 1}, {0, 3}})
		rec := metrics.NewRecorder()
		d := aggDriver(t, a, 0, rec) // aggregates disabled

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), 1)

		if got := aggregateArrivalCount(rec, 0); got != 0 {
			t.Fatalf("aggregate arrivals with AggregateDue=0 = %d, want 0", got)
		}
	})
}
