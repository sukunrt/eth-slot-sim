package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
)

// aggAssignment builds a one-slot assignment focused on the aggregate phase: C committees
// (one nominal attester each, so the attestation phase is well-formed but not the subject),
// the given per-subnet subscribers, and the given per-committee aggregator node sets. m is
// aggregates-per-committee.
func aggAssignment(n, m int, subscribers, aggregators [][]int) *committee.Assignment {
	c := len(subscribers)
	sp := committee.SlotPlan{Slot: 0}
	for ci := range c {
		sp.Committees = append(sp.Committees, []committee.AttesterRef{
			{Node: subscribers[ci][0], Val: subscribers[ci][0], Subnet: ci, Position: 0},
		})
		sp.SubnetOf = append(sp.SubnetOf, ci)
		sp.Aggregators = append(sp.Aggregators, aggregators[ci])
	}
	return &committee.Assignment{
		Params: committee.Params{
			N: n, V: n, C: c, Sc: 1, SubnetCount: 64, SubnetsPerNode: 1,
			SubscribeFloor: 1, TargetAggregators: 16, M: m, Seed: 1, NumSlots: 1,
		},
		SubnetSubscribers: subscribers,
		Slots:             []committee.SlotPlan{sp},
	}
}

// aggDriver builds the real Driver over a committee-aware netsim with the aggregate phase
// enabled (P=n-1 ⇒ the global aggregate topic is fully connected, so dissemination is
// one-hop and the test isolates the dedup/coverage logic from the dial path).
func aggDriver(t *testing.T, a *committee.Assignment, aggDue time.Duration, tr metrics.Tracer) *driver.Driver {
	t.Helper()
	n := a.Params.N
	nw, err := netsim.NewWithCommittee(a, netsim.Config{
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
		Committee: a, Attest: true, AttestationDue: 4 * time.Second, AggregateDue: aggDue,
	}, tr)
}

type aggKey struct{ subnet, aggIdx int }

// assertAggregateCoverage is the headline invariant: each of a committee's m aggregates
// reaches every node EXCEPT that committee's aggregators, EXACTLY ONCE. "Exactly once"
// proves gossipsub deduped the byte-identical copies the |A_c| aggregators each published
// (multi-source); the aggregator exclusion proves the publish loopback is skipped.
func assertAggregateCoverage(t *testing.T, a *committee.Assignment, rec *metrics.Recorder, slot int) {
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
		isAgg := map[int]bool{}
		for _, x := range sp.Aggregators[ci] {
			isAgg[x] = true
		}
		for aggIdx := range a.Params.M {
			recvd := got[aggKey{subnet, aggIdx}]
			for nd := range a.Params.N {
				want := 1
				if isAgg[nd] {
					want = 0 // aggregators publish it; their loopback is skipped
				}
				if recvd[nd] != want {
					t.Fatalf("subnet %d aggIdx %d node %d: got %d arrivals, want %d (dedup/loopback)",
						subnet, aggIdx, nd, recvd[nd], want)
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

// The headline test. Committee 0's aggregators {0,1} both publish the same m=2 aggregates
// (multi-source ⇒ dedup); node 0 also aggregates committee 1 (a node in two committees).
// Every non-aggregator receives each aggregate exactly once; aggregators receive none of
// their own committee's.
func TestAggregatesDedupAndCover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n, m = 6, 2
		a := aggAssignment(n, m, [][]int{{0, 1, 2}, {3, 4, 5}}, [][]int{{0, 1}, {0, 3}})
		rec := metrics.NewRecorder()
		d := aggDriver(t, a, 8*time.Second, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), 1)

		assertAggregateCoverage(t, a, rec, 0)
		// Σ_c m·(N−|A_c|) = 2·(6−2) + 2·(6−2) = 16.
		if got, want := aggregateArrivalCount(rec, 0), m*(n-2)+m*(n-2); got != want {
			t.Fatalf("aggregate arrivals = %d, want %d", got, want)
		}
	})
}

// Each aggregator publishes its committee's m aggregates, all at exactly slotStart+AggregateDue.
// Total publishes = Σ_c |A_c|·m (multi-source: the same logical aggregate is published once
// per aggregator, before gossipsub dedups them on the wire).
func TestAggregatesPublishAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n, m = 6, 2
		a := aggAssignment(n, m, [][]int{{0, 1, 2}, {3, 4, 5}}, [][]int{{0, 1}, {0, 3}})
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
		if want := (2 + 2) * m; aggPubs != want { // |A_0|=2, |A_1|=2, each ×m
			t.Fatalf("aggregate publishes = %d, want %d", aggPubs, want)
		}
	})
}

// With AggregateDue == 0 the aggregate phase is off: no aggregate is published or received
// (block + attestation behavior unchanged).
func TestAggregatesDisabled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := aggAssignment(6, 2, [][]int{{0, 1, 2}, {3, 4, 5}}, [][]int{{0, 1}, {0, 3}})
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
