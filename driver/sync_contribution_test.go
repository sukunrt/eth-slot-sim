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

// syncContribAssignment builds a one-slot assignment focused on the contribution phase: the given
// per-subnet sync members and per-subnet aggregator sets (a member is on one subnet, so each
// aggregator aggregates exactly its subnet — unlike attestation aggregators). No attestation
// committees; node 0 proposes the slot's block (contributions are fixed-deadline, not coupled).
func syncContribAssignment(n int, members, aggregators [][]int) *schedule.Assignment {
	return &schedule.Assignment{
		Params:          schedule.Params{N: n, V: n, SubnetCount: 64, NumSlots: 1},
		SyncSubscribers: members,
		Slots:           []schedule.SlotPlan{{Slot: 0, Proposer: 0, SyncAggregators: aggregators}},
	}
}

// syncContribDriver builds the real Driver over a sync-aware netsim with the contribution phase
// on and attestations off (P=n-1 ⇒ the global contribution topic is one-hop, isolating coverage
// from the dial path).
func syncContribDriver(t *testing.T, a *schedule.Assignment, aggDue time.Duration, tr metrics.Tracer) *driver.Driver {
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
		Schedule: a, Attest: false, Sync: true, AggregateDue: aggDue,
	}, tr)
}

type syncContribKey struct{ subnet, aggregator int }

// assertSyncContributionCoverage: each aggregator publishes ONE distinct contribution (the
// aggregator in the Attester field) on the global topic, which reaches every node EXCEPT that
// aggregator, exactly once. The contributions are distinct per aggregator, so a subnet's other
// aggregators DO receive it — only the publisher's own loopback is skipped.
func assertSyncContributionCoverage(t *testing.T, a *schedule.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	got := map[syncContribKey]map[int]int{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindSyncContribution || ar.ID.Slot != slot {
			continue
		}
		k := syncContribKey{ar.ID.Subnet, ar.ID.Attester}
		if got[k] == nil {
			got[k] = map[int]int{}
		}
		got[k][ar.Node]++
	}
	for subnet, aggs := range a.Slots[slot].SyncAggregators { // SyncAggregators is indexed by subnet
		for _, aggregator := range aggs {
			recvd := got[syncContribKey{subnet, aggregator}]
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

func syncContributionArrivalCount(rec *metrics.Recorder, slot int) int {
	n := 0
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind == node.KindSyncContribution && ar.ID.Slot == slot {
			n++
		}
	}
	return n
}

// Each subnet's aggregators publish one distinct contribution on the global topic, each reaching
// every node except itself. subnet 0 aggregators {0,1} ⊆ members {0,1,2}; subnet 1 {3,4} ⊆ {3,4,5}.
func TestSyncContributionsDistinctAndCover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 6
		a := syncContribAssignment(n, [][]int{{0, 1, 2}, {3, 4, 5}}, [][]int{{0, 1}, {3, 4}})
		rec := metrics.NewRecorder()
		d := syncContribDriver(t, a, 8*time.Second, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), 1)

		assertSyncContributionCoverage(t, a, rec, 0)
		// 4 distinct contributions, each reaching N−1 ⇒ (2+2)·(n−1) arrivals.
		if got, want := syncContributionArrivalCount(rec, 0), (2+2)*(n-1); got != want {
			t.Fatalf("contribution arrivals = %d, want %d", got, want)
		}
	})
}

// Each aggregator publishes exactly one contribution, all at slotStart+AggregateDue.
func TestSyncContributionsPublishAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 6
		a := syncContribAssignment(n, [][]int{{0, 1, 2}, {3, 4, 5}}, [][]int{{0, 1}, {3, 4}})
		tr := &timeTracer{}
		d := syncContribDriver(t, a, 8*time.Second, tr)

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
		var pubs int
		for _, p := range tr.pubs {
			if p.id.Kind != node.KindSyncContribution {
				continue
			}
			pubs++
			if !p.at.Equal(wantAt) {
				t.Fatalf("contribution %+v published at %v, want %v (slotStart+AggregateDue)", p.id, p.at, wantAt)
			}
		}
		if want := 2 + 2; pubs != want { // |A_0| + |A_1|, one contribution each
			t.Fatalf("contribution publishes = %d, want %d", pubs, want)
		}
	})
}
