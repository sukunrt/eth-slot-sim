package driver_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
)

// genSyncAssignment builds a sized sync assignment the way simctl/schedule.py does: `size` member
// nodes drawn from 1..n-1, round-robin into `subnets` subnets; per slot, `targetAgg` aggregators
// per subnet drawn from its members. Node 0 (kept out of the membership) proposes the block. A
// test fixture builder with its own RNG — the test asserts against this assignment's own sets.
func genSyncAssignment(n, size, subnets, targetAgg int, seed uint64) *schedule.Assignment {
	pool := rand.New(rand.NewPCG(seed, 6)).Perm(n - 1) // draw members from 1..n-1 (0 stays proposer)
	subs := make([][]int, subnets)
	for j := range size {
		m := pool[j] + 1
		subs[j%subnets] = append(subs[j%subnets], m)
	}
	for i := range subs {
		slices.Sort(subs[i])
	}
	aggRng := rand.New(rand.NewPCG(seed, 7))
	syncAggs := make([][]int, subnets)
	for i, members := range subs {
		k := min(targetAgg, len(members))
		picked := make([]int, k)
		for j, p := range aggRng.Perm(len(members))[:k] {
			picked[j] = members[p]
		}
		slices.Sort(picked)
		syncAggs[i] = picked
	}
	return &schedule.Assignment{
		Params:          schedule.Params{N: n, V: n, SubnetCount: 64, NumSlots: 1},
		SyncSubscribers: subs,
		Slots:           []schedule.SlotPlan{{Slot: 0, Proposer: 0, SyncAggregators: syncAggs}},
	}
}

// syncFullDriver builds the real Driver over a sync-aware netsim with the full sync phase on
// (messages at attestation_due + contributions at aggregate_due) and attestations off, so the
// run exercises only the sync traffic on the country-realistic discv5 graph.
func syncFullDriver(t *testing.T, a *schedule.Assignment, peersP int, rec metrics.Tracer) *driver.Driver {
	t.Helper()
	n := a.Params.N
	nw, err := netsim.NewWithSchedule(a, netsim.Config{
		N: n, P: peersP, Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond,
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
		Schedule: a, Attest: false, Sync: true,
		AttestationDue: 4 * time.Second, AggregateDue: 8 * time.Second,
	}, rec)
}

// A sized sync run produces the headline outputs: the per-subnet message arrival CDF +
// fraction_voted_head, and the global contribution CDF — with full coverage and no leak on both.
// Race-skipped (the flood overflows TSan's epoch, like the attestation/column runs).
func TestSyncFullRunOutputs(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	synctest.Test(t, func(t *testing.T) {
		a := genSyncAssignment(16, 8, 2, 2, 42) // 8 members, 2 subnets (4 each), 2 aggregators/subnet
		rec := metrics.NewRecorder()
		d := syncFullDriver(t, a, 6, rec)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), 1)

		assertSyncCoverageNoLeakage(t, a, rec, 0)
		assertSyncContributionCoverage(t, a, rec, 0)

		// Per-subnet message arrival CDF: each member reaches its subnet's other members.
		var msgDelays []time.Duration
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindSyncMessage && ar.ID.Slot == 0 {
				msgDelays = append(msgDelays, ar.Delay)
			}
		}
		wantMsg := 0
		for _, m := range a.SyncSubscribers {
			wantMsg += len(m) * (len(m) - 1)
		}
		mc := metrics.Summarize(msgDelays)
		if mc.Count != wantMsg {
			t.Fatalf("sync message CDF count = %d, want %d", mc.Count, wantMsg)
		}
		if mc.P50 <= 0 || mc.P50 > mc.P100 {
			t.Fatalf("sync message CDF percentiles unsorted/zero: p50=%v p100=%v", mc.P50, mc.P100)
		}

		// Every member saw the block well before the deadline ⇒ all voted head.
		if got := rec.FractionVotedHead(0); got != 1.0 {
			t.Fatalf("FractionVotedHead = %v, want 1.0", got)
		}

		// Global contribution CDF: each aggregator reaches N−1.
		var contribDelays []time.Duration
		for _, ar := range rec.Arrivals() {
			if ar.ID.Kind == node.KindSyncContribution && ar.ID.Slot == 0 {
				contribDelays = append(contribDelays, ar.Delay)
			}
		}
		wantContrib := 0
		for _, aggs := range a.Slots[0].SyncAggregators {
			wantContrib += len(aggs) * (a.Params.N - 1)
		}
		if cc := metrics.Summarize(contribDelays); cc.Count != wantContrib {
			t.Fatalf("contribution CDF count = %d, want %d", cc.Count, wantContrib)
		}
	})
}

// Determinism guard: the same seed in two separate bubbles delivers the same set of arrivals
// (the coverage is reproducible). Identities only — the per-arrival delay isn't compared, since
// gossipsub's mesh selection draws from the process-global RNG, which advances between the two
// bubbles, so relay paths (and thus timings) differ even at a fixed seed.
func TestSyncRunDeterministic(t *testing.T) {
	if raceEnabled {
		t.Skip("race detector overflows TSan's epoch on this sized synctest run")
	}
	runOnce := func(t *testing.T) []string {
		var keys []string
		synctest.Test(t, func(t *testing.T) {
			a := genSyncAssignment(16, 8, 2, 2, 42)
			rec := metrics.NewRecorder()
			d := syncFullDriver(t, a, 6, rec)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := d.BringUp(ctx); err != nil {
				t.Fatal(err)
			}
			d.Run(ctx, time.Now(), 1)
			for _, ar := range rec.Arrivals() {
				keys = append(keys, fmt.Sprintf("%d,%d,%d,%d",
					ar.Node, ar.ID.Kind, ar.ID.Subnet, ar.ID.Attester))
			}
			slices.Sort(keys)
		})
		return keys
	}
	var first, second []string
	t.Run("run1", func(t *testing.T) { first = runOnce(t) })
	t.Run("run2", func(t *testing.T) { second = runOnce(t) })
	if !slices.Equal(first, second) {
		t.Fatal("sync run not deterministic across two same-seed bubbles")
	}
}
