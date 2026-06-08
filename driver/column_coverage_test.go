package driver_test

import (
	"context"
	"math/rand/v2"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
)

// genColumnAssignment builds a data-column assignment by hand (the way simctl/committee.py
// does): the full-custody nodes hold every column (the relay backbone; the proposer is drawn
// from them), and each ordinary node draws custodyFloor random columns. No attestation
// committees — a focused column-coverage fixture. V=N so every node validates.
func genColumnAssignment(n, numColumns, custodyFloor int, fullCustody []int, seed uint64) *committee.Assignment {
	full := map[int]bool{}
	for _, f := range fullCustody {
		full[f] = true
	}
	colSets := make([]map[int]bool, numColumns)
	for i := range colSets {
		colSets[i] = map[int]bool{}
		for _, f := range fullCustody {
			colSets[i][f] = true
		}
	}
	rng := rand.New(rand.NewPCG(seed, 7))
	custody := min(custodyFloor, numColumns)
	for nd := range n {
		if full[nd] {
			continue
		}
		for _, c := range rng.Perm(numColumns)[:custody] {
			colSets[c][nd] = true
		}
	}
	colSubs := make([][]int, numColumns)
	for i := range colSubs {
		colSubs[i] = sortedKeys(colSets[i])
	}
	return &committee.Assignment{
		Params:            committee.Params{N: n, V: n, SubnetCount: 64, NumSlots: 1},
		NumColumns:        numColumns,
		ColumnSubscribers: colSubs,
		FullCustody:       slices.Sorted(slices.Values(fullCustody)),
		Slots:             []committee.SlotPlan{{Slot: 0, Proposer: fullCustody[0]}},
	}
}

// assertColumnCoverageNoLeakage checks the column invariant: each column reaches exactly its
// custodiers \ {proposer} — no missing, no duplicate, no leak to a non-custodier.
func assertColumnCoverageNoLeakage(t *testing.T, a *committee.Assignment, rec *metrics.Recorder, slot int) {
	t.Helper()
	proposer := a.Slots[slot].Proposer
	got := map[int]map[int]bool{}
	for _, ar := range rec.Arrivals() {
		if ar.ID.Kind != node.KindColumn || ar.ID.Slot != slot {
			continue
		}
		col := ar.ID.Subnet
		if got[col] == nil {
			got[col] = map[int]bool{}
		}
		if got[col][ar.Node] {
			t.Fatalf("duplicate column %d arrival at node %d", col, ar.Node)
		}
		got[col][ar.Node] = true
	}
	for col := range a.NumColumns {
		want := map[int]bool{}
		for _, nd := range a.ColumnSubscribersOf(col) {
			if nd != proposer {
				want[nd] = true
			}
		}
		g := got[col]
		for rcv := range g {
			if !want[rcv] {
				t.Fatalf("column %d leaked to non-custodier %d", col, rcv)
			}
		}
		if len(g) != len(want) {
			t.Fatalf("column %d: got %d receivers %v, want %d %v", col, len(g), keys(g), len(want), keys(want))
		}
		for nd := range want {
			if !g[nd] {
				t.Fatalf("column %d: missing custodier %d", col, nd)
			}
		}
	}
}

// The proposer (a full-custody node) bursts every column at t=0; each column reaches exactly
// its custodiers via the backbone relay (peersP < a column's custodier count ⇒ multi-hop),
// and no non-custodier receives it.
func TestColumnCoverageNoLeakage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := genColumnAssignment(12, 16, 4, []int{0, 1, 2}, 42)
		rec := metrics.NewRecorder()
		s := buildScenario(t, a, 4*time.Second, nil, rec, 4) // peersP=4 forces relay
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s.run(t, ctx, 1)
		assertColumnCoverageNoLeakage(t, a, rec, 0)
	})
}
