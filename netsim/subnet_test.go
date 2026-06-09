package netsim

import (
	"slices"
	"testing"

	"github.com/ethp2p/slot-sim/schedule"
)

// committeeWith builds a minimal assignment carrying just what discv5Graph reads: N and
// the per-subnet subscriber sets.
func committeeWith(n int, subnets [][]int) *schedule.Assignment {
	return &schedule.Assignment{
		Params:            schedule.Params{N: n, C: len(subnets), SubnetCount: 64, NumSlots: 1},
		SubnetSubscribers: subnets,
	}
}

// componentConnected reports whether members form one connected piece using only edges
// internal to the set. Fewer than two members is trivially connected.
func componentConnected(adj [][]int, members []int) bool {
	if len(members) < 2 {
		return true
	}
	in := make(map[int]bool, len(members))
	for _, m := range members {
		in[m] = true
	}
	seen := map[int]bool{members[0]: true}
	queue := []int{members[0]}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, j := range adj[cur] {
			if in[j] && !seen[j] {
				seen[j] = true
				queue = append(queue, j)
			}
		}
	}
	return len(seen) == len(members)
}

// assertSimple fails if adj has a self-loop, a duplicate, or an asymmetric edge.
func assertSimple(t *testing.T, adj [][]int) {
	t.Helper()
	for i := range adj {
		seen := map[int]bool{}
		for _, j := range adj[i] {
			switch {
			case j == i:
				t.Fatalf("self-loop at %d", i)
			case seen[j]:
				t.Fatalf("duplicate edge %d-%d", i, j)
			case !slices.Contains(adj[j], i):
				t.Fatalf("edge %d-%d not symmetric", i, j)
			}
			seen[j] = true
		}
	}
}

func TestDiscv5GraphConnectivityAndDegree(t *testing.T) {
	subnets := [][]int{
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{10, 11, 12, 13, 14, 15},
		{2, 5, 11, 18, 19}, // overlaps the others
	}
	const n, k = 20, 8
	adj := discv5Graph(committeeWith(n, subnets), k, 1)

	assertSimple(t, adj)
	if !componentConnected(adj, seq(n)) {
		t.Fatal("whole graph not connected (block topic would partition)")
	}
	for s, members := range subnets {
		if !componentConnected(adj, members) {
			t.Fatalf("subnet %d subscribers not connected: %v", s, members)
		}
	}

	// Soft target K: fill ran, so mean degree is around K (well above the tree-only ~2),
	// and no node exceeds n-1 (a simple graph).
	total := 0
	for i := range adj {
		total += len(adj[i])
		if len(adj[i]) > n-1 {
			t.Fatalf("node %d degree %d exceeds n-1", i, len(adj[i]))
		}
	}
	if mean := float64(total) / n; mean < k-1 {
		t.Fatalf("mean degree %.1f < K-1 (%d): fill did not reach K", mean, k-1)
	}

	// Deterministic in the seed.
	if !slices.EqualFunc(adj, discv5Graph(committeeWith(n, subnets), k, 1), slices.Equal) {
		t.Fatal("discv5Graph not deterministic for a fixed seed")
	}
}

func TestDiscv5GraphGracefulSmallN(t *testing.T) {
	// K far larger than n-1, plus a singleton and an empty subnet: must not crash or spin,
	// degree is capped at n-1, and everything stays connected.
	adj := discv5Graph(committeeWith(3, [][]int{{0, 1, 2}, {0}, {}}), 10, 1)
	if len(adj) != 3 {
		t.Fatalf("adjacency length %d, want 3", len(adj))
	}
	assertSimple(t, adj)
	for i := range adj {
		if len(adj[i]) > 2 {
			t.Fatalf("node %d degree %d exceeds n-1=2", i, len(adj[i]))
		}
	}
	if !componentConnected(adj, seq(3)) {
		t.Fatal("small-n graph not connected")
	}

	// n=1 is a no-op, not a panic.
	if got := discv5Graph(committeeWith(1, [][]int{{0}}), 10, 1); len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("n=1 graph = %v, want one empty adjacency list", got)
	}
}
