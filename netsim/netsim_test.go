package netsim

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/marcopolo/simnet"
)

const (
	testLo = 10 * time.Millisecond
	testHi = 150 * time.Millisecond
)

func TestPairLatencySymmetricStableInRange(t *testing.T) {
	seed := uint64(42)
	seen := map[time.Duration]int{}
	for i := range 50 {
		for j := i + 1; j < 50; j++ {
			a, b := simnet.IntToPublicIPv4(i), simnet.IntToPublicIPv4(j)
			d := pairLatency(seed, testLo, testHi, a, b)
			if d < testLo || d >= testHi {
				t.Fatalf("(%d,%d): latency %v out of [%v,%v)", i, j, d, testLo, testHi)
			}
			if rev := pairLatency(seed, testLo, testHi, b, a); rev != d {
				t.Fatalf("(%d,%d): asymmetric %v vs %v", i, j, d, rev)
			}
			if again := pairLatency(seed, testLo, testHi, a, b); again != d {
				t.Fatalf("(%d,%d): unstable %v vs %v", i, j, d, again)
			}
			seen[d]++
		}
	}
	if len(seen) < 50 { // expect a spread of distinct values, not a constant
		t.Fatalf("latency not varied: only %d distinct values", len(seen))
	}
}

func TestPickSupernodesExactShareDeterministic(t *testing.T) {
	const n = 100
	set := pickSupernodes(n, 0.20, 7)
	if len(set) != 20 {
		t.Fatalf("got %d supernodes, want 20", len(set))
	}
	for id := range set {
		if id < 0 || id >= n {
			t.Fatalf("supernode id %d out of range", id)
		}
	}
	set2 := pickSupernodes(n, 0.20, 7)
	if len(set2) != 20 {
		t.Fatalf("non-deterministic count: %d", len(set2))
	}
	for id := range set {
		if !set2[id] {
			t.Fatalf("non-deterministic membership for %d", id)
		}
	}
}

func TestPeerGraphConnectedSymmetric(t *testing.T) {
	const n, p = 100, 20
	adj := peerGraph(n, p, 3)
	if len(adj) != n {
		t.Fatalf("got %d adjacency lists, want %d", len(adj), n)
	}
	// Symmetric: j in adj[i] <=> i in adj[j].
	for i := range adj {
		for _, j := range adj[i] {
			if !contains(adj[j], i) {
				t.Fatalf("edge %d-%d not symmetric", i, j)
			}
		}
	}
	// Connected: BFS from 0 reaches every node.
	seen := make([]bool, n)
	queue := []int{0}
	seen[0] = true
	count := 1
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, j := range adj[cur] {
			if !seen[j] {
				seen[j] = true
				count++
				queue = append(queue, j)
			}
		}
	}
	if count != n {
		t.Fatalf("graph not connected: reached %d of %d", count, n)
	}
}

func contains(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// Build test (milestone 2): hosts come up on a shared sim, PeerAddr resolves,
// supernode share is right, and Peers form a connected graph.
func TestBuild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 12
		nw, err := New(Config{N: n, P: 4, SuperFrac: 0.25, Seed: 1, MinLatency: testLo, MaxLatency: testHi})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(nw.Close)

		if nw.Len() != n {
			t.Fatalf("Len()=%d, want %d", nw.Len(), n)
		}
		supers := 0
		for i := range n {
			if nw.PeerAddr(i) == nil {
				t.Fatalf("PeerAddr(%d) is nil", i)
			}
			if nw.Host(i) == nil {
				t.Fatalf("Host(%d) is nil", i)
			}
			if len(nw.Peers(i)) == 0 {
				t.Fatalf("Peers(%d) empty", i)
			}
			if nw.IsSupernode(i) {
				supers++
			}
		}
		if supers != 3 { // round(0.25*12)
			t.Fatalf("got %d supernodes, want 3", supers)
		}
	})
}
