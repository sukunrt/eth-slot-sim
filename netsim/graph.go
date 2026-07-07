package netsim

import "math/rand/v2"

// graph is an undirected simple-graph builder with O(1) duplicate-edge checks, shared by
// the random (peerGraph) and committee-driven (discv5Graph) peer-set builders. Edges are
// symmetric; self-loops, out-of-range endpoints, and duplicates are dropped.
type graph struct {
	n    int
	adj  [][]int
	edge []map[int]bool
}

func newGraph(n int) *graph {
	g := &graph{n: n, adj: make([][]int, n), edge: make([]map[int]bool, n)}
	for i := range n {
		g.edge[i] = make(map[int]bool)
	}
	return g
}

// add inserts the undirected edge (i,j) once.
func (g *graph) add(i, j int) {
	if i == j || i < 0 || j < 0 || i >= g.n || j >= g.n || g.edge[i][j] {
		return
	}
	g.edge[i][j], g.edge[j][i] = true, true
	g.adj[i] = append(g.adj[i], j)
	g.adj[j] = append(g.adj[j], i)
}

// randomTree links ids into one connected component with random edges: each id after the
// first attaches to a uniformly random earlier id. Fewer than two ids is a no-op
// (trivially connected). Same connectivity guarantee as a ring, but every edge is random.
func (g *graph) randomTree(ids []int, rng *rand.Rand) {
	for i := 1; i < len(ids); i++ {
		g.add(ids[i], ids[rng.IntN(i)])
	}
}

// fill tops every node up toward degree k with uniformly random peers. Retries are bounded
// so a too-small or already-dense graph degrades gracefully instead of spinning.
func (g *graph) fill(k int, rng *rand.Rand) {
	for i := range g.n {
		for tries := 0; len(g.adj[i]) < k && tries < k*4; tries++ {
			g.add(i, rng.IntN(g.n))
		}
	}
}

// groupFill tops every id up toward k neighbors WITHIN ids. Gossipsub meshes only form
// over existing links, so a flood-bearing membership group needs ~D internal degree per
// member — the tree alone leaves leaves at 1. Retries are bounded like fill's.
func (g *graph) groupFill(ids []int, k int, rng *rand.Rand) {
	in := make(map[int]bool, len(ids))
	for _, id := range ids {
		in[id] = true
	}
	target := min(k, len(ids)-1)
	for _, id := range ids {
		inGroup := 0
		for _, p := range g.adj[id] {
			if in[p] {
				inGroup++
			}
		}
		for tries := 0; inGroup < target && tries < k*4; tries++ {
			peer := ids[rng.IntN(len(ids))]
			if peer == id || g.edge[id][peer] {
				continue
			}
			g.add(id, peer)
			inGroup++
		}
	}
}

// seq returns the id list [0, 1, ..., n-1].
func seq(n int) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	return ids
}
