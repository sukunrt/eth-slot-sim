package netsim

import (
	"math/rand/v2"

	"github.com/marcopolo/simnet"

	"github.com/ethp2p/slot-sim/committee"
)

// Distinct PCG streams (continuing netsim.go's set) so the subnet-mesh and fill draws
// don't correlate with latency/supernode/peer draws from the same seed.
const (
	subnetStream = 4
	fillStream   = 5
)

// subnetMeshDegree is how many subnet-mates each subscriber connects to within a subnet —
// ~the gossipsub mesh degree D, so the subscribers can form a working mesh.
const subnetMeshDegree = 8

// NewWithCommittee builds a discv5-biased network from a committee assignment: every node
// keeps ~P long-lived peers, biased so the subscribers of each subnet are connected to
// each other (a mesh) and the whole graph stays connected for the block topic. Publishers
// reach a subnet they don't subscribe by dialing its subscribers per slot (the driver),
// not via static edges here. Latency and bandwidth come from cfg as in New; N is the
// assignment's.
func NewWithCommittee(a *committee.Assignment, cfg Config) (*Netsim, error) {
	n := a.Params.N
	supers := pickSupernodes(n, cfg.SuperFrac, cfg.Seed)
	sim := &simnet.Simnet{LatencyFunc: latencyFunc(n, cfg.Seed, cfg.MinLatency, cfg.MaxLatency)}
	hosts, err := buildHosts(sim, n, func(i int) simnet.NodeBiDiLinkSettings { return linkSettings(supers[i]) })
	if err != nil {
		return nil, err
	}
	sim.Start()
	return &Netsim{sim: sim, hosts: hosts, peers: discv5Graph(a, cfg.P, cfg.Seed), supers: supers}, nil
}

// discv5Graph builds each node's ~p long-lived peers: a global ring (block-topic
// connectivity), then for each subnet a connected mesh over its subscribers (ring +
// random chords up to subnetMeshDegree), then random peers to fill each node toward p.
// p is clamped to n-1. Returns symmetric adjacency.
func discv5Graph(a *committee.Assignment, p int, seed uint64) [][]int {
	n := a.Params.N
	if p > n-1 {
		p = n - 1
	}
	adj := make([][]int, n)
	edge := make([]map[int]bool, n)
	for i := range n {
		edge[i] = make(map[int]bool)
	}
	add := func(i, j int) {
		if i == j || i < 0 || j < 0 || i >= n || j >= n || edge[i][j] {
			return
		}
		edge[i][j], edge[j][i] = true, true
		adj[i] = append(adj[i], j)
		adj[j] = append(adj[j], i)
	}
	if n < 2 {
		return adj
	}

	for i := range n { // global ring: keep the block topic connected
		add(i, (i+1)%n)
	}

	// Subnet meshes: each subnet's subscribers form a connected ~D-degree subgraph, so an
	// attestation handed to a couple of them spreads to all of them.
	srng := rand.New(rand.NewPCG(seed, subnetStream))
	for _, subs := range a.SubnetSubscribers {
		m := len(subs)
		for i := range m {
			add(subs[i], subs[(i+1)%m]) // ring → guaranteed connected
		}
		for _, u := range subs { // chords → ~subnetMeshDegree within the subnet
			for k := 0; k < subnetMeshDegree && k < m-1; k++ {
				add(u, subs[srng.IntN(m)])
			}
		}
	}

	// Random fill toward p for general connectivity (also carries the block topic).
	frng := rand.New(rand.NewPCG(seed, fillStream))
	for i := range n {
		for tries := 0; len(adj[i]) < p && tries < p*4; tries++ {
			add(i, frng.IntN(n))
		}
	}
	return adj
}
