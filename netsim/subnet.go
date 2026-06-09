package netsim

import (
	"math/rand/v2"

	"github.com/marcopolo/simnet"

	"github.com/ethp2p/slot-sim/schedule"
)

// NewWithSchedule builds a discv5-biased network from a committee assignment: every node
// keeps ~K long-lived peers, biased so each subnet's subscribers form a connected subgraph
// (an attestation handed to a couple of them floods to all of them). The block topic rides
// the same graph. Latency and bandwidth come from cfg as in New; N is the assignment's.
// This is the in-process Go-test analogue of simctl/topology.py's builder — the two satisfy
// the same invariants (per-subnet connectivity, ~K degree) rather than being byte-identical
// (a cross-backend run shares one topology.json, so only one of them ever builds it).
func NewWithSchedule(a *schedule.Assignment, cfg Config) (*Netsim, error) {
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

// discv5Graph builds each node's long-lived peer set at target degree K: a global random
// spanning tree (keeps the block topic connected), then for each subnet a random spanning
// tree over its subscribers (so the subnet's subscribers are one connected piece), then
// random fill up to K. Subnet edges count toward K. K is clamped to n-1 and the fill is
// best-effort, so a small or dense graph degrades gracefully. Returns symmetric adjacency.
func discv5Graph(a *schedule.Assignment, k int, seed uint64) [][]int {
	n := a.Params.N
	g := newGraph(n)
	if n < 2 {
		return g.adj
	}
	if k > n-1 {
		k = n - 1
	}
	rng := rand.New(rand.NewPCG(seed, peersStream))
	g.randomTree(seq(n), rng) // global: keep the block topic connected
	for _, subs := range a.SubnetSubscribers {
		g.randomTree(subs, rng) // each subnet's subscribers: one connected piece
	}
	for _, subs := range a.ColumnSubscribers {
		g.randomTree(subs, rng) // each column's custodiers: one connected piece (the DA backbone)
	}
	g.fill(k, rng)
	return g.adj
}
