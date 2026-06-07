package netsim

import (
	"github.com/marcopolo/simnet"

	"github.com/ethp2p/slot-sim/committee"
)

// subnetReach bounds how many of a subnet's backbone subscribers a publisher/aggregator
// connects to: enough to seed fan-out into the subnet's (connected) subgraph, not all of
// them — so per-node degree stays bounded as the subscriber set grows with N.
const subnetReach = 16

// NewWithCommittee builds a subnet-aware network from a committee assignment: the
// raised-P random graph (the global block topic + base connectivity) augmented from the
// known subnet membership so that, by construction, each subnet's subscribers form a
// connected subgraph and every publisher/aggregator reaches them. This is the generator
// "playing discv5" — reachability comes from construction, not chance (§3). Latency and
// bandwidth come from cfg as in New; N is taken from the assignment.
func NewWithCommittee(a *committee.Assignment, cfg Config) (*Netsim, error) {
	n := a.Params.N
	supers := pickSupernodes(n, cfg.SuperFrac, cfg.Seed)
	sim := &simnet.Simnet{LatencyFunc: latencyFunc(n, cfg.Seed, cfg.MinLatency, cfg.MaxLatency)}
	hosts, err := buildHosts(sim, n, func(i int) simnet.NodeBiDiLinkSettings { return linkSettings(supers[i]) })
	if err != nil {
		return nil, err
	}
	sim.Start()
	return &Netsim{sim: sim, hosts: hosts, peers: subnetAwareGraph(a, cfg.P, cfg.Seed), supers: supers}, nil
}

// subnetAwareGraph overlays subnet connectivity onto the base random-P graph. For each
// subnet it (a) rings its backbone subscribers together so the mesh can form, and (b)
// links every node that publishes to or aggregates the subnet to a few of those
// subscribers so fan-out always has somewhere to send. Returns symmetric adjacency.
func subnetAwareGraph(a *committee.Assignment, p int, seed uint64) [][]int {
	n := a.Params.N
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

	for i, peers := range peerGraph(n, p, seed) { // base: global topic + connectivity
		for _, j := range peers {
			add(i, j)
		}
	}

	backboneSubs := make(map[int][]int)
	for node, subnets := range a.Backbone {
		for _, s := range subnets {
			backboneSubs[s] = append(backboneSubs[s], node)
		}
	}
	for _, subs := range backboneSubs { // (a) connect each subnet's subscribers in a ring
		for i := 1; i < len(subs); i++ {
			add(subs[i-1], subs[i])
		}
		if len(subs) > 2 {
			add(subs[len(subs)-1], subs[0])
		}
	}
	reach := func(node, subnet int) { // (b) link a publisher/aggregator to subscribers
		subs := backboneSubs[subnet]
		for i := 0; i < len(subs) && i < subnetReach; i++ {
			add(node, subs[i])
		}
	}
	for _, sp := range a.Slots {
		for _, com := range sp.Committees {
			for _, r := range com {
				reach(r.Node, r.Subnet)
			}
		}
		for _, aggs := range sp.Aggregators {
			for _, r := range aggs {
				reach(r.Node, r.Subnet)
			}
		}
	}
	return adj
}
