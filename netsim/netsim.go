// Package netsim builds a simnet-backed network of libp2p hosts for the
// simulator and is the in-process side of the Network seam (PeerAddr by node
// number). It builds either from a country-aware topology.json shared with the
// Shadow backend (NewFromTopology — same countries, per-edge latencies,
// bandwidths, and peer set, so the two backends compare like for like) or from a
// self-contained random graph with uniform latency for quick local runs (New).
// The topology is pure network data: node and validator stay backend- and
// topology-agnostic.
package netsim

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/libp2p/go-libp2p/core/host"

	"github.com/ethp2p/slot-sim/node"
)

// Bandwidth classes in Mbps. Supernodes get ~1 Gbps; regular nodes are
// asymmetric home-staker links. These mirror simctl/topology.py's get_bandwidth
// so the random and topology paths place the same classes.
const (
	regularUpMbps   = 25
	regularDownMbps = 50
	superMbps       = 1024
)

const listenPort = 8000

// Distinct PCG stream IDs so latency, supernode, and peer-graph draws from the
// same Config.Seed don't correlate. The values are arbitrary, only distinct.
const (
	latencyStream = 1
	supersStream  = 2
	peersStream   = 3
)

// Config parameterizes a network. Requires MaxLatency >= MinLatency;
// MinLatency == MaxLatency yields a fixed (static) per-link latency.
type Config struct {
	N          int
	P          int     // target peers per node
	SuperFrac  float64 // fraction of nodes that are supernodes
	Seed       uint64
	MinLatency time.Duration
	MaxLatency time.Duration
}

// Netsim is a built network: hosts on one shared simnet, plus the peer graph.
type Netsim struct {
	sim    *simnet.Simnet
	hosts  []host.Host
	peers  [][]int
	supers map[int]bool
}

// New builds a self-contained random network: a bounded-degree peer graph,
// uniform per-link latency, and a seeded supernode share. For quick local runs
// and tests; NewFromTopology is the country-aware, Shadow-comparable path.
func New(cfg Config) (*Netsim, error) {
	supers := pickSupernodes(cfg.N, cfg.SuperFrac, cfg.Seed)
	sim := &simnet.Simnet{LatencyFunc: latencyFunc(cfg.N, cfg.Seed, cfg.MinLatency, cfg.MaxLatency)}
	hosts, err := buildHosts(sim, cfg.N, func(i int) simnet.NodeBiDiLinkSettings { return linkSettings(supers[i]) })
	if err != nil {
		return nil, err
	}
	sim.Start()
	return &Netsim{sim: sim, hosts: hosts, peers: peerGraph(cfg.N, cfg.P, cfg.Seed), supers: supers}, nil
}

// NewFromTopology builds a network from a simctl topology: per-node bandwidth,
// directed per-edge country latency, and the topology's edges as the peer
// graph. It consumes the same topology.json the Shadow backend does, so a
// simnet run and a Shadow run of one topology are directly comparable. Node
// numbers must form the contiguous range [0, len(Nodes)).
func NewFromTopology(topo *Topology) (*Netsim, error) {
	n := len(topo.Nodes)
	links := make([]simnet.NodeBiDiLinkSettings, n)
	placed := make([]bool, n)
	supers := make(map[int]bool, n)
	for _, nd := range topo.Nodes {
		if nd.Num < 0 || nd.Num >= n {
			return nil, fmt.Errorf("node num %d out of range [0,%d)", nd.Num, n)
		}
		if placed[nd.Num] {
			return nil, fmt.Errorf("duplicate node num %d", nd.Num)
		}
		placed[nd.Num] = true
		links[nd.Num] = bwLinkSettings(nd.UploadMbps, nd.DownloadMbps)
		if nd.UploadMbps >= superMbps {
			supers[nd.Num] = true
		}
	}
	sim := &simnet.Simnet{LatencyFunc: latencyFromEdges(topo.Edges)}
	hosts, err := buildHosts(sim, n, func(i int) simnet.NodeBiDiLinkSettings { return links[i] })
	if err != nil {
		return nil, err
	}
	sim.Start()
	return &Netsim{sim: sim, hosts: hosts, peers: adjacencyFromEdges(n, topo.Edges), supers: supers}, nil
}

// buildHosts creates n libp2p/QUIC hosts on sim, each with the link settings
// linkOf returns for it; on failure it closes the hosts already built. Host
// identity i comes from node.NodePrivateKey(i) so node.ConnectToPeers can
// resolve each peer's ID by number — the one node-package touch point, shared
// with the Shadow host. Nothing here depends on the validator or message types.
func buildHosts(sim *simnet.Simnet, n int, linkOf func(int) simnet.NodeBiDiLinkSettings) ([]host.Host, error) {
	hosts := make([]host.Host, n)
	for i := range n {
		addr := fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", simnet.IntToPublicIPv4(i), listenPort)
		h, err := libp2p.New(
			libp2p.Identity(node.NodePrivateKey(i)),
			libp2p.ListenAddrStrings(addr),
			simlibp2p.QUICSimnet(sim, linkOf(i)),
			libp2p.DisableIdentifyAddressDiscovery(),
			libp2p.ResourceManager(&libp2pnet.NullResourceManager{}),
		)
		if err != nil {
			for _, h := range hosts[:i] {
				_ = h.Close()
			}
			return nil, fmt.Errorf("build host %d: %w", i, err)
		}
		hosts[i] = h
	}
	return hosts, nil
}

// PeerAddr implements node.Network.
func (nw *Netsim) PeerAddr(n int) ma.Multiaddr { return nw.hosts[n].Addrs()[0] }

// Peers returns node n's undirected neighbor list (both endpoints hold the edge;
// node.ConnectToPeers dedups the dial direction).
func (nw *Netsim) Peers(n int) []int { return nw.peers[n] }

// Host returns node n's libp2p host.
func (nw *Netsim) Host(n int) host.Host { return nw.hosts[n] }

// IsSupernode reports whether node n is a supernode.
func (nw *Netsim) IsSupernode(n int) bool { return nw.supers[n] }

// Len is the node count.
func (nw *Netsim) Len() int { return len(nw.hosts) }

// Close tears down hosts and the sim.
func (nw *Netsim) Close() {
	for _, h := range nw.hosts {
		_ = h.Close()
	}
	nw.sim.Close()
}

func linkSettings(super bool) simnet.NodeBiDiLinkSettings {
	if super {
		return bwLinkSettings(superMbps, superMbps)
	}
	return bwLinkSettings(regularUpMbps, regularDownMbps)
}

// bwLinkSettings builds a bidirectional link from upload/download bandwidths in
// Mbps (simnet wants bits/sec).
func bwLinkSettings(upMbps, downMbps int) simnet.NodeBiDiLinkSettings {
	return simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: downMbps * 1_000_000},
		Uplink:   simnet.LinkSettings{BitsPerSecond: upMbps * 1_000_000},
	}
}

// latencyFunc precomputes a fixed latency for every node pair at startup (one
// seeded draw each), then returns a lock-free per-packet lookup — no rand or
// allocation on the routing hot path. Latency is symmetric (keyed by the
// unordered address pair) and identical for every packet on a link. lo == hi
// gives a fixed latency for all links.
func latencyFunc(n int, seed uint64, lo, hi time.Duration) func(*simnet.Packet) time.Duration {
	span := int64(hi - lo)
	lat := make(map[uint64]time.Duration, n*(n-1)/2)
	if span > 0 {
		rng := rand.New(rand.NewPCG(seed, latencyStream))
		for i := range n {
			for j := i + 1; j < n; j++ {
				lat[ipPairKey(simnet.IntToPublicIPv4(i), simnet.IntToPublicIPv4(j))] =
					lo + time.Duration(rng.Int64N(span))
			}
		}
	}
	return func(p *simnet.Packet) time.Duration {
		if d, ok := lat[ipPairKey(ipOf(p.From), ipOf(p.To))]; ok {
			return d
		}
		return lo
	}
}

// defaultLatency is used only if a packet ever flows on a pair with no topology
// edge — which shouldn't happen, since connections form only along edges.
const defaultLatency = 100 * time.Millisecond

// latencyFromEdges builds a directed per-link latency table from topology edges
// (country latencies are asymmetric, so the source→target direction is kept as
// is) and returns a lock-free per-packet lookup — no rand or allocation on the
// routing hot path. topology.json carries both directions of every link, so the
// fallback is a safety net, not an expected path.
func latencyFromEdges(edges []TopoEdge) func(*simnet.Packet) time.Duration {
	lat := make(map[uint64]time.Duration, len(edges))
	for _, e := range edges {
		k := ipDirKey(simnet.IntToPublicIPv4(e.Source), simnet.IntToPublicIPv4(e.Target))
		lat[k] = time.Duration(e.LatencyMs) * time.Millisecond
	}
	return func(p *simnet.Packet) time.Duration {
		if d, ok := lat[ipDirKey(ipOf(p.From), ipOf(p.To))]; ok {
			return d
		}
		return defaultLatency
	}
}

func ipOf(a net.Addr) net.IP {
	if u, ok := a.(*net.UDPAddr); ok {
		return u.IP
	}
	return nil
}

// ipPairKey canonicalizes an unordered IPv4 pair into a stable map key.
func ipPairKey(a, b net.IP) uint64 {
	a4, b4 := a.To4(), b.To4()
	if bytes.Compare(a4, b4) > 0 {
		a4, b4 = b4, a4
	}
	return uint64(binary.BigEndian.Uint32(a4))<<32 | uint64(binary.BigEndian.Uint32(b4))
}

// ipDirKey packs an ordered (from, to) IPv4 pair into a directed map key, so a
// link's two directions can hold different latencies.
func ipDirKey(from, to net.IP) uint64 {
	return uint64(binary.BigEndian.Uint32(from.To4()))<<32 | uint64(binary.BigEndian.Uint32(to.To4()))
}

// pickSupernodes returns exactly round(frac*n) supernode ids via a seeded
// shuffle — a pure function of (n, frac, seed).
func pickSupernodes(n int, frac float64, seed uint64) map[int]bool {
	k := int(frac*float64(n) + 0.5)
	r := rand.New(rand.NewPCG(seed, supersStream))
	perm := r.Perm(n)
	set := make(map[int]bool, k)
	for _, id := range perm[:k] {
		set[id] = true
	}
	return set
}

// peerGraph builds a connected, bounded-degree undirected graph: a ring backbone
// guarantees connectivity, then random chords bring each node up to ~p peers.
// Returns symmetric adjacency (each edge on both endpoints).
func peerGraph(n, p int, seed uint64) [][]int {
	adj := make([][]int, n)
	edge := make([]map[int]bool, n)
	for i := range n {
		edge[i] = make(map[int]bool)
	}
	add := func(i, j int) {
		if i == j || edge[i][j] {
			return
		}
		edge[i][j], edge[j][i] = true, true
		adj[i] = append(adj[i], j)
		adj[j] = append(adj[j], i)
	}
	if n < 2 {
		return adj
	}
	for i := range n { // ring backbone
		add(i, (i+1)%n)
	}
	r := rand.New(rand.NewPCG(seed, peersStream))
	for i := range n {
		for tries := 0; len(adj[i]) < p && tries < p*4; tries++ {
			add(i, r.IntN(n))
		}
	}
	return adj
}

// adjacencyFromEdges builds the undirected peer graph from topology edges: two
// nodes are peers if an edge joins them in either direction. Returns symmetric
// adjacency (each edge on both endpoints), as Peers requires.
func adjacencyFromEdges(n int, edges []TopoEdge) [][]int {
	seen := make([]map[int]bool, n)
	for i := range seen {
		seen[i] = make(map[int]bool)
	}
	adj := make([][]int, n)
	add := func(i, j int) {
		if i == j || seen[i][j] {
			return
		}
		seen[i][j] = true
		adj[i] = append(adj[i], j)
	}
	for _, e := range edges {
		add(e.Source, e.Target)
		add(e.Target, e.Source)
	}
	return adj
}
