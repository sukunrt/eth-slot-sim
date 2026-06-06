// Package netsim builds a simnet-backed network of libp2p hosts for the
// simulator: random per-link latency, per-node bandwidth classes, and a
// placeholder bounded-random peer graph. It is the in-process side of the
// Network seam (PeerAddr by node number); a realistic graph comes from Python
// later. No topology model lives here — just "give gossip a peer set."
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

// Bandwidth classes (bits/sec). Supernodes get ~1 Gbps (1024 Mbps); regular
// nodes are asymmetric home-staker links.
const (
	regularUp   = 25 * 1_000_000   // 25 Mbps
	regularDown = 50 * 1_000_000   // 50 Mbps
	superLink   = 1024 * 1_000_000 // 1024 Mbps (~1 Gbps)
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

// New builds N hosts on a shared simnet and starts it. Each host gets its own
// bandwidth class; the sim looks up each packet's latency from a precomputed
// per-link table.
func New(cfg Config) (*Netsim, error) {
	supers := pickSupernodes(cfg.N, cfg.SuperFrac, cfg.Seed)
	sim := &simnet.Simnet{LatencyFunc: latencyFunc(cfg.N, cfg.Seed, cfg.MinLatency, cfg.MaxLatency)}

	hosts := make([]host.Host, cfg.N)
	for i := range cfg.N {
		addr := fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", simnet.IntToPublicIPv4(i), listenPort)
		h, err := libp2p.New(
			libp2p.Identity(node.NodePrivateKey(i)),
			libp2p.ListenAddrStrings(addr),
			simlibp2p.QUICSimnet(sim, linkSettings(supers[i])),
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
	sim.Start()

	return &Netsim{sim: sim, hosts: hosts, peers: peerGraph(cfg.N, cfg.P, cfg.Seed), supers: supers}, nil
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
		return simnet.NodeBiDiLinkSettings{
			Downlink: simnet.LinkSettings{BitsPerSecond: superLink},
			Uplink:   simnet.LinkSettings{BitsPerSecond: superLink},
		}
	}
	return simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: regularDown},
		Uplink:   simnet.LinkSettings{BitsPerSecond: regularUp},
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
