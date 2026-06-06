// Package netsim builds a simnet-backed network of libp2p hosts for the
// simulator: random per-link latency, per-node bandwidth classes, and a
// placeholder bounded-random peer graph. It is the in-process side of the
// Network seam (PeerAddr by node number); a realistic graph comes from Python
// later. No topology model lives here — just "give gossip a peer set."
package netsim

import (
	"bytes"
	"crypto/sha256"
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

// Bandwidth classes (bits/sec). Supernodes are effectively unlimited; regular
// nodes are asymmetric home-staker links. NOTE: this is a literal 1024 Gbps —
// not simlibp2p's 1024*OneMbps idiom, which is only ~1 Gbps.
const (
	regularUp   = 25 * 1_000_000
	regularDown = 50 * 1_000_000
	superLink   = 1024 * 1_000_000_000
)

const listenPort = 8000

// Config parameterizes a network. Zero MinLatency/MaxLatency are invalid.
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
// bandwidth class; the sim gets one latency function keyed on the link's
// address pair.
func New(cfg Config) (*Netsim, error) {
	supers := pickSupernodes(cfg.N, cfg.SuperFrac, cfg.Seed)
	sim := &simnet.Simnet{LatencyFunc: newLatencyFunc(cfg.Seed, cfg.MinLatency, cfg.MaxLatency)}

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

// newLatencyFunc returns simnet's per-packet latency function: a deterministic,
// symmetric random draw per link in [lo, hi).
func newLatencyFunc(seed uint64, lo, hi time.Duration) func(*simnet.Packet) time.Duration {
	return func(p *simnet.Packet) time.Duration {
		return pairLatency(seed, lo, hi, ipOf(p.From), ipOf(p.To))
	}
}

func ipOf(a net.Addr) net.IP {
	if u, ok := a.(*net.UDPAddr); ok {
		return u.IP
	}
	return nil
}

// pairLatency hashes the canonicalized (min,max) address pair so A→B and B→A
// agree, mapping to a stable value in [lo, hi). IntToPublicIPv4 isn't cleanly
// invertible, so we hash the address bytes rather than recover node numbers.
func pairLatency(seed uint64, lo, hi time.Duration, a, b net.IP) time.Duration {
	a4, b4 := a.To4(), b.To4()
	loIP, hiIP := a4, b4
	if bytes.Compare(a4, b4) > 0 {
		loIP, hiIP = b4, a4
	}
	span := uint64(hi - lo)
	if span == 0 {
		return lo // static latency
	}
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], seed)
	copy(buf[8:12], loIP)
	copy(buf[12:16], hiIP)
	sum := sha256.Sum256(buf[:])
	return lo + time.Duration(binary.BigEndian.Uint64(sum[:8])%span)
}

// pickSupernodes returns exactly round(frac*n) supernode ids via a seeded
// shuffle — a pure function of (n, frac, seed).
func pickSupernodes(n int, frac float64, seed uint64) map[int]bool {
	k := int(frac*float64(n) + 0.5)
	r := rand.New(rand.NewPCG(seed, 0x5314e7))
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
	r := rand.New(rand.NewPCG(seed, 0x9e3779b9))
	for i := range n {
		for tries := 0; len(adj[i]) < p && tries < p*4; tries++ {
			add(i, r.IntN(n))
		}
	}
	return adj
}
