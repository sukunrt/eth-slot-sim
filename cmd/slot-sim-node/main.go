// Command slot-sim-node is one beacon node for a Shadow run: a single real
// libp2p/QUIC host that connects to a Python-supplied peer list, joins the
// global block topic, and runs the cyclic-proposer slot loop. Shadow launches N
// of these (one per host); all share Shadow's virtual clock, so each node logs
// publish/arrival events as JSON on stdout with absolute timestamps and the
// records are reassembled afterward by (slot, origin). The node logic is the
// same as simnet — only the network backend (this Shadow host + DNS peer
// resolution) differs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
	"go.uber.org/fx"

	"github.com/ethp2p/slot-sim/committee"
	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/validator"
)

const (
	listenPort = 8000
	// meshJoinStagger bounds the random pause between dialing peers and joining the
	// global topic, so a large fleet doesn't GRAFT in lockstep (mirrors
	// batched-attestation-sim). The -startup settle window must exceed it.
	meshJoinStagger = 30 * time.Second
	// drainWindow keeps the receive loops alive after the final slot to capture a
	// large block's dissemination tail — a 1 MiB block can outlast a 12s slot.
	drainWindow = 30 * time.Second
)

// settleWindow is how long a worst-case-staggered host still has to mesh before
// slot 0: the startup window minus the maximum join stagger. It must stay > 0,
// else a late joiner would publish before its mesh forms.
func settleWindow(startup, stagger time.Duration) time.Duration { return startup - stagger }

func main() {
	// Capture the start instant first: runStart = programStart + startup must be
	// identical on every host, which holds because Shadow starts all hosts at
	// sim-time 0 (so time.Now() here is the same on each).
	programStart := time.Now()

	var (
		nodeNum     = flag.Int("node-num", 0, "this node's number")
		numNodes    = flag.Int("num-nodes", 1, "total nodes (for the cyclic proposer)")
		peerNumsStr = flag.String("peer-nums", "", "comma-separated peer node numbers to dial")
		numSlots    = flag.Int("num-slots", 1, "slots to run")
		slotDur     = flag.Duration("slot", 12*time.Second, "slot duration")
		blockSize   = flag.Int("block-size", 128*1024, "block size in bytes")
		verifyDelay = flag.Duration("verify-delay", 10*time.Millisecond, "per-hop verify-hook delay")
		offset      = flag.Duration("offset", 0, "proposer publish offset into the slot")
		jitter      = flag.Duration("jitter", 2*time.Second, "proposer publish jitter: offset + rand(0,jitter)")
		d           = flag.Int("D", 8, "gossipsub D")
		dlo         = flag.Int("Dlo", 6, "gossipsub Dlo")
		dhi         = flag.Int("Dhi", 12, "gossipsub Dhi")
		seed        = flag.Uint64("seed", 1, "validator rng seed (combined with node-num)")
		startup     = flag.Duration("startup", 60*time.Second, "bring-up window before slot 0")

		committeePath = flag.String("committee", "", "path to committee.json (empty → block-only)")
		attestations  = flag.Bool("attestations", true, "emit attestations (false → block-only; committee still sets the proposer schedule)")
		attDue        = flag.Duration("att-due", 4*time.Second, "attestation deadline offset into the slot")
		aggDue        = flag.Duration("agg-due", 0, "aggregate emit offset into the slot (0 ⇒ aggregates off)")
		prep          = flag.Duration("prep", 0, "extra processing before emitting on block receipt")
		attestVerify  = flag.Duration("attest-verify-delay", 10*time.Millisecond, "attestation batch base verify delay")
		attestPerItem = flag.Duration("attest-per-item", 0, "attestation per-item verify cost")
		attestWindow  = flag.Duration("attest-batch-window", 50*time.Millisecond, "attestation batch window")
		rpcLogNode    = flag.Int("rpc-log-node", -1, "node-num to enable gossipsub debug RPC logging on (-1 = off)")

		// Data columns are driven by committee.json (num_columns/column_subscribers/full_custody);
		// these size the per-node width-P column verifier.
		colVerify      = flag.Duration("col-verify-service", 3*time.Millisecond, "per-column verify delay")
		colVerifySuper = flag.Int("col-verify-super", 16, "column verify parallelism P for a full-custody node")
		colVerifyReg   = flag.Int("col-verify-regular", 4, "column verify parallelism P for an ordinary node")
	)
	flag.Parse()
	if settleWindow(*startup, meshJoinStagger) <= 0 {
		log.Fatalf("startup %v must exceed mesh-join stagger %v", *startup, meshJoinStagger)
	}

	tracer := metrics.NewSlogTracer(slog.NewJSONHandler(os.Stdout, nil))
	var comm *committee.Assignment
	var proposers []int // supernode proposer schedule; nil ⇒ cyclic (block-only)
	if *committeePath != "" {
		c, err := committee.Load(*committeePath)
		if err != nil {
			log.Fatalf("load committee %s: %v", *committeePath, err)
		}
		proposers = c.ProposerSchedule() // supernode block schedule (used even when block-only)
		if *attestations || c.NumColumns > 0 {
			comm = c // drives attestations and/or columns; -attestations gates the votes
		}
	}
	val := validator.New(*nodeNum, *numNodes, *blockSize, *offset, *jitter,
		rand.New(rand.NewPCG(*seed, uint64(*nodeNum))), proposers)
	nd := &node.Node{
		Num: *nodeNum, Host: newShadowHost(*nodeNum), Network: &shadowNetwork{},
		VerifyDelay:       func() time.Duration { return *verifyDelay },
		AttestVerifyDelay: func() time.Duration { return *attestVerify },
		AttestPerItem:     *attestPerItem, AttestBatchWindow: *attestWindow,
		D: *d, Dlo: *dlo, Dhi: *dhi,
	}
	if comm != nil && comm.NumColumns > 0 { // size the column verifier from this node's role
		nd.ColVerifyService = func() time.Duration { return *colVerify }
		nd.ColVerifyParallelism = *colVerifyReg
		if comm.Node(*nodeNum).IsFullCustody() {
			nd.ColVerifyParallelism = *colVerifySuper
		}
	}
	if *rpcLogNode == *nodeNum {
		nd.RPCLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	peers := parseIntList(*peerNumsStr)
	runner := driver.NewRunner(*nodeNum, nd, val, comm, *attestations, tracer, *slotDur, *attDue, *aggDue, *prep, *seed, peers)
	runner.Attach() // sets nd.OnReceive before JoinTopics

	ctx := context.Background()
	if err := nd.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}
	// Bring-up cadence (mirrors batched-attestation-sim — nicer at scale): a small
	// jitter before dialing and a larger random pause before joining, so a large
	// fleet doesn't dial then GRAFT in lockstep. The -startup window then lets
	// every host settle and mesh before slot 0.
	time.Sleep(rand.N(time.Second))
	nd.ConnectToPeers(peers)
	slog.Info("peers connected", "node", *nodeNum)
	time.Sleep(rand.N(meshJoinStagger))
	if err := nd.JoinTopics(ctx); err != nil {
		log.Fatalf("join topics: %v", err)
	}
	runner.Prepare() // subscribe this node's own subnets, before the settle

	runStart := programStart.Add(*startup)
	time.Sleep(time.Until(runStart)) // chillax until slot 0 — every host has meshed
	runner.Run(ctx, runStart, *numSlots)
	time.Sleep(drainWindow) // capture the last block's dissemination tail
	nd.Close()
}

// parseIntList parses "1,3,7" into []int, tolerating whitespace and empty
// fields. Empty input returns nil.
func parseIntList(s string) []int {
	var out []int
	for f := range strings.SplitSeq(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		n, err := strconv.Atoi(f)
		if err != nil {
			log.Fatalf("invalid integer %q: %v", f, err)
		}
		out = append(out, n)
	}
	return out
}

// shadowNetwork implements node.Network for Shadow: peer addresses are resolved
// by the "node<N>" hostname convention via DNS.
type shadowNetwork struct{}

func (s *shadowNetwork) PeerAddr(nodeNum int) ma.Multiaddr {
	hostname := fmt.Sprintf("node%d", nodeNum)
	addrs, err := net.LookupHost(hostname)
	if err != nil || len(addrs) == 0 {
		log.Fatalf("resolve %s: %v", hostname, err)
	}
	maddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", addrs[0], listenPort))
	if err != nil {
		log.Fatalf("multiaddr for %s: %v", hostname, err)
	}
	return maddr
}

// newShadowHost builds a real libp2p/QUIC host bound to this host's IP, with the
// Shadow-specific packet-conn plumbing (serialized writes + source-IP override).
func newShadowHost(nodeNum int) host.Host {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: listenPort})
	if err != nil {
		log.Fatalf("listen udp: %v", err)
	}
	sconn := newShadowUDPConn(conn)
	maddr := ma.StringCast(fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", listenPort))
	h, err := libp2p.New(
		quicWithPacketConn(sconn),
		libp2p.Identity(node.NodePrivateKey(nodeNum)),
		libp2p.ListenAddrs(maddr),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.ResourceManager(&libp2pnet.NullResourceManager{}),
		libp2p.ConnectionManager(&connmgr.NullConnMgr{}),
		libp2p.ForceReachabilityPublic(),
	)
	if err != nil {
		log.Fatalf("create host: %v", err)
	}
	return h
}

// shadowUDPConn serializes UDP writes, which Shadow's network stack requires.
type shadowUDPConn struct {
	net.PacketConn
	ch chan struct{}
}

func newShadowUDPConn(pc net.PacketConn) *shadowUDPConn {
	return &shadowUDPConn{PacketConn: pc, ch: make(chan struct{}, 1)}
}

func (s *shadowUDPConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	s.ch <- struct{}{}
	defer func() { <-s.ch }()
	return s.PacketConn.WriteTo(p, addr)
}

// sourceIPSelector pins QUIC's source IP to this host's address under Shadow.
type sourceIPSelector struct {
	ip atomic.Pointer[net.IP]
}

func (m *sourceIPSelector) PreferredSourceIPForDestination(_ *net.UDPAddr) (net.IP, error) {
	return *m.ip.Load(), nil
}

// quicWithPacketConn wires the pre-created Shadow packet conn into libp2p's QUIC
// reuse manager (Shadow won't let libp2p open its own UDP socket).
func quicWithPacketConn(conn net.PacketConn) libp2p.Option {
	ca := conn.LocalAddr().(*net.UDPAddr)
	sel := &sourceIPSelector{}
	sel.ip.Store(&ca.IP)
	reuseOpts := []quicreuse.Option{
		quicreuse.OverrideSourceIPSelector(func() (quicreuse.SourceIPSelector, error) {
			return sel, nil
		}),
		quicreuse.OverrideListenUDP(func(_ string, address *net.UDPAddr) (net.PacketConn, error) {
			if ca.IP.Equal(address.IP) && ca.Port == address.Port {
				return conn, nil
			}
			return nil, fmt.Errorf("invalid listen address: %s, wanted: %s", address, ca)
		}),
	}
	return libp2p.QUICReuse(
		func(l fx.Lifecycle, resetKey quic.StatelessResetKey, tokenKey quic.TokenGeneratorKey, opts ...quicreuse.Option) (*quicreuse.ConnManager, error) {
			cm, err := quicreuse.NewConnManager(resetKey, tokenKey, opts...)
			if err != nil {
				return nil, err
			}
			l.Append(fx.StopHook(func() error { return cm.Close() }))
			return cm, nil
		}, reuseOpts...)
}
