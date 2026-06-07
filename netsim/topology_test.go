package netsim

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestLoadTopology(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topology.json")
	// Includes fanout_nodes (unknown to the struct) to prove it's ignored, so a
	// real simctl topology.json loads cleanly.
	const doc = `{
	  "nodes": [
	    {"num": 0, "upload_bw_mbps": 1024, "download_bw_mbps": 1024, "country": "germany"},
	    {"num": 1, "upload_bw_mbps": 25, "download_bw_mbps": 50, "country": "france"}
	  ],
	  "edges": [
	    {"source": 0, "target": 1, "latency_ms": 14},
	    {"source": 1, "target": 0, "latency_ms": 13}
	  ],
	  "fanout_nodes": []
	}`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	topo, err := LoadTopology(path)
	if err != nil {
		t.Fatalf("LoadTopology: %v", err)
	}
	if len(topo.Nodes) != 2 || len(topo.Edges) != 2 {
		t.Fatalf("got %d nodes / %d edges, want 2 / 2", len(topo.Nodes), len(topo.Edges))
	}
	if n := topo.Nodes[0]; n.Country != "germany" || n.UploadMbps != 1024 || n.DownloadMbps != 1024 {
		t.Fatalf("node0 = %+v", n)
	}
	if topo.Edges[0].LatencyMs != 14 || topo.Edges[1].LatencyMs != 13 {
		t.Fatalf("edge latencies = %d/%d, want 14/13", topo.Edges[0].LatencyMs, topo.Edges[1].LatencyMs)
	}
}

// latencyFromEdges keeps each direction's latency (country latencies are
// asymmetric); a pair with no edge falls back to the default.
func TestLatencyFromEdgesDirected(t *testing.T) {
	edges := []TopoEdge{
		{Source: 0, Target: 1, LatencyMs: 10},
		{Source: 1, Target: 0, LatencyMs: 25}, // asymmetric reverse
		{Source: 1, Target: 2, LatencyMs: 40},
	}
	f := latencyFromEdges(edges)
	cases := []struct {
		from, to int
		want     time.Duration
	}{
		{0, 1, 10 * time.Millisecond},
		{1, 0, 25 * time.Millisecond},
		{1, 2, 40 * time.Millisecond},
		{2, 1, defaultLatency}, // no edge 2->1
	}
	for _, c := range cases {
		if got := f(pkt(c.from, c.to)); got != c.want {
			t.Fatalf("%d->%d = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

// adjacencyFromEdges yields a symmetric, deduped peer graph regardless of edge
// direction or repeats.
func TestAdjacencyFromEdges(t *testing.T) {
	edges := []TopoEdge{
		{0, 1, 10}, {1, 0, 10}, // both directions
		{1, 2, 20},                         // one direction
		{2, 0, 30}, {0, 2, 30}, {0, 2, 30}, // with a duplicate
	}
	adj := adjacencyFromEdges(3, edges)
	want := map[int][]int{0: {1, 2}, 1: {0, 2}, 2: {0, 1}}
	for i := range 3 {
		got := slices.Sorted(slices.Values(adj[i]))
		if !slices.Equal(got, want[i]) {
			t.Fatalf("adj[%d] = %v, want %v", i, got, want[i])
		}
	}
}

// NewFromTopology builds hosts with the topology's bandwidth, peer set, and
// supernode classification (upload >= superMbps).
func TestNewFromTopology(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		topo := &Topology{
			Nodes: []TopoNode{
				{Num: 0, UploadMbps: 1024, DownloadMbps: 1024, Country: "germany"},
				{Num: 1, UploadMbps: 25, DownloadMbps: 50, Country: "france"},
				{Num: 2, UploadMbps: 25, DownloadMbps: 50, Country: "japan"},
			},
			Edges: []TopoEdge{
				{0, 1, 14}, {1, 0, 14},
				{1, 2, 220}, {2, 1, 210},
				{2, 0, 230}, {0, 2, 240},
			},
		}
		nw, err := NewFromTopology(topo)
		if err != nil {
			t.Fatalf("NewFromTopology: %v", err)
		}
		t.Cleanup(nw.Close)

		if nw.Len() != 3 {
			t.Fatalf("Len()=%d, want 3", nw.Len())
		}
		for i := range 3 {
			if nw.PeerAddr(i) == nil || nw.Host(i) == nil {
				t.Fatalf("node %d: PeerAddr/Host nil", i)
			}
			if len(nw.Peers(i)) != 2 {
				t.Fatalf("Peers(%d)=%v, want 2 (triangle)", i, nw.Peers(i))
			}
		}
		if !nw.IsSupernode(0) {
			t.Fatal("node 0 (1024 up) should be a supernode")
		}
		if nw.IsSupernode(1) || nw.IsSupernode(2) {
			t.Fatal("nodes 1,2 (25 up) should not be supernodes")
		}
	})
}

func TestNewFromTopologyRejectsBadNodeNum(t *testing.T) {
	topo := &Topology{Nodes: []TopoNode{{Num: 5, UploadMbps: 25, DownloadMbps: 50}}}
	if _, err := NewFromTopology(topo); err == nil {
		t.Fatal("expected error for out-of-range node num")
	}
}
