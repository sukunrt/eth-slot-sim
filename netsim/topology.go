package netsim

import (
	"encoding/json"
	"fmt"
	"os"
)

// Topology is the country-aware network description that simctl generates as
// topology.json and the Shadow backend consumes. Loading the *same* file here
// lets a simnet run and a Shadow run use an identical graph — same countries,
// per-edge latencies, bandwidths, and peer set — so their arrival CDFs compare
// like for like. It is plain data: the country model lives in the network
// layer, never in node/validator. Field tags match simctl/topology.py's output.
type Topology struct {
	Nodes []TopoNode `json:"nodes"`
	Edges []TopoEdge `json:"edges"`
}

// TopoNode is one node's placement: its bandwidth class and assigned country.
type TopoNode struct {
	Num          int    `json:"num"`
	UploadMbps   int    `json:"upload_bw_mbps"`
	DownloadMbps int    `json:"download_bw_mbps"`
	Country      string `json:"country"`
}

// TopoEdge is one directed link with its country-pair latency in milliseconds.
// topology.json carries both directions of every link (country latencies are
// asymmetric), so the per-direction value is honored as-is.
type TopoEdge struct {
	Source    int `json:"source"`
	Target    int `json:"target"`
	LatencyMs int `json:"latency_ms"`
}

// LoadTopology reads and parses a topology.json produced by simctl.
func LoadTopology(path string) (*Topology, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var t Topology
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &t, nil
}
