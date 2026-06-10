//go:build simnetrun

// This file is compiled only with -tags simnetrun, so the normal `go test ./...`
// suite never builds or runs it. simctl sets the tag (and SIMRUN_PARAMS) to drive
// a simnet comparison run; see package doc.

package simnetrun

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
	"github.com/ethp2p/slot-sim/schedule"
)

// params mirrors the scenario knobs simctl passes a Shadow run (so the two
// backends run the same scenario), plus the topology input and CSV output paths.
// simctl writes it as JSON and points SIMRUN_PARAMS at it.
type params struct {
	Topology        string  `json:"topology"`
	CSV             string  `json:"csv"`
	LatencyMultiple float64 `json:"latency_multiple"`
	NumSlots        int     `json:"num_slots"`
	SlotSeconds     int     `json:"slot_seconds"`
	BlockSize       int     `json:"block_size"`
	VerifyMs        int     `json:"verify_ms"`
	OffsetMs        int     `json:"offset_ms"`
	JitterMs        int     `json:"jitter_ms"`
	D               int     `json:"d"`
	Dlo             int     `json:"dlo"`
	Dhi             int     `json:"dhi"`
	Seed            uint64  `json:"seed"`

	// Attestation phase (empty Schedule ⇒ block-only). Attest false with a Schedule set
	// keeps the proposer schedule but emits no attestations (block-only on the same network).
	Schedule string `json:"schedule"`
	Attest   bool   `json:"attest"`
	Sync     bool   `json:"sync"` // sync-committee phase (size/subnets/aggregators in schedule.json)
	// Decoupled-consensus phase (membership/voters in schedule.json). Forces attest/sync off.
	Decoupled        bool    `json:"decoupled"`
	K                int     `json:"k"`
	FCVoteOffsetMs   int     `json:"fc_vote_offset_ms"`
	FCAggFraction    int     `json:"fc_agg_fraction"`
	AttDueMs         int     `json:"att_due_ms"`
	AggDueMs         int     `json:"agg_due_ms"` // aggregate phase (0 ⇒ off)
	PrepMs           int     `json:"prep_ms"`
	AttVerifyMs      int     `json:"att_verify_ms"`
	AttPerItemMs     float64 `json:"att_per_item_ms"` // fractional: 0.1 ⇒ 100µs
	AttBatchWindowMs int     `json:"att_batch_window_ms"`
	AttBatchMax      int     `json:"att_batch_max"` // max attestations per batch; 0 = uncapped

	// Data-columns phase (active when schedule.json has num_columns > 0). The verifier P is
	// sized per node from its full-custody role in schedule.json.
	ColVerifyServiceMs int `json:"col_verify_service_ms"`
	ColVerifySuper     int `json:"col_verify_super"`
	ColVerifyReg       int `json:"col_verify_regular"`
}

// TestRun is the simnet backend: it runs the block-dissemination scenario over a
// simctl topology.json under synctest's virtual clock and writes the arrival CSV
// the comparison consumes. It is a test because synctest — the only clock under
// which simnet's timing is meaningful — runs only under `go test`. The simnetrun
// build tag keeps it out of the normal suite; SIMRUN_PARAMS supplies the run.
func TestRun(t *testing.T) {
	path := os.Getenv("SIMRUN_PARAMS")
	if path == "" {
		t.Skip("set SIMRUN_PARAMS=<params.json> to run the simnet backend")
	}
	p := loadParams(t, path)

	topo, err := netsim.LoadTopology(p.Topology)
	if err != nil {
		t.Fatalf("load topology %s: %v", p.Topology, err)
	}
	if p.LatencyMultiple != 0 && p.LatencyMultiple != 1 {
		for i := range topo.Edges {
			topo.Edges[i].LatencyMs = int(math.Round(float64(topo.Edges[i].LatencyMs) * p.LatencyMultiple))
		}
	}

	// rec is filled by bubble goroutines; read it only after the bubble exits, so
	// the CSV write stays outside synctest (no file I/O against the fake clock).
	rec := metrics.NewRecorder()
	synctest.Test(t, func(t *testing.T) {
		nw, err := netsim.NewFromTopology(topo)
		if err != nil {
			t.Fatalf("NewFromTopology: %v", err)
		}
		t.Cleanup(nw.Close)

		cfg := driver.Config{
			BlockSize:    p.BlockSize,
			SlotDuration: time.Duration(p.SlotSeconds) * time.Second,
			Offset:       time.Duration(p.OffsetMs) * time.Millisecond,
			Jitter:       time.Duration(p.JitterMs) * time.Millisecond,
			VerifyDelay:  func() time.Duration { return time.Duration(p.VerifyMs) * time.Millisecond },
			D:            p.D, Dlo: p.Dlo, Dhi: p.Dhi,
			Seed: p.Seed,
		}
		if p.Schedule != "" {
			a, err := schedule.Load(p.Schedule)
			if err != nil {
				t.Fatalf("load schedule %s: %v", p.Schedule, err)
			}
			cfg.Schedule = a
			cfg.Attest = p.Attest
			cfg.Sync = p.Sync
			if p.Decoupled { // replaces attestations + sync (driver.New forces those off)
				cfg.Decoupled = &driver.DecoupledParams{
					K:             p.K,
					FCVoteOffset:  time.Duration(p.FCVoteOffsetMs) * time.Millisecond,
					FCAggFraction: p.FCAggFraction,
				}
			}
			cfg.AttestationDue = time.Duration(p.AttDueMs) * time.Millisecond
			cfg.AggregateDue = time.Duration(p.AggDueMs) * time.Millisecond
			cfg.Prep = time.Duration(p.PrepMs) * time.Millisecond
			cfg.AttestVerifyDelay = func() time.Duration { return time.Duration(p.AttVerifyMs) * time.Millisecond }
			cfg.AttestPerItem = time.Duration(p.AttPerItemMs * float64(time.Millisecond))
			cfg.AttestBatchWindow = time.Duration(p.AttBatchWindowMs) * time.Millisecond
			cfg.AttestBatchMax = p.AttBatchMax
			if a.NumColumns > 0 { // size the per-node column verifier (P from full-custody role)
				cfg.ColVerifyService = func() time.Duration { return time.Duration(p.ColVerifyServiceMs) * time.Millisecond }
				cfg.ColVerifyParallelismSuper = p.ColVerifySuper
				cfg.ColVerifyParallelismReg = p.ColVerifyReg
			}
		}
		d := driver.New(nw, cfg, rec)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := d.BringUp(ctx); err != nil {
			t.Fatal(err)
		}
		d.Run(ctx, time.Now(), p.NumSlots)
	})

	writeCSV(t, p.CSV, rec)
}

func loadParams(t *testing.T, path string) params {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read params %s: %v", path, err)
	}
	var p params
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("parse params %s: %v", path, err)
	}
	return p
}

func writeCSV(t *testing.T, path string, rec *metrics.Recorder) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create csv %s: %v", path, err)
	}
	defer f.Close()
	if err := rec.WriteCSV(f); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	arr := rec.Arrivals()
	delays := make([]time.Duration, len(arr))
	for i, a := range arr {
		delays[i] = a.Delay
	}
	s := metrics.Summarize(delays)
	t.Logf("simnet (synctest) CDF over %d arrivals: p50=%v p90=%v p99=%v p100=%v",
		s.Count, s.P50, s.P90, s.P99, s.P100)
}
