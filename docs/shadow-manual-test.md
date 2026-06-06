# Manual test: block dissemination on Shadow

The Shadow QUIC host plumbing and the end-to-end `shadow` run can't be exercised
in a Go/pytest unit (they need Shadow's network + virtual clock). This doc states
the **expected** behavior *before* that code is written; after implementing, run
it and tick each box (or record the deviation and fix).

Milestone under test: each slot one cyclic proposer publishes one sized block on
the global topic, and **every other node receives it**, running as N real
processes under Shadow.

## Prerequisites
- `shadow` 3.3.0 on `PATH` (installed locally).
- `uv` for the `simctl` CLI.

## Build & run
The Go binary is built automatically by `simctl run` (`go build ./cmd/slot-sim-node`).

```bash
cd /home/sukun/dev/eth-slot-sim
uv sync
# Inspect generated artifacts without running shadow:
uv run simctl run --config configs/smoke.yaml --output-dir runs/smoke --dry-run
# Real local run:
uv run simctl run --config configs/smoke.yaml --output-dir runs/smoke
# Verify receipt + arrival CDF:
python analysis/check_arrivals.py runs/smoke/run-*
```

`configs/smoke.yaml`: `num_nodes: 25`, `num_slots: 5`, `slot_duration_seconds:
12`, `degree: 8`, `super_node_fraction: 0.1`, `block_size: 131072`,
`verify_delay_ms: 10`, `jitter_ms: 2000`, `startup_seconds: 60`. So
`N = 25`, `slots = 5`, proposer of slot `k` is node `k % 25` (nodes 0..4).

## Expected results

### Generated artifacts (`--dry-run` and the run dir)
- [x] `shadow.yaml` has exactly 25 hosts `node0`..`node24`, each with
      `network_node_id: i` and `start_time: 0 sec`.
- [x] Each host runs `./slot-sim-node` with shared flags (`-num-nodes=25
      -num-slots=5 -slot=12s -block-size=131072 -verify-delay=10ms -D=8 -Dlo=6
      -Dhi=12 -startup=60s` …) plus per-host `-node-num=i` and a non-empty
      `-peer-nums=...`.
- [x] `network.use_shortest_path: true`; the inline GML has 25 nodes with
      `host_bandwidth_up/down` and one edge per topology edge (+ self-loops).
- [x] `general.stop_time` ≥ startup + slots·slot + drain (≈ 60 + 60 + drain).

### Run outcome
- [x] `shadow` exits 0 (`shadow.log` ends cleanly, no panics/fatal).
- [x] Each host dir `shadow.data/hosts/node{i}/` contains a
      `slot-sim-node.*.stdout` with JSON log lines.
- [x] A `publish` line appears on host `node{i}` **iff** `i ∈ {0,1,2,3,4}` (the
      slot proposers), exactly once each → 5 publish lines total across all hosts.
- [x] Every non-proposer logs an `arrival` for each of the 5 blocks.

### `check_arrivals.py`
- [x] Reports **exactly `num_slots*(N-1)` = 5·24 = 120 arrivals**.
- [x] **0 missing** (every non-proposer received every block) and **0
      duplicates** (`(node, slot, origin)` unique). This is the hard pass/fail.
- [x] Prints an arrival-delay CDF (p50/p90/p99/p100), monotonically
      non-decreasing, all positive.
- [x] CDF sits in a plausible band (sanity, not exact): with country latencies
      (tens–hundreds of ms/hop), a 10 ms verify hook per hop, and 128 KiB blocks
      over 25↑/50↓ Mbps links, expect p50 ≈ tens–few-hundred ms and p100 under a
      few seconds. Record actual numbers below.

## Actual results (2026-06-06, local Shadow 3.3.0)
- shadow exit code: **0**
- publish lines: **5** (only on hosts node0..node4, one each)
- arrivals: **120** (expected 120) ; missing: **0** ; duplicates: **0**
- CDF: p50=598.0ms p90=794.0ms p99=888.0ms p100=1166.0ms
- notes: timestamps use Shadow's virtual epoch (2000-01-01), confirming the
  shared-clock assumption. All expectations met on the first green run; the only
  fix needed was integer `stop_time` ("5 min", not "5.0 min" — Shadow rejects
  fractional minutes).
