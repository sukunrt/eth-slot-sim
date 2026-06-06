# Shadow vs simnet: the large-block arrival-delay discrepancy

Both backends run the **same** `topology.json` (same countries, per-edge
latencies, peer graph, bandwidth classes) behind the same node/gossipsub/validator
logic — see `current-state.md`. A side-by-side run therefore isolates whatever is
left. This documents a discrepancy that *looked* like a transport-model
difference but was mostly a **measurement artifact**, how it was found, and what
remains.

## TL;DR

The originally-reported gap — simnet's 128 KiB arrival CDF running **~45% higher**
than Shadow's, while a 1 KiB block agreed to ~5% — was **mostly an artifact of
running the simnet backend on the OS clock instead of under `testing/synctest`**.

simnet has no virtual clock of its own; it calls `time.Now()` / `time.NewTimer()`,
which are virtualized **only** inside a `go test` synctest bubble. The old
`./slot-sim` *binary* was not a test, so it ran on the real OS clock: every paced
packet and timer fire waited on the real goroutine scheduler, and that
scheduling latency — accumulated over the ~95 QUIC packets a 128 KiB block takes
per hop, across every relay hop — was recorded as "network" delay. A 1 KiB block
is ~1 packet/hop, so the artifact was invisible there (hence the ~5% agreement
that made the 128 KiB gap look like a real transport effect).

Running the **identical** simnet model under synctest's virtual clock collapses
the gap from ~45% to ~11%:

| 128 KiB, one topology | p50 | p90 | p99 |
|---|---:|---:|---:|
| Shadow (virtual clock) | 598 | 794 | 888 |
| simnet **OS-clock binary** *(old `./slot-sim`)* | 801–866 | 1110–1166 | 1231–1324 |
| simnet **synctest** (same code, virtual clock) | 642–673 | 886–924 | 1006–1023 |

The fix: the simnet backend is now a build-tagged synctest test (`simnetrun`,
`TestRun`) that `simctl` drives via `go test -tags simnetrun`. The OS-clock
`cmd/slot-sim` binary has been removed — it could never be correct, because Go
1.26 exposes synctest only as `synctest.Test(*testing.T, …)`, i.e. only under
`go test`.

The remaining **~11%** (synctest-simnet still above Shadow) is the genuinely open
question — a real per-host bandwidth / AQM model difference plus mesh-formation
noise — see *What's left*.

## How it was found

1. **Reproduce.** `simctl compare` reproduced 128 KiB ≈ +45%, 1 KiB ≈ +5%.
2. **Suspect the measurement, not the model.** The simnet backend (`./slot-sim`,
   run by `simctl compare`) is a plain binary on the OS clock; Shadow virtualizes
   the clock. So simnet's "delay" includes real scheduler latency, which grows
   with the per-block packet count — exactly the block-size dependence observed.
3. **Run the same model under synctest.** A probe ran the full driver over the
   *same* `topology.json` under `synctest.Test` (virtual clock). p50 dropped from
   ~801–866 to ~671 — most of the gap was the clock, not the transport.
4. **Rule out CPU starvation.** `GOMAXPROCS=1` gave p50 ≈ 816, essentially equal
   to `GOMAXPROCS=16`'s ≈ 801 — so it is timer / scheduler **wakeup** latency that
   synctest removes, not core contention.
5. **Last-slot / steady state.** Over a 10-slot run, the last slot's gap (+14%)
   matched the average (+11%): the residual is not a cold-start transient. At
   slot-scale spacing each QUIC connection idles between blocks and resets its
   congestion window, so every block is cold-start slow-start in **both**
   backends — cwnd ramp is symmetric and not the cause.

## What is shared, and what differs

Everything above the network seam is identical between the two runs — that is the
point of feeding both from one `topology.json`:

**Shared (verified equivalent):**
- Node→country assignment and per-edge **country-pair latency** (directed).
- The **peer graph** (adjacency) and per-node **bandwidth class** (25↑/50↓ Mbps
  regular, 1024/1024 super).
- Scenario knobs: block size, verify-delay (10 ms/hop, a gossipsub validator
  sleep), gossipsub D/Dlo/Dhi, offset, jitter, seed, slot count.
- The same go-libp2p host + go-libp2p-pubsub, with Prysm-tuned params.

**Differs (the seam):**
1. **Clock — this was the bug.** Shadow virtualizes the kernel clock. simnet is
   virtualized *only* under synctest; the old binary ran on the OS clock and so
   folded real scheduler latency into the measured delay. Both backends now run on
   a virtual clock (Shadow's, and simnet's via synctest), so they finally compare
   like for like.
2. **Transport implementation (the residual).**
   - *Shadow* runs real QUIC over Shadow's virtualized stack. Bandwidth is a
     **per-host rate limit** (`host_bandwidth_up/down`); latency comes from the
     GML graph with `use_shortest_path: true`.
   - *simnet* runs real QUIC over `marcopolo/simnet`'s in-memory transport.
     Bandwidth is **per host, per direction**: one uplink + one downlink driver
     per host (so all of a node's connections share its 25 Mbps uplink — the same
     per-host shape as Shadow, *not* per-link as an earlier draft of this doc
     claimed), each direction an **FQ-CoDel** queue (CoDel target 5 ms / interval
     100 ms). Latency comes from our directed per-edge table.
3. **Mesh-formation randomness (noise).** GRAFT/PRUNE choices are not seeded
   identically across two separate processes, so the dissemination tree differs
   run-to-run. This is the bulk of the ~5% residual seen at 1 KiB and a chunk of
   the per-slot variance at 128 KiB.

## What's left (open follow-up)

The synctest-vs-Shadow residual is ~11% at 128 KiB (and ~5% at 1 KiB, which is
essentially all mesh noise). To localize the 128 KiB part:

- **Fixed-latency / few-country topology.** Replace the country table with a
  uniform (or 2-country) latency so each hop's expected serialization + latency is
  analytically known; then per-hop divergence between the backends is readable
  directly instead of being smeared by 14 countries of latency spread.
- **Per-hop instrumentation.** An RPC/packet tracer measuring per-hop transfer
  time and CoDel drop counts on each backend (hop reconstruction per
  `scaling.md §9`) would say whether the residual is queueing delay, drops +
  backoff, or just the per-host token-bucket vs FQ-CoDel difference.
- **Block-size sweep.** Sweep 1 KiB → 128 KiB under the (now correct) synctest
  backend and plot Δ vs size; a residual that grows with serialized size points at
  the bandwidth/AQM model, one flat in size points at mesh noise.

## Reproduce

Prereqs: `shadow` 3.3.0 on `PATH`, `uv`, Go 1.26 (see `docs/shadow-manual-test.md`).

```bash
cd /home/sukun/dev/eth-slot-sim
uv sync

# 128 KiB block — both backends on one topology.json. simctl builds the Shadow
# hosts, runs Shadow, then runs the simnet backend under synctest
# (go test -tags simnetrun), and prints the side-by-side CDF (~11% now).
uv run simctl compare --config configs/smoke.yaml --output-dir runs/compare

# 1 KiB control (~5%, essentially mesh noise):
cat > /tmp/diag-smallblock.yaml <<'YAML'
topology: {num_nodes: 25, degree: 8, type: random, seed: 42, super_node_fraction: 0.1}
gossipsub: {D: 8, Dlow: 6, Dhigh: 12}
num_slots: 3
slot_duration_seconds: 12
block_size: 1024
verify_delay_ms: 10
offset_ms: 0
jitter_ms: 2000
startup_seconds: 60
stop_time_minutes: 5
log_level: info
seed: 1
YAML
uv run simctl compare --config /tmp/diag-smallblock.yaml --output-dir runs/diag
```

Each `compare` writes a `compare.json` and the simnet backend's
`simnet_arrivals.csv` (node,slot,origin,delay_ms) in the run dir; the Shadow
per-host logs are under `shadow.data/hosts/node*/`. Both carry the slot column,
so a per-slot (e.g. last-slot) CDF is a filter on each.

To run the simnet backend directly under synctest (what `simctl` invokes):

```bash
# SIMRUN_PARAMS points at a JSON with topology/csv paths + scenario knobs;
# simctl writes one as <run-dir>/simnet_params.json.
SIMRUN_PARAMS=<run-dir>/simnet_params.json \
  go test -tags simnetrun -run TestRun -count=1 ./simnetrun -v
```
