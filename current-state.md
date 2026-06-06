# Current state & next steps

Status snapshot for the slot simulator. See `plan.md` (architecture/build order),
`phase1-spec.md` (the block milestone), `slot-messages.md`/`scaling.md` (the
target model), and `docs/shadow-manual-test.md` (the Shadow run + its results).

## Where we are

**Phase 1 — block dissemination — runs on BOTH backends behind the same node
logic.** Each slot one cyclic proposer (`slot % N`) publishes one sized,
incompressible block on the global gossipsub topic; every other node receives and
relays it; arrival time is recorded.

- **simnet** — `simnetrun.TestRun`: one process builds all N nodes on
  `marcopolo/simnet` (in-memory QUIC) and runs the driver under
  `testing/synctest`'s **virtual clock** — the only clock under which simnet's
  timing is valid (a plain binary on the OS clock measures goroutine-scheduler
  latency, not the network; that was the old 45% red herring). Build-tagged
  (`-tags simnetrun`) and driven by `simctl` via `go test`. It consumes the
  *same* country-aware `topology.json` Shadow does (per-node bandwidth, directed
  per-edge country latency, the topology's peer graph) and writes the arrival
  CSV. The `testing/synctest` unit tests cover the same stack with assertions;
  `netsim.New` (random graph + uniform latency) lives there for quick tests.
- **Shadow** — `cmd/slot-sim-node`: one real libp2p/QUIC process **per node**,
  N of them launched by Shadow. Orchestrated by the `simctl` Python CLI (local).
  Verified locally (Shadow 3.3.0): 25 nodes / 5 slots → **120/120 arrivals, 0
  missing, 0 duplicates**, CDF p50=598ms … p100=1166ms.

### The one seam
`node.Network` (`PeerAddr(nodeNum) → multiaddr`) is the only thing that differs
between backends. `netsim` implements it in-process; `shadowNetwork` (in
`cmd/slot-sim-node`) resolves the `node<N>` hostname via DNS. **Node, Validator,
gossipsub params, and the slot loop are identical on both.**

### Shared orchestration (netsim-free, used by both)
- `driver.SlotLoop(ctx, node, validator, tracer, runStart, numSlots, slotDur)` —
  one node's per-slot publish loop.
- `driver.RouteReceived(num, received, tracer)` — receipt → tracer, skipping the
  node's own loopback (origin == self).
- `driver.Driver` runs N `SlotLoop`s for simnet over a `driver.Fabric` (which
  `*netsim.Netsim` satisfies); the Shadow `main` runs exactly one.

### Metrics
- `metrics.Recorder` — in-memory, for simnet/tests (CDF + CSV).
- `metrics.SlogTracer` — slog JSON lines to stdout, for Shadow (all hosts share
  Shadow's clock, so absolute `t_ns` is comparable across processes).
- `analysis/check_arrivals.py` — joins per-host stdout logs by `(slot, origin)`,
  confirms full receipt, prints the arrival CDF.

### Topology (shared by both backends)
Country-realistic latency model from `../batched-attestation-sim`
(`simctl/topology.py` + `data/country_latencies.json` / `country_weights.json`).
`simctl/runner.py` generates one `topology.json` (nodes: bandwidth + country;
edges: country-pair latency) that is the **single source of truth**: Shadow
consumes it via the GML network (per-node bandwidth + self-loops + per-edge
latency) + `shadow.yaml`, and simnet consumes it via `netsim.LoadTopology` /
`NewFromTopology`. The country model lives entirely in the **network layer** —
`node`/`validator` never see countries, latencies, or the topology.

### Cross-backend comparison
`simctl compare --config <c> --output-dir <d>` runs one `topology.json` on both
backends and prints a side-by-side arrival CDF (also `compare.json`). Percentiles
come from one shared helper (`analysis/check_arrivals.cdf`) so the two are
summarized identically. **Finding (smoke, 25 nodes):** full receipt on both; at
1 KiB the CDFs agree within ~5% (mesh noise). A previously-reported ~45% gap at
128 KiB was mostly a **measurement artifact** — the simnet backend used to run as
an OS-clock binary, folding goroutine-scheduler latency (≈95 packets/hop) into the
delay; running the same model under synctest drops the gap to ~11%. The residual
is an open transport-model question. Full write-up in `shadow-simnet.md`.

## Key files
```
simnetrun/run_test.go        simnet backend: TestRun runs the driver under synctest (-tags simnetrun)
cmd/slot-sim-node/main.go    single Shadow node (QUIC plumbing + DNS network)
driver/driver.go             Fabric, Driver, SlotLoop, RouteReceived
node/                        passive Node (host, gossipsub, send/recv) — backend-agnostic
validator/                   Duty + makeBlock
metrics/tracer.go            Recorder (simnet) + SlogTracer (shadow)
netsim/netsim.go             simnet hosts, peer graph, Network impl (New + NewFromTopology)
netsim/topology.go           Topology types + LoadTopology (consumes simctl topology.json)
simctl/                      config.py, topology.py, runner.py, main.py (run + compare), manifest.py
analysis/check_arrivals.py   receipt check + CDF (cdf/delays_from_csv reused by compare)
configs/smoke.yaml           25-node / 5-slot local Shadow smoke run
docs/shadow-manual-test.md   expected vs actual for the Shadow run
```

## How to run
```bash
# Shadow (local): build is automatic
uv sync
uv run simctl run --config configs/smoke.yaml --output-dir runs/smoke
python analysis/check_arrivals.py runs/smoke/run-*

# Same topology on BOTH backends (simnet runs under synctest), side-by-side CDF
uv run simctl compare --config configs/smoke.yaml --output-dir runs/compare

# simnet backend alone (what compare invokes): synctest harness via go test
SIMRUN_PARAMS=runs/compare/run-*/simnet_params.json \
  go test -tags simnetrun -run TestRun -count=1 ./simnetrun -v

# tests
go test ./...        # incl. synctest suites
uv run pytest        # topology / config / runner / check_arrivals
```

## Gotchas / things to watch
- **Shadow rejects fractional `stop_time`** — emit whole minutes ("5 min", not
  "5.0 min"). Handled in `runner._stop_time_minutes`.
- **Connect race**: every host starts at sim-time 0; `node.ConnectToPeers` does a
  single `ctx.Background()` dial and relies on QUIC handshake retransmission +
  the `-startup` window (default 60s) to absorb start skew. Fine at N=25; if edges
  drop at larger N, add a bounded retry in `node.ConnectToPeers` (harmless for
  simnet).
- **Clock alignment**: `runStart = programStart + startup`, with `programStart`
  captured as the first statement in `main`, so every host agrees on slot 0. Holds
  only while hosts start at `start_time: 0 sec`.
- Each Shadow run dir contains a ~35 MB Go binary + `shadow.data/`; `runs/` is
  gitignored.

## Next steps (rough order)
1. **Explain the residual ~11% (128 KiB).** With the simnet backend now under
   synctest, `compare` shows ~11% (not ~45%) at 128 KiB and ~5% at 1 KiB.
   Localize the 128 KiB residual with a fixed-latency / few-country topology and
   per-hop tracing (Shadow per-host token bucket vs simnet per-host FQ-CoDel),
   plus a block-size sweep. See `shadow-simnet.md` *What's left*.
2. **simctl `--remote`.** Port `remote.py` from `../batched-attestation-sim`
   (rsync → build on remote → run → tarball). Mirrors the other simctl skills.
3. **Scale-out (plan.md Phase 4).** Climb N under Shadow; watch the connect
   race/startup window; find the ceiling. Add CDF/percentile tooling for sweeps.
4. **More messages (plan.md Phase 1 steps 4-6 → Phase 2/5).** Attestations
   (validator gains an attest duty + `makeAttestation`; the node loop is
   unchanged), then aggregates, data columns, then gloas.
5. **Nice-to-haves.** An `eth-slot-sim-simctl` skill; richer metrics (hop
   reconstruction via an RPC tracer, per `scaling.md` §9).
