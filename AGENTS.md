# eth-slot-sim

Network simulator for Ethereum slot-level message dissemination: blocks, attestations +
aggregates, data columns (DA), sync committee, and the decoupled-consensus design
(availability chain + finality chain). The output of every run is arrival-delay data —
who received which message when — analyzed into CDFs/coverage by the Python side.

## Architecture (Go)

Layering, bottom-up. Each package has a doc comment worth reading; this is the map.

- `pb/` — generated protobuf wire messages. Regenerate: `go generate ./pb` (needs protoc;
  edit the `.proto`, never the `.pb.go`).
- `validator/` — the message-creating half: topic constants, `Make*` payload builders
  (size-realistic filler), per-slot proposer duties. One file per message family.
- `node/` — the passive half: libp2p host, gossipsub, join/subscribe/publish, receive →
  decode → `OnReceive`. **`node/registry.go` is the per-kind message table**: topic match,
  verify-queue routing, decode + identity/origin extraction. Adding a message kind =
  proto + `validator.Make*` + one registry entry (+ a Python analyzer). Verification is
  modeled as sleep: fixed per-hop (block), width-P semaphore (columns), batched
  single-server queues (floods).
- `schedule/` — Go consumer of `schedule.json`. **Generation lives in Python**
  (`simctl/schedule.py`); Go only reads it. This is the topology seam: validators → nodes →
  committees/subnets/custody/finality membership are all drawn once, in Python, so both
  backends run the identical plan.
- `metrics/` — `Tracer` (publish/receive events), `MsgID` = Kind + `node.Identity`
  (identity policy lives on the registry entries; `roundtrip_test.go` pins publish vs
  receive sides), in-memory `Recorder` for tests, `SlogTracer` for Shadow runs, CSV dump.
- `driver/` — orchestration: slot clock, `NodeRunner` per node (block→vote coupling, DA
  gate, deadlines, finality timers). The multi-node `Driver` runs N runners in-process;
  the Shadow binary runs exactly one.
- `netsim/` — in-process fabric (latency-shaped in-memory links) implementing
  `driver.Fabric`.
- `cmd/slot-sim-node/` — the one-process-per-node binary for Shadow runs; flags mirror the
  Python config.
- `simnetrun/` — sized in-process comparison run, build-tagged: compiled only with
  `-tags simnetrun` (driven by simctl, not by the normal test suite). Uses
  `testing/synctest`.

## Python side

- `simctl/` — the CLI (`uv run simctl ...`): pydantic config (`config.py`), schedule
  generation (`schedule.py`), Shadow topology (`topology.py`), local/remote run
  orchestration (`runner.py`, `remote.py`). Configs in `configs/*.yaml`.
- `analysis/check_arrivals.py` — parses Shadow slog output / simnet CSV into coverage +
  delay CDFs per message kind. **Contract with Go**: kind ints 1..9 and the MsgID field
  encoding (pinned by `node/registry_test.go` + `metrics/roundtrip_test.go`); don't
  renumber kinds or reshape the CSV/slog fields without updating this file.
- `analysis/to_parquet.py` + `analysis/duck_report.py` — the DuckDB fast path: convert a
  run's slog logs to parquet event tables once, then `check_arrivals.py <run> --parquet`
  analyzes them in SQL. check_arrivals stays the stdlib-only REFERENCE implementation;
  the two paths are pinned to identical reports by `tests/test_duck_report.py` — extend
  both together.
- `tests/` — pytest suite for the Python side.

## Two backends, one plan

1. **Simnet (in-process)**: `driver` + `netsim`, used by the Go tests (incl. e2e full-run
   tests in `driver/*_fullrun_test.go`) and by `simnetrun` for sized comparisons.
2. **Shadow**: `simctl run --config configs/<x>.yaml [--remote sukun@ethp2p]` builds
   `cmd/slot-sim-node`, generates `schedule.json` + Shadow topology, runs one process per
   node under Shadow's virtual clock, then analyzes. A `--remote` run is nohup'd on the
   remote and leaves two tarballs in `~/eth-slot-sim/runs/`: `<name>.tar.gz` (raw slog, the
   durable artifact) and `<name>-parquet.tar.gz` (small; pull this one and run
   `analysis/check_arrivals.py <run-dir> --parquet`).

Both read the same `schedule.json`, so results are comparable by construction.

## Commands

```bash
go test ./...        # full Go suite (e2e full-runs included, ~10s)
go vet ./...
uv run pytest        # Python suite
uv run simctl run --config configs/<x>.yaml   # Shadow run (add --remote sukun@ethp2p)
go generate ./pb     # after editing .proto files
```

## Conventions

- jj (Jujutsu) for VCS; atomic commits, linear history, `Assisted-By:` trailer.
- 100-char lines; `go vet` + modernize clean (generated `pb/*.pb.go` exempt).
- Comment style is deliberately dense: invariants and "why", at the declaration site.
- Feature phases are opt-in and layered: block-only → +columns → +attestations/aggregates →
  +sync → decoupled (which replaces attestation/sync emit). Mutual exclusion is enforced in
  `driver.New` and `cmd/slot-sim-node/main.go`.

The design/results markdowns (`*-spec.md`, `improvements.md`, `run.md`) were removed once
their content was implemented or superseded; see the repo history.
