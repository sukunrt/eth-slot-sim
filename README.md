# eth-slot-sim

eth-slot-sim is a network simulator for Ethereum slot-level messages. It simulates blocks,
attestations, aggregates, data columns, sync-committee messages, and the decoupled-consensus
design. Each run makes arrival-delay data. The Python tools change this data into coverage
reports and delay CDFs.

## How the simulator operates

The simulator has two backends. Both backends read the same `schedule.json` file. Thus you can
compare their results directly.

1. **Simnet**. This backend runs all nodes in one Go process. The Go tests use it.
2. **Shadow**. This backend runs one process for each node under the Shadow virtual clock.
   The `simctl` tool builds, runs, and analyzes each Shadow run.

## Structure of the repository

- Go packages: `pb` (wire messages), `validator` (message builders), `node` (libp2p and
  gossipsub), `schedule` (plan reader), `metrics` (event tracing), `driver` (slot clock and
  node runner), `netsim` (in-process network), `cmd/slot-sim-node` (Shadow binary).
- Python: `simctl` (command-line tool), `analysis` (report tools), `configs` (run
  configurations), `tests` (pytest suite).

See `AGENTS.md` for the full package map.

## Before you start

Make sure that you have Go and `uv`. For Shadow runs, you must also have Shadow.

## Run the tests

1. Run the Go tests: `go test ./...`
2. Run the Python tests: `uv run pytest`

## Configuration

A run configuration is one YAML file. The schema is in `simctl/config.py`. The schema rejects
unknown keys. The top-level sections are:

- `topology` — node count, peer degree, seed, latency, supernode fraction.
- `gossipsub` — mesh parameters D, Dlow, Dhigh.
- `attestation` — validator count, committees, subnets, deadlines, and the transport
  (`classic` or `partial`).
- `data_columns` — column count, blob count, custody, and verify parallelism.
- `sync` — sync-committee size, subnets, and aggregators.
- `decoupled_consensus` — availability votes, finality subnets, aggregators, and validator
  segregation. This phase replaces attestations and sync.
- `epbs` — the two-phase block send and the PTC. It is on by default.
- `validator_distribution` — how validators map to nodes: `uniform`, `tiered`, or `explicit`.
- Top-level values — slot count, slot length, block size, jitter, and seeds.

Each phase section is optional. A phase runs only when its section is present and enabled.
See `configs/` for examples, from `smoke.yaml` to the large `decoupled_n4000_*` runs.

## Do a Shadow run

1. Select a configuration file from `configs/`.
2. Run this command: `uv run simctl run --config configs/<x>.yaml`
3. To run on the remote host, add `--remote sukun@ethp2p`.

A remote run writes two tarballs to `~/eth-slot-sim/runs/` on the remote host. Pull the
`<name>-parquet.tar.gz` file for analysis.

## Analyze a run

Run this command: `uv run python analysis/check_arrivals.py <run-dir>`

The tool prints coverage and delay CDFs for each message kind.
