"""Convert a Shadow run's slog stdout logs into parquet event tables.

A Shadow run leaves one JSON-lines stdout file per host (see metrics.SlogTracer for the
schema). Re-parsing tens of GB of JSON on every analysis pass is the bottleneck, so this
converts a run dir ONCE into <run-dir>/parquet/:

  arrivals.parquet   node, kind, slot, subnet, attester, origin, t_ns
  publishes.parquet  kind, slot, subnet, attester, origin, voted_block, t_ns
  meta.json          {"nodes": [...]} — every host dir, so a node that received NOTHING
                     still counts in coverage checks (it appears in no parquet row)

The conversion is additive: raw logs are never touched, and re-running is a no-op unless
--force. DuckDB does the heavy lifting (parallel NDJSON scan over the host glob); non-event
lines (RPC text logs, startup noise) are skipped by ignore_errors + the msg filter. Rows
are sorted by (kind, slot, subnet, attester) so per-kind analysis queries prune row groups.

Usage: uv run python analysis/to_parquet.py <run-dir> [--force]
"""

import argparse
import json
import re
import sys
from pathlib import Path

import duckdb

# Every field either event carries; absent fields read as NULL (e.g. voted_block on
# arrivals). slog's own time/level fields are simply not extracted.
_COLUMNS = (
    "{msg: 'VARCHAR', node: 'BIGINT', kind: 'BIGINT', slot: 'BIGINT', subnet: 'BIGINT', "
    "attester: 'BIGINT', origin: 'BIGINT', voted_block: 'BOOLEAN', t_ns: 'BIGINT'}"
)


def convert(run_dir: Path, force: bool = False) -> Path:
    """Convert run_dir's slog stdout files into run_dir/parquet/; returns the parquet dir.
    Idempotent: skips work if the outputs already exist (unless force)."""
    hosts_dir = run_dir / "shadow.data" / "hosts"
    if not hosts_dir.is_dir():
        raise FileNotFoundError(f"no shadow.data/hosts under {run_dir}")
    out_dir = run_dir / "parquet"
    outputs = [out_dir / "arrivals.parquet", out_dir / "publishes.parquet", out_dir / "meta.json"]
    if not force and all(p.exists() for p in outputs):
        return out_dir

    nodes = sorted(
        int(m.group(1))
        for host in hosts_dir.iterdir()
        if (m := re.fullmatch(r"node(\d+)", host.name))
    )
    if not nodes:
        raise FileNotFoundError(f"no node* host dirs under {hosts_dir}")

    out_dir.mkdir(exist_ok=True)
    glob = str(hosts_dir / "node*" / "slot-sim-node.*.stdout")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")  # cuts sort memory on big runs
    src = f"read_ndjson('{glob}', columns={_COLUMNS}, ignore_errors=true)"
    con.execute(
        f"""COPY (SELECT node, kind, slot, subnet, attester, origin, t_ns
                  FROM {src}
                  WHERE msg = 'arrival' AND t_ns IS NOT NULL
                  ORDER BY kind, slot, subnet, attester)
            TO '{out_dir / "arrivals.parquet"}' (FORMAT parquet, COMPRESSION zstd)"""
    )
    con.execute(
        f"""COPY (SELECT kind, slot, subnet, attester, origin,
                         coalesce(voted_block, false) AS voted_block, t_ns
                  FROM {src}
                  WHERE msg = 'publish' AND t_ns IS NOT NULL
                  ORDER BY kind, slot, subnet, attester)
            TO '{out_dir / "publishes.parquet"}' (FORMAT parquet, COMPRESSION zstd)"""
    )
    con.close()
    (out_dir / "meta.json").write_text(json.dumps({"nodes": nodes}) + "\n")
    return out_dir


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--force", action="store_true", help="re-convert even if outputs exist")
    args = parser.parse_args(argv[1:])
    out = convert(args.run_dir, force=args.force)
    for f in sorted(out.iterdir()):
        print(f"{f}  {f.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
