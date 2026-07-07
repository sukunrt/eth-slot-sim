"""Converter tests: slog stdout files → parquet event tables (analysis/to_parquet.py)."""

import json

import duckdb
import pytest

from analysis import to_parquet

MS = 1_000_000  # ns per ms


def _pub(kind, slot, subnet, attester, origin, t_ms, voted=False):
    return json.dumps(
        {"time": "2026-01-01T00:00:00Z", "level": "INFO", "msg": "publish", "kind": kind,
         "slot": slot, "subnet": subnet, "attester": attester, "origin": origin,
         "voted_block": voted, "t_ns": t_ms * MS}
    )


def _arr(node, kind, slot, subnet, attester, origin, t_ms):
    return json.dumps(
        {"time": "2026-01-01T00:00:00Z", "level": "INFO", "msg": "arrival", "node": node,
         "kind": kind, "slot": slot, "subnet": subnet, "attester": attester,
         "origin": origin, "t_ns": t_ms * MS}
    )


def _mini_run(tmp_path):
    """3 hosts; node2 wrote no stdout at all but must still appear in meta."""
    hosts = tmp_path / "shadow.data" / "hosts"
    (hosts / "node0").mkdir(parents=True)
    (hosts / "node1").mkdir()
    (hosts / "node2").mkdir()
    (hosts / "node0" / "slot-sim-node.1000.stdout").write_text(
        "\n".join(
            [
                _pub(1, 0, -1, -1, 0, 0),
                "time=x level=DEBUG msg=rpc non-json noise",  # RPC text logs are skipped
                '{"msg":"peers connected","node":0}',  # JSON but neither event: no t_ns row
            ]
        )
        + "\n"
    )
    (hosts / "node1" / "slot-sim-node.1001.stdout").write_text(
        _arr(1, 1, 0, -1, -1, 0, 500) + "\n" + _arr(1, 2, 0, 3, 7, 0, 600) + "\n"
    )
    return tmp_path


def test_convert_writes_tables_and_meta(tmp_path):
    out = to_parquet.convert(_mini_run(tmp_path))
    assert out == tmp_path / "parquet"
    con = duckdb.connect()
    pubs = con.execute(f"SELECT * FROM '{out / 'publishes.parquet'}' ORDER BY ALL").fetchall()
    arrs = con.execute(f"SELECT * FROM '{out / 'arrivals.parquet'}' ORDER BY ALL").fetchall()
    assert pubs == [(1, 0, -1, -1, 0, False, 0)]
    assert arrs == [(1, 1, 0, -1, -1, 0, 500 * MS), (1, 2, 0, 3, 7, 0, 600 * MS)]
    assert json.loads((out / "meta.json").read_text()) == {"nodes": [0, 1, 2]}


def test_convert_is_idempotent_unless_forced(tmp_path):
    out = to_parquet.convert(_mini_run(tmp_path))
    marker = out / "arrivals.parquet"
    before = marker.stat().st_mtime_ns
    assert to_parquet.convert(tmp_path) == out  # second call: no rewrite
    assert marker.stat().st_mtime_ns == before
    to_parquet.convert(tmp_path, force=True)
    assert marker.stat().st_mtime_ns > before


def test_convert_requires_run_dir(tmp_path):
    with pytest.raises(FileNotFoundError):
        to_parquet.convert(tmp_path)
