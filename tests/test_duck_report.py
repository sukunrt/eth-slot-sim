"""Equivalence tests: the DuckDB parquet path (analysis/duck_report.py) must produce the
EXACT report dict of the reference implementation (check_arrivals.build_report) on the
same run. Two synthetic runs cover every kind and every loss structure (missing, leaked,
duplicate, publisher drop), plus per-slot CDFs, vote fractions, coverage-at-deadline,
validator skew, and the proposer guard. Extend these fixtures whenever either path grows
a new report field."""

import json

from analysis import check_arrivals as ca
from analysis import duck_report, to_parquet

MS = 1_000_000  # ns per ms


def _pub(kind, slot, subnet, attester, origin, t_ms, voted=False):
    return json.dumps(
        {"msg": "publish", "kind": kind, "slot": slot, "subnet": subnet, "attester": attester,
         "origin": origin, "voted_block": voted, "t_ns": t_ms * MS}
    )


def _arr(node, kind, slot, subnet, attester, origin, t_ms):
    return json.dumps(
        {"msg": "arrival", "node": node, "kind": kind, "slot": slot, "subnet": subnet,
         "attester": attester, "origin": origin, "t_ns": t_ms * MS}
    )


def _write_run(tmp_path, n, lines, schedule=None, supernodes=()):
    hosts = tmp_path / "shadow.data" / "hosts"
    for i in range(n):
        (hosts / f"node{i}").mkdir(parents=True)
    (hosts / "node0" / "slot-sim-node.1000.stdout").write_text("\n".join(lines) + "\n")
    if schedule is not None:
        (tmp_path / "schedule.json").write_text(json.dumps(schedule))
    if supernodes:
        nodes = [
            {"num": i, "upload_bw_mbps": 2048 if i in supernodes else 100} for i in range(n)
        ]
        (tmp_path / "topology.json").write_text(json.dumps({"nodes": nodes}))
    return tmp_path


def _assert_equivalent(run_dir, capsys):
    """Both paths on one run: identical report dicts (and analysis.json contents)."""
    to_parquet.convert(run_dir)
    raw = ca.build_report(run_dir)
    raw_json = (run_dir / "analysis.json").read_text()
    capsys.readouterr()
    duck = duck_report.build_report(run_dir, run_dir / "parquet")
    assert duck == raw
    assert (run_dir / "analysis.json").read_text() == raw_json
    return raw


def test_equivalence_block_only(tmp_path, capsys):
    lines = [
        _pub(1, 0, -1, -1, 0, 0),
        _arr(1, 1, 0, -1, -1, 0, 500),
        _arr(2, 1, 0, -1, -1, 0, 700),
        _arr(2, 1, 0, -1, -1, 0, 800),  # duplicate at node 2
        _pub(1, 1, -1, -1, 0, 12_000),
        _arr(1, 1, 1, -1, -1, 0, 12_400),  # node 2 misses slot 1
        _pub(1, 2, -1, -1, 0, 24_000),  # publisher drop: nobody receives slot 2
    ]
    raw = _assert_equivalent(_write_run(tmp_path, 3, lines), capsys)
    blocks = raw["kinds"]["blocks"]
    assert blocks["missing"] == 3 and blocks["duplicates"] == 1 and blocks["publisher_drops"] == 1
    assert raw["result"] == "FAIL"


def _classic_schedule():
    """n=5, two slots: attestation subnets, aggregate + sync-contribution draws, columns,
    sync members. The slot-1 aggregate/contribution draws get no events at all, so their
    coverage is all-missing — exercising schedule-expected-but-never-published."""
    return {
        "params": {"n": 5, "v": 8, "ac_slots_per_finality_slot": 1},
        "subnet_subscribers": [[1, 2], [3, 4]],
        "column_subscribers": [[0, 1, 2], [2, 3, 4]],
        "sync_subscribers": [[1, 3]],
        "slots": [
            {"slot": 0, "proposer": 0, "committees": [], "subnet_of": [0, 1],
             "aggregators": [[1], [3]], "sync_aggregators": [[2]]},
            {"slot": 1, "proposer": 0, "committees": [], "subnet_of": [0, 1],
             "aggregators": [[2], [4]], "sync_aggregators": [[4]]},
        ],
    }


def test_equivalence_classic_kinds(tmp_path, capsys):
    lines = [
        # blocks: slot 0 complete, slot 1 misses node 4
        _pub(1, 0, -1, -1, 0, 0),
        *[_arr(i, 1, 0, -1, -1, 0, 500 + i) for i in (1, 2, 3, 4)],
        _pub(1, 1, -1, -1, 0, 12_000),
        *[_arr(i, 1, 1, -1, -1, 0, 12_400 + i) for i in (1, 2, 3)],
        # attestations: one delivered + leak at node 3; one publisher drop (=> missing at 4);
        # one on slot 1 for the per-slot view; voted 2/3
        _pub(2, 0, 0, 5, 1, 100, voted=True),
        _arr(2, 2, 0, 0, 5, 1, 300),
        _arr(3, 2, 0, 0, 5, 1, 310),  # node 3 is not a subnet-0 subscriber
        _pub(2, 0, 1, 6, 3, 100),
        _pub(2, 1, 0, 7, 2, 12_100, voted=True),
        _arr(1, 2, 1, 0, 7, 2, 12_300),
        # aggregates: (0,0,1) complete; (0,1,3) misses 4 + leaks at its own aggregator;
        # the slot-1 draws have no events
        _pub(3, 0, 0, 1, -1, 200),
        *[_arr(i, 3, 0, 0, 1, -1, 900 + i) for i in (0, 2, 3, 4)],
        _pub(3, 0, 1, 3, -1, 200),
        *[_arr(i, 3, 0, 1, 3, -1, 900 + i) for i in (0, 1, 2)],
        _arr(3, 3, 0, 1, 3, -1, 860),  # loopback at aggregator 3
        # columns: col 0 complete; col 1 misses 4 and leaks at non-custodier 0
        _pub(4, 0, 0, -1, 0, 50),
        _arr(1, 4, 0, 0, -1, 0, 150),
        _arr(2, 4, 0, 0, -1, 0, 160),
        _pub(4, 0, 1, -1, 0, 50),
        _arr(2, 4, 0, 1, -1, 0, 170),
        _arr(3, 4, 0, 1, -1, 0, 180),
        _arr(0, 4, 0, 1, -1, 0, 190),
        # sync messages: member 1 → 3 delivered; member 3 → 1 duplicated; voted-head 1/2
        _pub(5, 0, 0, 1, -1, 400, voted=True),
        _arr(3, 5, 0, 0, 1, -1, 600),
        _pub(5, 0, 0, 3, -1, 400),
        _arr(1, 5, 0, 0, 3, -1, 650),
        _arr(1, 5, 0, 0, 3, -1, 660),
        # sync contributions: (0,0,2) complete; the slot-1 draw has no events
        _pub(6, 0, 0, 2, -1, 800),
        *[_arr(i, 6, 0, 0, 2, -1, 950 + i) for i in (0, 1, 3, 4)],
    ]
    run = _write_run(tmp_path, 5, lines, schedule=_classic_schedule(), supernodes={0})
    raw = _assert_equivalent(run, capsys)
    kinds = raw["kinds"]
    assert set(kinds) == {
        "blocks", "attestations", "aggregates", "columns", "sync_messages", "sync_contributions",
    }
    att = kinds["attestations"]
    assert att["missing"] == 1 and att["leaked"] == 1 and att["publisher_drops"] == 1
    assert att["fraction_voted_block"] == 2 / 3
    assert kinds["aggregates"]["missing"] == 9 and kinds["aggregates"]["leaked"] == 1
    assert kinds["columns"]["missing"] == 1 and kinds["columns"]["leaked"] == 1
    assert kinds["sync_messages"]["duplicates"] == 1
    assert kinds["sync_contributions"]["missing"] == 4
    assert raw["proposer_guard"]["ok"]


def _decoupled_schedule():
    """n=4, k=2, segregated: per-AC-slot rounds, aggregator draws every slot,
    validator_counts (host = val). The (1,1,2) finality-aggregate draw never publishes,
    so its coverage cell has no deadline and is skipped."""
    return {
        "params": {"n": 4, "v": 4, "ac_slots_per_finality_slot": 2},
        "subnet_subscribers": [],
        "finality_subscribers": [[0, 1], [2, 3]],
        "finality_subnet_of": [0, 0, 1, 1],
        "finality_round_of": [0, 1, 0, 1],
        "validator_counts": [1, 1, 1, 1],
        "slots": [
            {"slot": 0, "proposer": 0, "committees": [], "subnet_of": [],
             "ac_voters": [{"node": 0, "val": 0, "subnet": 0, "position": 0},
                           {"node": 1, "val": 1, "subnet": 0, "position": 1}],
             "finality_aggregators": [[{"node": 1, "val": 1, "subnet": 0, "position": 0}],
                                      [{"node": 3, "val": 3, "subnet": 1, "position": 0}]]},
            {"slot": 1, "proposer": 0, "committees": [], "subnet_of": [],
             "ac_voters": [{"node": 2, "val": 2, "subnet": 0, "position": 0}],
             "finality_aggregators": [[{"node": 0, "val": 0, "subnet": 0, "position": 0}],
                                      [{"node": 2, "val": 2, "subnet": 1, "position": 0}]]},
        ],
    }


def test_equivalence_decoupled_kinds(tmp_path, capsys):
    lines = [
        # blocks: both slots complete
        _pub(1, 0, -1, -1, 0, 0),
        *[_arr(i, 1, 0, -1, -1, 0, 500 + i) for i in (1, 2, 3)],
        _pub(1, 1, -1, -1, 0, 12_000),
        *[_arr(i, 1, 1, -1, -1, 0, 12_400 + i) for i in (1, 2, 3)],
        # AC votes: val 0 complete; val 1 misses 3 + duplicate at 2; val 2 complete; voted 2/3
        _pub(7, 0, -1, 0, 0, 1000, voted=True),
        *[_arr(i, 7, 0, -1, 0, 0, 1200 + i) for i in (1, 2, 3)],
        _pub(7, 0, -1, 1, 1, 1000),
        _arr(0, 7, 0, -1, 1, 1, 1210),
        _arr(2, 7, 0, -1, 1, 1, 1220),
        _arr(2, 7, 0, -1, 1, 1, 1230),
        _pub(7, 1, -1, 2, 2, 13_000, voted=True),
        *[_arr(i, 7, 1, -1, 2, 2, 13_200 + i) for i in (0, 1, 3)],
        # finality attestations (slot = AC slot under segregation): val 0's vote reaches
        # its required receiver (node 1) and LEAKS at node 2 (outside allowed(0,0));
        # val 3's slot-1 vote is a publisher drop (missing at required node 2)
        _pub(8, 0, 0, 0, 0, 2000),
        _arr(1, 8, 0, 0, 0, 0, 2200),
        _arr(2, 8, 0, 0, 0, 0, 2300),
        _pub(8, 0, 1, 2, 2, 2000),
        _arr(3, 8, 0, 1, 2, 2, 2100),
        _pub(8, 1, 0, 1, 1, 14_000),
        _arr(0, 8, 1, 0, 1, 1, 14_200),
        _pub(8, 1, 1, 3, 3, 14_000),
        # finality aggregates: three of the four draws publish + fully disseminate;
        # (1,1,2) never publishes. The publish instants are the coverage deadlines.
        _pub(9, 0, 0, 1, -1, 5000),
        *[_arr(i, 9, 0, 0, 1, -1, 5200 + i) for i in (0, 2, 3)],
        _pub(9, 0, 1, 3, -1, 5000),
        *[_arr(i, 9, 0, 1, 3, -1, 5200 + i) for i in (0, 1, 2)],
        _pub(9, 1, 0, 0, -1, 17_000),
        *[_arr(i, 9, 1, 0, 0, -1, 17_200 + i) for i in (1, 2, 3)],
    ]
    run = _write_run(tmp_path, 4, lines, schedule=_decoupled_schedule(), supernodes={0})
    raw = _assert_equivalent(run, capsys)
    kinds = raw["kinds"]
    assert set(kinds) == {"blocks", "attestations", "ac_votes", "finality_attestations",
                          "finality_aggregates"}
    ac = kinds["ac_votes"]
    assert ac["missing"] == 1 and ac["duplicates"] == 1 and ac["fraction_voted_block"] == 2 / 3
    fv = kinds["finality_attestations"]
    assert fv["missing"] == 1 and fv["leaked"] == 1 and fv["publisher_drops"] == 1
    # per-slot view counts ALL arrivals (incl. the leak); the headline CDF is strict-only
    assert fv["per_slot"]["0"]["count"] == 3 and fv["cdf_ms"]["count"] == 3
    assert fv["coverage_at_deadline"] == {"0": {"0": 1.0, "1": 1.0}, "1": {"0": 1.0}}
    assert kinds["finality_aggregates"]["missing"] == 3
    assert raw["validators"]["v"] == 4
    assert raw["proposer_guard"]["ok"]
