"""TDD for the arrival check/CDF core (parse slog JSON events → verify receipt)."""

from analysis import check_arrivals as ca

MS = 1_000_000  # ns per ms


def _pub(slot, origin, t_ms):
    return f'{{"msg":"publish","slot":{slot},"origin":{origin},"t_ns":{t_ms * MS}}}'


def _arr(node, slot, origin, t_ms):
    return f'{{"msg":"arrival","node":{node},"slot":{slot},"origin":{origin},"t_ns":{t_ms * MS}}}'


def test_full_receipt_and_delays():
    lines = [
        _pub(0, 0, 0),
        _arr(1, 0, 0, 500),
        _arr(2, 0, 0, 1000),
        '{"msg":"peers connected","node":0}',  # unrelated line is ignored
    ]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze(pubs, arrs, node_nums={0, 1, 2})
    assert res.arrivals == 2
    assert res.expected == 2
    assert res.missing == []
    assert res.duplicates == []
    assert res.delays_ms == [500.0, 1000.0]


def test_detects_missing():
    lines = [_pub(0, 0, 0), _arr(1, 0, 0, 500)]  # node 2 never received
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze(pubs, arrs, node_nums={0, 1, 2})
    assert res.expected == 2
    assert res.arrivals == 1
    assert res.missing == [(2, 0, 0)]


def test_detects_duplicate():
    lines = [
        _pub(0, 0, 0),
        _arr(1, 0, 0, 500),
        _arr(1, 0, 0, 600),  # duplicate (node, slot, origin)
        _arr(2, 0, 0, 700),
    ]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze(pubs, arrs, node_nums={0, 1, 2})
    assert res.duplicates == [(1, 0, 0)]
    assert res.missing == []


def test_percentile_nearest_rank():
    delays = [float(x) for x in range(1, 101)]  # 1..100 ms
    assert ca.percentile(delays, 50) == 50.0
    assert ca.percentile(delays, 99) == 99.0
    assert ca.percentile(delays, 100) == 100.0


def test_cdf_summary():
    delays = [float(x) for x in range(1, 101)]  # 1..100 ms
    assert ca.cdf(delays) == {
        "count": 100, "p50": 50.0, "p90": 90.0, "p99": 99.0, "p100": 100.0,
    }


def test_delays_from_csv(tmp_path):
    # Matches the slot-sim Recorder.WriteCSV header + integer-ms rows.
    csv_path = tmp_path / "simnet_arrivals.csv"
    csv_path.write_text("node,slot,origin,delay_ms\n1,0,0,500\n2,0,0,1000\n")
    assert ca.delays_from_csv(csv_path) == [500.0, 1000.0]
