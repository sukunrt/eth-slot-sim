"""TDD for the arrival check/CDF core: parse slog JSON events → verify receipt.

Blocks (kind=1) must reach every node; attestations (kind=2) must reach exactly their
subnet's subscribers and nobody else (missing/leaked/duplicate all fail).
"""

from analysis import check_arrivals as ca

MS = 1_000_000  # ns per ms


def _pub(slot, origin, t_ms):
    return (
        f'{{"msg":"publish","kind":1,"slot":{slot},"subnet":-1,"attester":-1,'
        f'"origin":{origin},"voted_block":false,"t_ns":{t_ms * MS}}}'
    )


def _arr(node, slot, origin, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":1,"slot":{slot},"subnet":-1,'
        f'"attester":-1,"origin":{origin},"t_ns":{t_ms * MS}}}'
    )


def _apub(slot, subnet, attester, origin, t_ms, voted):
    return (
        f'{{"msg":"publish","kind":2,"slot":{slot},"subnet":{subnet},"attester":{attester},'
        f'"origin":{origin},"voted_block":{"true" if voted else "false"},"t_ns":{t_ms * MS}}}'
    )


def _aarr(node, slot, subnet, attester, origin, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":2,"slot":{slot},"subnet":{subnet},'
        f'"attester":{attester},"origin":{origin},"t_ns":{t_ms * MS}}}'
    )


def test_block_full_receipt_and_delays():
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
    assert res.ok


def test_block_detects_missing():
    lines = [_pub(0, 0, 0), _arr(1, 0, 0, 500)]  # node 2 never received
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze(pubs, arrs, node_nums={0, 1, 2})
    assert res.expected == 2
    assert res.arrivals == 1
    assert res.missing == [(2, 0, 0)]


def test_block_detects_duplicate():
    lines = [_pub(0, 0, 0), _arr(1, 0, 0, 500), _arr(1, 0, 0, 600), _arr(2, 0, 0, 700)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze(pubs, arrs, node_nums={0, 1, 2})
    assert res.duplicates == [(1, 0, 0)]
    assert res.missing == []


def test_attestation_full_coverage_ok():
    # attester val 5 on node 0 publishes subnet 3; subscribers {1,2} both receive.
    subs = {3: {1, 2}}
    lines = [_apub(0, 3, 5, 0, 0, True), _aarr(1, 0, 3, 5, 0, 200), _aarr(2, 0, 3, 5, 0, 250)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_attestations(pubs, arrs, subs)
    assert res.expected == 2
    assert res.arrivals == 2
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok
    assert res.fraction_voted_block == 1.0


def test_attestation_detects_missing():
    subs = {3: {1, 2}}
    lines = [_apub(0, 3, 5, 0, 0, True), _aarr(1, 0, 3, 5, 0, 200)]  # node 2 missing
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_attestations(pubs, arrs, subs)
    assert res.missing == [(2, 0, 3, 5, 0)]
    assert not res.ok


def test_attestation_detects_leak():
    # node 9 is not a subscriber of subnet 3 — receiving the attestation is a leak.
    subs = {3: {1, 2}}
    lines = [_apub(0, 3, 5, 0, 0, True), _aarr(1, 0, 3, 5, 0, 200), _aarr(2, 0, 3, 5, 0, 200), _aarr(9, 0, 3, 5, 0, 200)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_attestations(pubs, arrs, subs)
    assert res.leaked == [(9, 0, 3, 5, 0)]
    assert not res.ok


def test_attestation_fraction_voted_block():
    subs = {0: set()}  # no subscribers; we only test the publish-side fraction
    lines = [
        _apub(0, 0, 0, 0, 0, True),
        _apub(0, 0, 1, 1, 0, True),
        _apub(0, 0, 2, 2, 0, False),
    ]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_attestations(pubs, arrs, subs)
    assert res.published == 3
    assert abs(res.fraction_voted_block - 2 / 3) < 1e-9


def test_percentile_nearest_rank():
    delays = [float(x) for x in range(1, 101)]
    assert ca.percentile(delays, 50) == 50.0
    assert ca.percentile(delays, 99) == 99.0
    assert ca.percentile(delays, 100) == 100.0


def test_cdf_summary():
    delays = [float(x) for x in range(1, 101)]
    assert ca.cdf(delays) == {"count": 100, "p50": 50.0, "p90": 90.0, "p99": 99.0, "p100": 100.0}


def test_delays_from_csv_filters_by_kind(tmp_path):
    # Matches Recorder.WriteCSV: node,slot,kind,subnet,attester,delay_ms,voted_block.
    csv_path = tmp_path / "simnet_arrivals.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "1,0,1,-1,-1,500,false\n"  # block
        "2,0,1,-1,-1,1000,false\n"  # block
        "3,0,2,4,7,40,true\n"  # attestation — excluded from the block CDF
    )
    assert ca.delays_from_csv(csv_path) == [500.0, 1000.0]
    assert ca.delays_from_csv(csv_path, kind=ca.ATTEST_KIND) == [40.0]


# committee.json shape the simnet CSV check joins against: subscribers per subnet + the draw.
def _committee(subnet_subscribers, committees):
    return {
        "subnet_subscribers": subnet_subscribers,
        "slots": [{"slot": 0, "committees": committees, "subnet_of": list(range(len(committees)))}],
    }


def test_analyze_attestations_csv_coverage_ok(tmp_path):
    # attester val 5 on node 0 publishes subnet 0; subscribers {1,2} both receive.
    data = _committee([[1, 2]], [[{"node": 0, "val": 5, "subnet": 0, "position": 0}]])
    csv_path = tmp_path / "simnet_arrivals.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "1,0,2,0,5,40,true\n"
        "2,0,2,0,5,55,true\n"
    )
    res = ca.analyze_attestations_csv(csv_path, data)
    assert res.expected == 2 and res.arrivals == 2
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok and res.fraction_voted_block == 1.0


def test_analyze_attestations_csv_detects_missing_and_leak(tmp_path):
    data = _committee([[1, 2]], [[{"node": 0, "val": 5, "subnet": 0, "position": 0}]])
    csv_path = tmp_path / "a.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "1,0,2,0,5,40,true\n"  # node 2 (a subscriber) never receives -> missing
        "9,0,2,0,5,40,true\n"  # node 9 is not a subscriber -> leak
    )
    res = ca.analyze_attestations_csv(csv_path, data)
    assert res.missing == [(2, 0, 0, 5, 0)]
    assert res.leaked == [(9, 0, 0, 5, 0)]
    assert not res.ok
