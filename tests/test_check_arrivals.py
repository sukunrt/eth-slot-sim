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


def test_check_proposers_flags_non_supernode():
    # Every scheduled proposer must be a supernode.
    assert ca.check_proposers([2, 3], {2, 3, 5}) == []
    bad = ca.check_proposers([2, 4], {2, 3, 5})
    assert len(bad) == 1 and "proposer 4" in bad[0]


def test_check_proposers_flags_origin_off_schedule():
    # When per-slot block origins are known (the Shadow event path), each block must be
    # published by its slot's scheduled proposer.
    bad = ca.check_proposers([2, 3], {2, 3, 9}, block_origins={0: 2, 1: 9})
    assert len(bad) == 1 and "slot 1" in bad[0]


def test_load_proposers_and_supernodes(tmp_path):
    (tmp_path / "committee.json").write_text(
        '{"subnet_subscribers": [], "slots": ['
        '{"slot":0,"committees":[],"subnet_of":[],"proposer":2},'
        '{"slot":1,"committees":[],"subnet_of":[],"proposer":3}]}'
    )
    (tmp_path / "topology.json").write_text(
        '{"nodes":['
        '{"num":0,"upload_bw_mbps":25,"download_bw_mbps":50,"country":"x"},'
        '{"num":2,"upload_bw_mbps":1024,"download_bw_mbps":1024,"country":"x"},'
        '{"num":3,"upload_bw_mbps":1024,"download_bw_mbps":1024,"country":"x"}],'
        '"edges":[]}'
    )
    assert ca.load_proposers(tmp_path) == [2, 3]
    assert ca.load_supernodes(tmp_path) == {2, 3}
    assert ca.check_proposers(ca.load_proposers(tmp_path), ca.load_supernodes(tmp_path)) == []


def test_load_proposers_none_for_block_only(tmp_path):
    assert ca.load_proposers(tmp_path) is None  # no committee.json
    assert ca.load_supernodes(tmp_path) is None  # no topology.json


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


# --- aggregates (kind=3, global topic, multi-source dedup) ---------------------
#
# A committee's m aggregates are published byte-identically by all its aggregators and
# deduped on the wire. So each (slot, subnet, agg_idx) must reach every node EXCEPT that
# committee's aggregators (they publish it; their loopback is skipped) exactly once.
# Expected = Σ_c m·(N − |A_c|).


def _gpub(slot, subnet, agg_idx, t_ms):
    return (
        f'{{"msg":"publish","kind":3,"slot":{slot},"subnet":{subnet},"attester":{agg_idx},'
        f'"origin":-1,"voted_block":false,"t_ns":{t_ms * MS}}}'
    )


def _garr(node, slot, subnet, agg_idx, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":3,"slot":{slot},"subnet":{subnet},'
        f'"attester":{agg_idx},"origin":-1,"t_ns":{t_ms * MS}}}'
    )


def _committee_agg(n, m, aggregators, subnet_of=None):
    c = len(aggregators)
    return {
        "params": {"n": n, "m": m},
        "subnet_subscribers": [[] for _ in range(c)],
        "slots": [{
            "slot": 0,
            "committees": [[] for _ in range(c)],
            "subnet_of": subnet_of if subnet_of is not None else list(range(c)),
            "aggregators": aggregators,
        }],
    }


def test_aggregate_full_coverage_ok():
    # committee 0 on subnet 0, aggregators {0,1}, m=1, N=4. Both aggregators publish the same
    # aggregate (multi-source); it reaches {2,3} exactly once.
    data = _committee_agg(4, 1, [[0, 1]])
    lines = [_gpub(0, 0, 0, 800), _gpub(0, 0, 0, 800), _garr(2, 0, 0, 0, 900), _garr(3, 0, 0, 0, 950)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.expected == 2 and res.arrivals == 2
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.published == 1  # one distinct logical aggregate (m·C = 1·1)
    assert res.ok


def test_aggregate_m_gt_1_and_two_committees():
    # subnet 0 aggregators {0,1}, subnet 1 aggregators {0,3}, m=2, N=5.
    # Expected = 2·(5−2) + 2·(5−2) = 12; published = m·C = 4.
    data = _committee_agg(5, 2, [[0, 1], [0, 3]])
    lines = []
    for sub, aggs in ((0, {0, 1}), (1, {0, 3})):
        for ai in range(2):
            for node in set(range(5)) - aggs:
                lines.append(_garr(node, 0, sub, ai, 900))
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.expected == 12 and res.arrivals == 12
    assert res.published == 4
    assert res.ok


def test_aggregate_detects_missing():
    data = _committee_agg(4, 1, [[0, 1]])
    lines = [_gpub(0, 0, 0, 800), _garr(2, 0, 0, 0, 900)]  # node 3 missing
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.missing == [(3, 0, 0, 0)]
    assert not res.ok


def test_aggregate_detects_duplicate():
    # node 2 receives the same aggregate twice — gossipsub failed to dedup.
    data = _committee_agg(4, 1, [[0, 1]])
    lines = [_gpub(0, 0, 0, 800), _garr(2, 0, 0, 0, 900), _garr(2, 0, 0, 0, 910), _garr(3, 0, 0, 0, 950)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.duplicates == [(2, 0, 0, 0)]
    assert not res.ok


def test_aggregate_detects_leak_at_own_aggregator():
    # node 0 is an aggregator of subnet 0 — it published the aggregate, so recording its own
    # copy (loopback not skipped) is a failure.
    data = _committee_agg(4, 1, [[0, 1]])
    lines = [_gpub(0, 0, 0, 800), _garr(2, 0, 0, 0, 900), _garr(3, 0, 0, 0, 950), _garr(0, 0, 0, 0, 860)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.leaked == [(0, 0, 0, 0)]
    assert not res.ok


def test_analyze_aggregates_csv_coverage_ok(tmp_path):
    # simnet CSV path (kind=3, attester column carries agg_idx, no origin column).
    data = _committee_agg(4, 1, [[0, 1]])
    csv_path = tmp_path / "simnet_arrivals.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "2,0,3,0,0,90,false\n"
        "3,0,3,0,0,95,false\n"
    )
    res = ca.analyze_aggregates_csv(csv_path, data)
    assert res.expected == 2 and res.arrivals == 2
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok


def test_analyze_aggregates_csv_detects_missing_and_leak(tmp_path):
    data = _committee_agg(4, 1, [[0, 1]])
    csv_path = tmp_path / "a.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "2,0,3,0,0,90,false\n"  # node 3 (a non-aggregator) never receives -> missing
        "0,0,3,0,0,90,false\n"  # node 0 is an aggregator -> leak (loopback not skipped)
    )
    res = ca.analyze_aggregates_csv(csv_path, data)
    assert res.missing == [(3, 0, 0, 0)]
    assert res.leaked == [(0, 0, 0, 0)]
    assert not res.ok
