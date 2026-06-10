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


# schedule.json shape the simnet CSV check joins against: subscribers per subnet + the draw.
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
    (tmp_path / "schedule.json").write_text(
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
    assert ca.load_proposers(tmp_path) is None  # no schedule.json
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


# --- aggregates (kind=3, global topic, distinct per aggregator) ----------------
#
# Each aggregator publishes ONE distinct aggregate (signed with its key; the aggregator is
# carried in the attester field). It must reach every node EXCEPT that aggregator, exactly
# once — the aggregates are NOT deduped. Expected = Σ_c |A_c|·(N − 1).


def _gpub(slot, subnet, aggregator, t_ms):
    return (
        f'{{"msg":"publish","kind":3,"slot":{slot},"subnet":{subnet},"attester":{aggregator},'
        f'"origin":-1,"voted_block":false,"t_ns":{t_ms * MS}}}'
    )


def _garr(node, slot, subnet, aggregator, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":3,"slot":{slot},"subnet":{subnet},'
        f'"attester":{aggregator},"origin":-1,"t_ns":{t_ms * MS}}}'
    )


def _committee_agg(n, aggregators, subnet_of=None):
    c = len(aggregators)
    return {
        "params": {"n": n},
        "subnet_subscribers": [[] for _ in range(c)],
        "slots": [{
            "slot": 0,
            "committees": [[] for _ in range(c)],
            "subnet_of": subnet_of if subnet_of is not None else list(range(c)),
            "aggregators": aggregators,
        }],
    }


def _agg_arrivals(slot, subnet, aggregators, n, t_ms=900):
    # Each aggregator's aggregate reaches every node except itself.
    return [
        _garr(node, slot, subnet, agg, t_ms)
        for agg in aggregators
        for node in set(range(n)) - {agg}
    ]


def test_aggregate_full_coverage_ok():
    # committee 0 on subnet 0, aggregators {0,1}, N=4. Each publishes a distinct aggregate:
    # node 0's reaches {1,2,3}, node 1's reaches {0,2,3}.
    data = _committee_agg(4, [[0, 1]])
    lines = [_gpub(0, 0, 0, 800), _gpub(0, 0, 1, 800)] + _agg_arrivals(0, 0, [0, 1], 4)
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.expected == 6 and res.arrivals == 6  # 2 aggregators × (4−1)
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.published == 2  # one aggregate per aggregator
    assert res.ok


def test_aggregate_two_committees():
    # subnet 0 aggregators {0,1}, subnet 1 aggregators {0,3}, N=5.
    # Expected = 2·(5−1) + 2·(5−1) = 16; published = Σ|A_c| = 4.
    data = _committee_agg(5, [[0, 1], [0, 3]])
    lines = _agg_arrivals(0, 0, [0, 1], 5) + _agg_arrivals(0, 1, [0, 3], 5)
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.expected == 16 and res.arrivals == 16
    assert res.published == 4
    assert res.ok


def test_aggregate_detects_missing():
    data = _committee_agg(4, [[0]])  # single aggregator 0; its aggregate should reach {1,2,3}
    lines = [_garr(1, 0, 0, 0, 900), _garr(2, 0, 0, 0, 900)]  # node 3 missing
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.missing == [(3, 0, 0, 0)]
    assert not res.ok


def test_aggregate_detects_duplicate():
    data = _committee_agg(4, [[0]])
    lines = [_garr(1, 0, 0, 0, 900), _garr(1, 0, 0, 0, 910), _garr(2, 0, 0, 0, 900), _garr(3, 0, 0, 0, 950)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.duplicates == [(1, 0, 0, 0)]
    assert not res.ok


def test_aggregate_detects_leak_at_own_aggregator():
    # node 0 published its own aggregate, so recording its own copy (loopback not skipped) fails.
    data = _committee_agg(4, [[0]])
    lines = [_garr(1, 0, 0, 0, 900), _garr(2, 0, 0, 0, 900), _garr(3, 0, 0, 0, 950), _garr(0, 0, 0, 0, 860)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_aggregates(pubs, arrs, data)
    assert res.leaked == [(0, 0, 0, 0)]
    assert not res.ok


def test_analyze_aggregates_csv_coverage_ok(tmp_path):
    # simnet CSV path (kind=3, attester column carries the aggregator, no origin column).
    data = _committee_agg(4, [[0, 1]])
    csv_path = tmp_path / "simnet_arrivals.csv"
    rows = ["node,slot,kind,subnet,attester,delay_ms,voted_block"]
    for agg in (0, 1):
        for node in sorted(set(range(4)) - {agg}):
            rows.append(f"{node},0,3,0,{agg},90,false")
    csv_path.write_text("\n".join(rows) + "\n")
    res = ca.analyze_aggregates_csv(csv_path, data)
    assert res.expected == 6 and res.arrivals == 6
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok


def test_analyze_aggregates_csv_detects_missing_and_leak(tmp_path):
    data = _committee_agg(4, [[0]])  # single aggregator 0
    csv_path = tmp_path / "a.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "1,0,3,0,0,90,false\n"  # nodes 2,3 never receive -> missing
        "0,0,3,0,0,90,false\n"  # node 0 is the aggregator -> leak (loopback not skipped)
    )
    res = ca.analyze_aggregates_csv(csv_path, data)
    assert res.missing == [(2, 0, 0, 0), (3, 0, 0, 0)]
    assert res.leaked == [(0, 0, 0, 0)]
    assert not res.ok


# --- data columns (kind=4, per-column subnet, distinct per proposer burst) ------
#
# Each column (the column index in the subnet field, origin = proposer) must reach exactly its
# custodiers \ {proposer}, once — missing/leaked/duplicate all fail.


def _cpub(slot, col, origin, t_ms):
    return (
        f'{{"msg":"publish","kind":4,"slot":{slot},"subnet":{col},"attester":-1,'
        f'"origin":{origin},"voted_block":false,"t_ns":{t_ms * MS}}}'
    )


def _carr(node, slot, col, origin, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":4,"slot":{slot},"subnet":{col},'
        f'"attester":-1,"origin":{origin},"t_ns":{t_ms * MS}}}'
    )


def test_column_full_coverage_ok():
    # column 0, proposer 0; custodiers {0,1,2}; reaches {1,2}.
    custodiers = {0: {0, 1, 2}}
    lines = [_cpub(0, 0, 0, 0), _carr(1, 0, 0, 0, 100), _carr(2, 0, 0, 0, 150)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_columns(pubs, arrs, custodiers)
    assert res.expected == 2 and res.arrivals == 2 and res.published == 1
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok


def test_column_detects_missing():
    custodiers = {0: {0, 1, 2}}
    lines = [_cpub(0, 0, 0, 0), _carr(1, 0, 0, 0, 100)]  # node 2 missing
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_columns(pubs, arrs, custodiers)
    assert res.missing == [(2, 0, 0, 0)]
    assert not res.ok


def test_column_detects_leak():
    # node 9 is not a custodier of column 0 — receiving it is a leak.
    custodiers = {0: {0, 1, 2}}
    lines = [_cpub(0, 0, 0, 0), _carr(1, 0, 0, 0, 100), _carr(2, 0, 0, 0, 100), _carr(9, 0, 0, 0, 100)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_columns(pubs, arrs, custodiers)
    assert res.leaked == [(9, 0, 0, 0)]
    assert not res.ok


def _committee_col(column_subscribers, proposer=0):
    return {
        "params": {"n": 4},
        "subnet_subscribers": [],
        "num_columns": len(column_subscribers),
        "column_subscribers": column_subscribers,
        "slots": [{"slot": 0, "committees": [], "subnet_of": [], "proposer": proposer}],
    }


def test_analyze_columns_csv_coverage_ok(tmp_path):
    data = _committee_col([[0, 1, 2]], proposer=0)
    csv_path = tmp_path / "simnet_arrivals.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "1,0,4,0,-1,40,false\n"
        "2,0,4,0,-1,55,false\n"
    )
    res = ca.analyze_columns_csv(csv_path, data)
    assert res.expected == 2 and res.arrivals == 2 and res.published == 1
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok


def test_analyze_columns_csv_detects_missing_and_leak(tmp_path):
    data = _committee_col([[0, 1, 2]], proposer=0)
    csv_path = tmp_path / "a.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "1,0,4,0,-1,40,false\n"  # node 2 (a custodier) never receives -> missing
        "9,0,4,0,-1,40,false\n"  # node 9 not a custodier -> leak
    )
    res = ca.analyze_columns_csv(csv_path, data)
    assert res.missing == [(2, 0, 0, 0)]
    assert res.leaked == [(9, 0, 0, 0)]
    assert not res.ok


def test_load_column_subscribers(tmp_path):
    (tmp_path / "schedule.json").write_text(
        '{"subnet_subscribers": [], "num_columns": 2, '
        '"column_subscribers": [[0,1,2],[0,3]], '
        '"slots": [{"slot":0,"committees":[],"subnet_of":[],"proposer":0}]}'
    )
    assert ca.load_column_subscribers(tmp_path) == {0: {0, 1, 2}, 1: {0, 3}}


def test_load_column_subscribers_none_without_columns(tmp_path):
    (tmp_path / "schedule.json").write_text('{"subnet_subscribers": [], "slots": []}')
    assert ca.load_column_subscribers(tmp_path) is None


# --- sync messages (kind=5, per-subnet, the member in the attester field) -------
#
# Each member publishes ONE message on its subnet (member in the attester field, origin -1, like
# an aggregate). It must reach exactly subscribers(subnet) \ {member} — missing/leaked/duplicate
# all fail. The publish carries the head vote (fraction_voted_head, the sync analogue of
# fraction_voted_block).


def _spub(slot, subnet, member, t_ms, voted):
    return (
        f'{{"msg":"publish","kind":5,"slot":{slot},"subnet":{subnet},"attester":{member},'
        f'"origin":-1,"voted_block":{"true" if voted else "false"},"t_ns":{t_ms * MS}}}'
    )


def _sarr(node, slot, subnet, member, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":5,"slot":{slot},"subnet":{subnet},'
        f'"attester":{member},"origin":-1,"t_ns":{t_ms * MS}}}'
    )


def test_sync_message_full_coverage_ok():
    # subnet 0 members {1,2,3}; member 1 publishes, reaches {2,3}.
    subs = {0: {1, 2, 3}}
    lines = [_spub(0, 0, 1, 0, True), _sarr(2, 0, 0, 1, 200), _sarr(3, 0, 0, 1, 250)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_sync_messages(pubs, arrs, subs)
    assert res.expected == 2 and res.arrivals == 2
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok and res.fraction_voted_head == 1.0


def test_sync_message_detects_missing():
    subs = {0: {1, 2, 3}}
    lines = [_spub(0, 0, 1, 0, True), _sarr(2, 0, 0, 1, 200)]  # member 3 missing
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_sync_messages(pubs, arrs, subs)
    assert res.missing == [(3, 0, 0, 1)]
    assert not res.ok


def test_sync_message_detects_leak():
    # node 9 is not a member of subnet 0 — receiving the message is a leak.
    subs = {0: {1, 2, 3}}
    lines = [_spub(0, 0, 1, 0, True), _sarr(2, 0, 0, 1, 200), _sarr(3, 0, 0, 1, 200), _sarr(9, 0, 0, 1, 200)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_sync_messages(pubs, arrs, subs)
    assert res.leaked == [(9, 0, 0, 1)]
    assert not res.ok


def test_sync_message_fraction_voted_head():
    subs = {0: set()}  # publish-side fraction only
    lines = [_spub(0, 0, 1, 0, True), _spub(0, 0, 2, 0, True), _spub(0, 0, 3, 0, False)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_sync_messages(pubs, arrs, subs)
    assert res.published == 3
    assert abs(res.fraction_voted_head - 2 / 3) < 1e-9


def _sync_committee(sync_subscribers, slots=1):
    return {
        "params": {"n": 4},
        "subnet_subscribers": [],
        "sync_subscribers": sync_subscribers,
        "slots": [{"slot": s, "committees": [], "subnet_of": []} for s in range(slots)],
    }


def test_analyze_sync_messages_csv_coverage_ok(tmp_path):
    data = _sync_committee([[1, 2, 3]])  # subnet 0 members {1,2,3}
    csv_path = tmp_path / "simnet_arrivals.csv"
    rows = ["node,slot,kind,subnet,attester,delay_ms,voted_block"]
    for member in (1, 2, 3):  # each member's message reaches the other two
        for node in sorted({1, 2, 3} - {member}):
            rows.append(f"{node},0,5,0,{member},40,true")
    csv_path.write_text("\n".join(rows) + "\n")
    res = ca.analyze_sync_messages_csv(csv_path, data)
    assert res.expected == 6 and res.arrivals == 6  # 3 members × 2
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok and res.fraction_voted_head == 1.0


def test_analyze_sync_messages_csv_detects_missing_and_leak(tmp_path):
    data = _sync_committee([[1, 2, 3]])
    csv_path = tmp_path / "a.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "2,0,5,0,1,40,true\n"  # member 1's message reaches only 2 (member 3 missing)
        "9,0,5,0,1,40,true\n"  # node 9 not a member -> leak
    )
    res = ca.analyze_sync_messages_csv(csv_path, data)
    assert (3, 0, 0, 1) in res.missing  # member 1 -> 3 missing
    assert res.leaked == [(9, 0, 0, 1)]
    assert not res.ok


# --- sync contributions (kind=6, global topic, distinct per aggregator) ---------
#
# Each aggregator publishes ONE distinct contribution (the aggregator in the attester field). It
# must reach every node EXCEPT that aggregator, exactly once. sync_aggregators is indexed by
# subnet directly. Expected = Σ_subnet |A|·(N − 1) — the same shape as aggregates.


def _scarr(node, slot, subnet, aggregator, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":6,"slot":{slot},"subnet":{subnet},'
        f'"attester":{aggregator},"origin":-1,"t_ns":{t_ms * MS}}}'
    )


def _scpub(slot, subnet, aggregator, t_ms):
    return (
        f'{{"msg":"publish","kind":6,"slot":{slot},"subnet":{subnet},"attester":{aggregator},'
        f'"origin":-1,"voted_block":false,"t_ns":{t_ms * MS}}}'
    )


def _sync_committee_agg(n, sync_aggregators):
    return {
        "params": {"n": n},
        "subnet_subscribers": [],
        "sync_subscribers": [[] for _ in sync_aggregators],
        "slots": [{"slot": 0, "committees": [], "subnet_of": [], "sync_aggregators": sync_aggregators}],
    }


def test_sync_contribution_full_coverage_ok():
    # subnet 0 aggregators {0,1}, N=4. Each contribution reaches every node but its aggregator.
    data = _sync_committee_agg(4, [[0, 1]])
    lines = [_scpub(0, 0, 0, 800), _scpub(0, 0, 1, 800)] + [
        _scarr(node, 0, 0, agg, 900) for agg in (0, 1) for node in set(range(4)) - {agg}
    ]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_sync_contributions(pubs, arrs, data)
    assert res.expected == 6 and res.arrivals == 6 and res.published == 2
    assert res.ok


def test_sync_contribution_two_subnets():
    # subnet 0 aggregators {0,1}, subnet 1 aggregators {3,4}, N=6 ⇒ 4·(6−1)=20, published 4.
    data = _sync_committee_agg(6, [[0, 1], [3, 4]])
    lines = [
        _scarr(node, 0, sub, agg, 900)
        for sub, aggs in enumerate([[0, 1], [3, 4]])
        for agg in aggs
        for node in set(range(6)) - {agg}
    ]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_sync_contributions(pubs, arrs, data)
    assert res.expected == 20 and res.arrivals == 20 and res.published == 4
    assert res.ok


def test_sync_contribution_detects_leak_at_own_aggregator():
    data = _sync_committee_agg(4, [[0]])  # aggregator 0; reaches {1,2,3}
    lines = [_scarr(1, 0, 0, 0, 900), _scarr(2, 0, 0, 0, 900), _scarr(3, 0, 0, 0, 950), _scarr(0, 0, 0, 0, 860)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_sync_contributions(pubs, arrs, data)
    assert res.leaked == [(0, 0, 0, 0)]  # node 0 recorded its own contribution (loopback not skipped)
    assert not res.ok


def test_analyze_sync_contributions_csv_coverage_ok(tmp_path):
    data = _sync_committee_agg(4, [[0, 1]])
    csv_path = tmp_path / "simnet_arrivals.csv"
    rows = ["node,slot,kind,subnet,attester,delay_ms,voted_block"]
    for agg in (0, 1):
        for node in sorted(set(range(4)) - {agg}):
            rows.append(f"{node},0,6,0,{agg},90,false")
    csv_path.write_text("\n".join(rows) + "\n")
    res = ca.analyze_sync_contributions_csv(csv_path, data)
    assert res.expected == 6 and res.arrivals == 6 and res.ok


def test_load_sync_subscribers(tmp_path):
    (tmp_path / "schedule.json").write_text(
        '{"subnet_subscribers": [], "sync_subscribers": [[0,2],[1,3]], '
        '"slots": [{"slot":0,"committees":[],"subnet_of":[]}]}'
    )
    assert ca.load_sync_subscribers(tmp_path) == {0: {0, 2}, 1: {1, 3}}


def test_load_sync_subscribers_none_without_sync(tmp_path):
    (tmp_path / "schedule.json").write_text('{"subnet_subscribers": [], "slots": []}')
    assert ca.load_sync_subscribers(tmp_path) is None


# --- AC votes (kind=7, global topic, distinct per (val, publisher), DA-gated vote) -----
#
# Each VRF-selected validator publishes ONE availability-chain vote on the single global topic (the
# validator rides the attester field, its host rides origin). It must reach every node EXCEPT its
# publisher, exactly once — like an aggregate, but the publisher (origin) is excluded, not the
# attester. The publish carries the block vote (fraction_voted_block, the AC analogue of an
# attestation's). num_columns/finality membership are irrelevant here.


def _avpub(slot, val, origin, t_ms, voted):
    return (
        f'{{"msg":"publish","kind":7,"slot":{slot},"subnet":-1,"attester":{val},'
        f'"origin":{origin},"voted_block":{"true" if voted else "false"},"t_ns":{t_ms * MS}}}'
    )


def _avarr(node, slot, val, origin, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":7,"slot":{slot},"subnet":-1,'
        f'"attester":{val},"origin":{origin},"t_ns":{t_ms * MS}}}'
    )


def _decoupled_ac(n, ac_voters, k=1):
    # ac_voters: list of (val, node) for slot 0. finality_subscribers present so main() would run it.
    return {
        "params": {"n": n, "v": n, "ac_slots_per_finality_slot": k},
        "subnet_subscribers": [],
        "finality_subscribers": [list(range(n))],
        "slots": [{
            "slot": 0, "committees": [], "subnet_of": [], "proposer": 0,
            "ac_voters": [{"node": nd, "val": val, "subnet": 0, "position": i}
                          for i, (val, nd) in enumerate(ac_voters)],
        }],
    }


def _ac_arrivals(slot, ac_voters, n, t_ms=300):
    # Each voter's vote reaches every node except its publisher (origin == its host node).
    return [
        _avarr(node, slot, val, nd, t_ms)
        for val, nd in ac_voters
        for node in set(range(n)) - {nd}
    ]


def test_ac_vote_full_coverage_ok():
    # voters val0@node0, val1@node1, val2@node2, N=4. Each vote reaches every node but its host.
    voters = [(0, 0), (1, 1), (2, 2)]
    data = _decoupled_ac(4, voters)
    lines = [_avpub(0, val, nd, 100, True) for val, nd in voters] + _ac_arrivals(0, voters, 4)
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_ac_votes(pubs, arrs, data)
    assert res.expected == 9 and res.arrivals == 9  # 3 voters × (4−1)
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.published == 3 and res.ok and res.fraction_voted_block == 1.0


def test_ac_vote_detects_missing():
    voters = [(0, 0)]  # single voter val0@node0; its vote should reach {1,2,3}
    data = _decoupled_ac(4, voters)
    lines = [_avpub(0, 0, 0, 100, True), _avarr(1, 0, 0, 0, 300), _avarr(2, 0, 0, 0, 300)]  # 3 missing
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_ac_votes(pubs, arrs, data)
    assert res.missing == [(3, 0, -1, 0, 0)]
    assert not res.ok


def test_ac_vote_detects_duplicate():
    voters = [(0, 0)]
    data = _decoupled_ac(4, voters)
    lines = [_avarr(1, 0, 0, 0, 300), _avarr(1, 0, 0, 0, 310), _avarr(2, 0, 0, 0, 300), _avarr(3, 0, 0, 0, 350)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_ac_votes(pubs, arrs, data)
    assert res.duplicates == [(1, 0, 0, 0)]
    assert not res.ok


def test_ac_vote_detects_leak_at_own_publisher():
    # node 0 published its own vote, so recording its own copy (loopback not skipped) fails.
    voters = [(0, 0)]
    data = _decoupled_ac(4, voters)
    lines = [_avarr(1, 0, 0, 0, 300), _avarr(2, 0, 0, 0, 300), _avarr(3, 0, 0, 0, 350), _avarr(0, 0, 0, 0, 260)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_ac_votes(pubs, arrs, data)
    assert res.leaked == [(0, 0, -1, 0, 0)]
    assert not res.ok


def test_ac_vote_fraction_voted_block():
    # publish-side fraction only: two of three voters saw the block + columns in time.
    voters = [(0, 0), (1, 1), (2, 2)]
    data = _decoupled_ac(4, voters)
    lines = [_avpub(0, 0, 0, 100, True), _avpub(0, 1, 1, 100, True), _avpub(0, 2, 2, 100, False)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_ac_votes(pubs, arrs, data)
    assert res.published == 3
    assert abs(res.fraction_voted_block - 2 / 3) < 1e-9


def test_analyze_ac_votes_csv_coverage_ok(tmp_path):
    # simnet CSV path (kind=7, attester=val, no origin column — origin comes from ac_voters).
    voters = [(0, 0), (1, 1)]
    data = _decoupled_ac(4, voters)
    csv_path = tmp_path / "simnet_arrivals.csv"
    rows = ["node,slot,kind,subnet,attester,delay_ms,voted_block"]
    for val, nd in voters:
        for node in sorted(set(range(4)) - {nd}):
            rows.append(f"{node},0,7,-1,{val},90,true")
    csv_path.write_text("\n".join(rows) + "\n")
    res = ca.analyze_ac_votes_csv(csv_path, data)
    assert res.expected == 6 and res.arrivals == 6
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok and res.fraction_voted_block == 1.0


def test_analyze_ac_votes_csv_detects_missing_and_leak(tmp_path):
    voters = [(0, 0)]  # single voter val0@node0
    data = _decoupled_ac(4, voters)
    csv_path = tmp_path / "a.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "1,0,7,-1,0,90,true\n"  # nodes 2,3 never receive -> missing
        "0,0,7,-1,0,90,true\n"  # node 0 is the publisher -> leak (loopback not skipped)
    )
    res = ca.analyze_ac_votes_csv(csv_path, data)
    assert res.missing == [(2, 0, -1, 0, 0), (3, 0, -1, 0, 0)]
    assert res.leaked == [(0, 0, -1, 0, 0)]
    assert not res.ok


# --- finality votes (kind=8, per finality-subnet, distinct per (subnet, val)) ----------
#
# Each node emits ONE finality vote per validator it hosts on its subnet (the validator rides the
# attester field, its host rides origin). It must reach exactly finality_subscribers(subnet) \
# {host} — missing/leaked/duplicate all fail. The finality slot rides the slot field.
# Dissemination-only: no vote bool / no fraction column. V>N ⇒ a host emits several votes (val%N).


def _fvpub(fslot, subnet, val, origin, t_ms):
    return (
        f'{{"msg":"publish","kind":8,"slot":{fslot},"subnet":{subnet},"attester":{val},'
        f'"origin":{origin},"voted_block":false,"t_ns":{t_ms * MS}}}'
    )


def _fvarr(node, fslot, subnet, val, origin, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":8,"slot":{fslot},"subnet":{subnet},'
        f'"attester":{val},"origin":{origin},"t_ns":{t_ms * MS}}}'
    )


def test_finality_vote_full_coverage_ok():
    # subnet 0 members {1,2,3}; val 5 hosted by node 1 publishes, reaches {2,3}.
    subs = {0: {1, 2, 3}}
    lines = [_fvpub(0, 0, 5, 1, 0), _fvarr(2, 0, 0, 5, 1, 200), _fvarr(3, 0, 0, 5, 1, 250)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_votes(pubs, arrs, subs)
    assert res.expected == 2 and res.arrivals == 2 and res.published == 1
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok


def test_finality_vote_detects_missing():
    subs = {0: {1, 2, 3}}
    lines = [_fvpub(0, 0, 5, 1, 0), _fvarr(2, 0, 0, 5, 1, 200)]  # member 3 missing
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_votes(pubs, arrs, subs)
    assert res.missing == [(3, 0, 0, 5)]
    assert not res.ok


def test_finality_vote_detects_leak():
    # node 9 is not a member of subnet 0 — receiving the vote is a leak.
    subs = {0: {1, 2, 3}}
    lines = [_fvpub(0, 0, 5, 1, 0), _fvarr(2, 0, 0, 5, 1, 200), _fvarr(3, 0, 0, 5, 1, 200), _fvarr(9, 0, 0, 5, 1, 200)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_votes(pubs, arrs, subs)
    assert res.leaked == [(9, 0, 0, 5)]
    assert not res.ok


def test_finality_vote_per_validator_multiplicity():
    # node 1 hosts val 1 and val 5 on subnet 0 {1,2,3}; each vote reaches {2,3} -> 2 distinct votes.
    subs = {0: {1, 2, 3}}
    lines = [
        _fvpub(0, 0, 1, 1, 0), _fvpub(0, 0, 5, 1, 0),
        _fvarr(2, 0, 0, 1, 1, 200), _fvarr(3, 0, 0, 1, 1, 200),
        _fvarr(2, 0, 0, 5, 1, 210), _fvarr(3, 0, 0, 5, 1, 210),
    ]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_votes(pubs, arrs, subs)
    assert res.published == 2 and res.expected == 4 and res.arrivals == 4
    assert res.ok


def _decoupled_fc(n, v, finality_subscribers, k=1, num_slots=1):
    return {
        "params": {"n": n, "v": v, "ac_slots_per_finality_slot": k},
        "subnet_subscribers": [],
        "finality_subscribers": finality_subscribers,
        "slots": [{"slot": s, "committees": [], "subnet_of": [], "proposer": 0}
                  for s in range(num_slots)],
    }


def test_analyze_finality_votes_csv_coverage_ok(tmp_path):
    # 4 nodes, V=4 (val==node), subnet 0 {0,2}, subnet 1 {1,3}. Each member's one vote reaches its
    # subnet mate. fslot 0 (k=1). Expected = 2 subnets × 1 mate each = 4 (2 votes × 1).
    data = _decoupled_fc(4, 4, [[0, 2], [1, 3]])
    csv_path = tmp_path / "simnet_arrivals.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "2,0,8,0,0,40,false\n"  # val 0 (host 0) reaches mate 2
        "0,0,8,0,2,40,false\n"  # val 2 (host 2) reaches mate 0
        "3,0,8,1,1,40,false\n"  # val 1 (host 1) reaches mate 3
        "1,0,8,1,3,40,false\n"  # val 3 (host 3) reaches mate 1
    )
    res = ca.analyze_finality_votes_csv(csv_path, data)
    assert res.expected == 4 and res.arrivals == 4 and res.published == 4
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok


def test_analyze_finality_votes_csv_with_validator_counts(tmp_path):
    # The Dist seam: validator_counts [2,1,1] ⇒ node0 hosts vals {0,1}, node1 {2}, node2 {3}
    # (contiguous ids, NOT val % N). Subnet 0 {0,1}: node0's two votes reach node1, node1's
    # vote reaches node0. Subnet 1 {2} has no mates ⇒ val 3 expects 0 arrivals.
    data = _decoupled_fc(3, 4, [[0, 1], [2]])
    data["validator_counts"] = [2, 1, 1]
    csv_path = tmp_path / "simnet_arrivals.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "1,0,8,0,0,40,false\n"  # val 0 (host 0) reaches mate 1
        "1,0,8,0,1,40,false\n"  # val 1 (ALSO host 0 under counts) reaches mate 1
        "0,0,8,0,2,40,false\n"  # val 2 (host 1) reaches mate 0
    )
    res = ca.analyze_finality_votes_csv(csv_path, data)
    assert res.expected == 3 and res.arrivals == 3 and res.published == 4
    assert res.missing == [] and res.leaked == [] and res.duplicates == []
    assert res.ok
    # Under the uniform fallback the same arrivals would be wrong (val 1 would host on node 1,
    # making its arrival at node 1 a self-receive and val 2's host node 2 — a different plan).
    del data["validator_counts"]
    res = ca.analyze_finality_votes_csv(csv_path, data)
    assert not res.ok


def test_analyze_finality_votes_csv_detects_missing_and_leak(tmp_path):
    # subnet 0 {1,2,3}, V=N=4 so vals 1,2,3 host on subnet 0; node 0 alone on subnet 1.
    data = _decoupled_fc(4, 4, [[1, 2, 3], [0]])
    csv_path = tmp_path / "a.csv"
    csv_path.write_text(
        "node,slot,kind,subnet,attester,delay_ms,voted_block\n"
        "2,0,8,0,1,40,false\n"  # val 1 (host 1) reaches only 2 (member 3 missing)
        "9,0,8,0,1,40,false\n"  # node 9 not a member -> leak
    )
    res = ca.analyze_finality_votes_csv(csv_path, data)
    assert (3, 0, 0, 1) in res.missing  # val 1 -> member 3 missing
    assert res.leaked == [(9, 0, 0, 1)]
    assert not res.ok


def test_load_finality_subscribers(tmp_path):
    (tmp_path / "schedule.json").write_text(
        '{"subnet_subscribers": [], "finality_subscribers": [[0,2],[1,3]], '
        '"slots": [{"slot":0,"committees":[],"subnet_of":[]}]}'
    )
    assert ca.load_finality_subscribers(tmp_path) == {0: {0, 2}, 1: {1, 3}}


def test_load_finality_subscribers_none_without_decoupled(tmp_path):
    (tmp_path / "schedule.json").write_text('{"subnet_subscribers": [], "slots": []}')
    assert ca.load_finality_subscribers(tmp_path) is None


# --- finality aggregates (kind=9, global topic, distinct per aggregator) ---------------
#
# Each finality-subnet aggregator publishes ONE distinct aggregate (the aggregator rides attester,
# origin -1) on the global topic; it must reach every node EXCEPT that aggregator, exactly once —
# the aggregate twin. finality_aggregators rides the finality-BOUNDARY AC slot (slot % k == 0),
# indexed by subnet; the finality slot = slot // k rides the arrival's slot field.


def _faarr(node, fslot, subnet, aggregator, t_ms):
    return (
        f'{{"msg":"arrival","node":{node},"kind":9,"slot":{fslot},"subnet":{subnet},'
        f'"attester":{aggregator},"origin":-1,"t_ns":{t_ms * MS}}}'
    )


def _fapub(fslot, subnet, aggregator, t_ms):
    return (
        f'{{"msg":"publish","kind":9,"slot":{fslot},"subnet":{subnet},"attester":{aggregator},'
        f'"origin":-1,"voted_block":false,"t_ns":{t_ms * MS}}}'
    )


def _decoupled_fc_agg(n, finality_aggregators, k=2, num_slots=None):
    # finality_aggregators (indexed by subnet) rides boundary slot 0 (k·0); finality slot 0.
    num_slots = num_slots if num_slots is not None else k
    slots = []
    for s in range(num_slots):
        sp = {"slot": s, "committees": [], "subnet_of": [], "proposer": 0}
        if s % k == 0:
            sp["finality_aggregators"] = finality_aggregators
        slots.append(sp)
    return {
        "params": {"n": n, "v": n, "ac_slots_per_finality_slot": k},
        "subnet_subscribers": [],
        "finality_subscribers": [list(range(n))],
        "slots": slots,
    }


def test_finality_aggregate_full_coverage_ok():
    # subnet 0 aggregators {0,1}, N=4, k=2 ⇒ finality slot 0. Each reaches every node but itself.
    data = _decoupled_fc_agg(4, [[0, 1]], k=2)
    lines = [_fapub(0, 0, 0, 800), _fapub(0, 0, 1, 800)] + [
        _faarr(node, 0, 0, agg, 900) for agg in (0, 1) for node in set(range(4)) - {agg}
    ]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_aggregates(pubs, arrs, data)
    assert res.expected == 6 and res.arrivals == 6 and res.published == 2
    assert res.ok


def test_finality_aggregate_two_subnets():
    # subnet 0 aggregators {0,1}, subnet 1 aggregators {3,4}, N=6, k=2 ⇒ 4·(6−1)=20, published 4.
    data = _decoupled_fc_agg(6, [[0, 1], [3, 4]], k=2)
    lines = [
        _faarr(node, 0, sub, agg, 900)
        for sub, aggs in enumerate([[0, 1], [3, 4]])
        for agg in aggs
        for node in set(range(6)) - {agg}
    ]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_aggregates(pubs, arrs, data)
    assert res.expected == 20 and res.arrivals == 20 and res.published == 4
    assert res.ok


def test_finality_aggregate_detects_missing():
    data = _decoupled_fc_agg(4, [[0]], k=2)  # aggregator 0; reaches {1,2,3}
    lines = [_faarr(1, 0, 0, 0, 900), _faarr(2, 0, 0, 0, 900)]  # node 3 missing
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_aggregates(pubs, arrs, data)
    assert res.missing == [(3, 0, 0, 0)]
    assert not res.ok


def test_finality_aggregate_detects_leak_at_own_aggregator():
    data = _decoupled_fc_agg(4, [[0]], k=2)
    lines = [_faarr(1, 0, 0, 0, 900), _faarr(2, 0, 0, 0, 900), _faarr(3, 0, 0, 0, 950), _faarr(0, 0, 0, 0, 860)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_aggregates(pubs, arrs, data)
    assert res.leaked == [(0, 0, 0, 0)]  # node 0 recorded its own aggregate (loopback not skipped)
    assert not res.ok


def test_finality_aggregate_finality_slot_from_boundary():
    # Boundary slot 2 (k=2) carries the aggregators ⇒ finality slot 1; the arrival's slot is 1.
    data = _decoupled_fc_agg(4, [[0]], k=2, num_slots=3)
    data["slots"][0].pop("finality_aggregators")  # only the slot-2 boundary has them
    data["slots"][2]["finality_aggregators"] = [[0]]
    lines = [_faarr(node, 1, 0, 0, 900) for node in (1, 2, 3)]
    pubs, arrs = ca.parse_events(lines)
    res = ca.analyze_finality_aggregates(pubs, arrs, data)
    assert res.published == 1 and res.expected == 3 and res.arrivals == 3
    assert res.ok


def test_analyze_finality_aggregates_csv_coverage_ok(tmp_path):
    data = _decoupled_fc_agg(4, [[0, 1]], k=2)
    csv_path = tmp_path / "simnet_arrivals.csv"
    rows = ["node,slot,kind,subnet,attester,delay_ms,voted_block"]
    for agg in (0, 1):
        for node in sorted(set(range(4)) - {agg}):
            rows.append(f"{node},0,9,0,{agg},90,false")
    csv_path.write_text("\n".join(rows) + "\n")
    res = ca.analyze_finality_aggregates_csv(csv_path, data)
    assert res.expected == 6 and res.arrivals == 6 and res.ok
