"""M0: the schedule assignment generator — exact-count invariants from a seed.

The assignment is the highest-leverage layer (every later expected-count derives from
it), so it is tested exhaustively before any network exists. Stdlib + pytest only.
"""

import json
from collections import Counter
from pathlib import Path

import pytest

from simctl import schedule

# Params of the committed Go contract-test fixture (schedule/testdata/schedule.json).
_FIXTURE = Path(__file__).parent.parent / "schedule" / "testdata" / "schedule.json"
_FIXTURE_PARAMS = schedule.Params(
    n=6, v=12, c=2, sc=3, subnets_per_node=1, subscribe_floor=2, seed=1, num_slots=1
)


def _attesters(slot: schedule.SlotPlan) -> list[schedule.AttesterRef]:
    return [ref for com in slot.committees for ref in com]


def test_v_to_node_is_a_partition():
    # val -> node is uniform i % N: a partition of V over N with no overlap.
    p = schedule.Params(n=16, v=32, c=1, sc=8, num_slots=3)
    a = schedule.generate(p)
    for slot in a.slots:
        for ref in _attesters(slot):
            assert ref.node == ref.val % p.n
            assert 0 <= ref.val < p.v


def test_each_slot_has_exactly_c_committees_of_sc():
    p = schedule.Params(n=16, v=32, c=2, sc=4, num_slots=5)
    a = schedule.generate(p)
    assert len(a.slots) == p.num_slots
    for slot in a.slots:
        assert len(slot.committees) == p.c
        for com in slot.committees:
            assert len(com) == p.sc
            assert [ref.position for ref in com] == list(range(p.sc))
        attesters = _attesters(slot)
        assert len(attesters) == p.c * p.sc
        assert len({ref.val for ref in attesters}) == p.c * p.sc


def test_oversized_committees_raise():
    with pytest.raises(ValueError):
        schedule.generate(schedule.Params(n=4, v=8, c=2, sc=8))


def test_committee_to_subnet_is_identity_bijection():
    p = schedule.Params(n=8, v=64, c=4, sc=8, num_slots=2)
    a = schedule.generate(p)
    for slot in a.slots:
        assert slot.subnet_of == list(range(p.c))  # identity into subnets 0..C-1
        for com_idx, com in enumerate(slot.committees):
            for ref in com:
                assert ref.subnet == com_idx


def test_too_many_committees_for_subnets_raise():
    with pytest.raises(ValueError):
        schedule.generate(schedule.Params(n=200, v=200, c=65, sc=1, subnet_count=64))


def test_subnet_subscribers_meet_floor_and_in_range():
    p = schedule.Params(n=30, v=60, c=4, sc=4, subnets_per_node=2, subscribe_floor=10)
    a = schedule.generate(p)
    assert len(a.subnet_subscribers) == p.c
    for members in a.subnet_subscribers:
        assert len(members) >= min(p.subscribe_floor, p.n)  # floor per active subnet
        assert members == sorted(set(members))  # sorted, distinct
        assert all(0 <= m < p.n for m in members)
    # Every node subscribes at least one subnet (~subnets_per_node, more after top-up).
    cnt = Counter(m for members in a.subnet_subscribers for m in members)
    assert all(node in cnt for node in range(p.n))


def test_subnet_subscribers_floor_capped_at_n():
    # floor > N can't be met — cap at N (can't have more subscribers than nodes).
    a = schedule.generate(schedule.Params(n=5, v=10, c=1, sc=2, subscribe_floor=10))
    assert len(a.subnet_subscribers[0]) == 5


def test_same_seed_identical_seed_plus_one_differs():
    base = schedule.Params(n=16, v=64, c=2, sc=8, seed=7, num_slots=3)
    assert schedule.generate(base).to_dict() == schedule.generate(base).to_dict()
    other = schedule.Params(n=16, v=64, c=2, sc=8, seed=8, num_slots=3)
    assert schedule.generate(other).to_dict() != schedule.generate(base).to_dict()


def test_direct_knobs_are_honored_not_rederived():
    # A non-mainnet point (V/32 != C*s_c) is allowed and used verbatim.
    p = schedule.Params(n=10, v=100, c=3, sc=5, num_slots=1)
    a = schedule.generate(p)
    assert len(a.slots[0].committees) == 3
    assert all(len(com) == 5 for com in a.slots[0].committees)


def test_committed_go_fixture_is_current():
    # The Go contract test (schedule/committee_test.go) loads this exact file; if the
    # generator changes, regenerate it.
    on_disk = json.loads(_FIXTURE.read_text())
    assert schedule.generate(_FIXTURE_PARAMS).to_dict() == on_disk, "stale fixture; regenerate it"


def test_proposers_are_supernodes_round_robin():
    # Block proposers are drawn only from the supernode set, round-robin over its sorted ids.
    p = schedule.Params(n=10, v=40, c=2, sc=4, num_slots=5)
    supers = [9, 3, 5]
    a = schedule.generate(p, supers=supers)
    pool = sorted(supers)
    for sp in a.slots:
        assert sp.proposer == pool[sp.slot % len(pool)]
        assert sp.proposer in supers
    assert [s["proposer"] for s in a.to_dict()["slots"]] == [
        pool[i % len(pool)] for i in range(p.num_slots)
    ]


def test_proposers_fall_back_to_cyclic_without_supernodes():
    p = schedule.Params(n=4, v=16, c=1, sc=2, num_slots=6)
    want = [i % p.n for i in range(p.num_slots)]
    assert [s.proposer for s in schedule.generate(p).slots] == want
    assert [s.proposer for s in schedule.generate(p, supers=[]).slots] == want


def test_proposers_match_topology_supernodes():
    # The proposer pool is exactly topology.supernode_ids(...) — the single source of truth.
    from simctl import topology

    p = schedule.Params(n=20, v=80, c=2, sc=4, num_slots=8)
    supers = topology.supernode_ids(p.n, 0.2, seed=42)
    assert supers, "fixture needs supernodes"
    a = schedule.generate(p, supers=sorted(supers))
    for sp in a.slots:
        assert sp.proposer in supers


def test_params_from_v_fills_the_mainnet_formula():
    # C = min(64, V/4096); s_c = (V/32)/C.
    p = schedule.params_from_v(v=4096, n=64)
    assert p.c == 1 and p.sc == 128
    big = schedule.params_from_v(v=64 * 4096, n=1000)  # 262144 validators
    assert big.c == 64 and big.sc == (big.v // 32) // 64
    assert big.c * big.sc <= big.v


# --- aggregators (the t≈8s phase) ---------------------------------------------
#
# An aggregator is, for now, a node drawn from its schedule's subnet subscribers
# (so it already receives the attestations). Each schedule gets
# min(target_aggregators, |subscribers|) of them, seeded per slot.


def test_aggregators_are_subscribers_and_meet_target_count():
    p = schedule.Params(
        n=40, v=80, c=4, sc=4, subnets_per_node=2, subscribe_floor=10,
        target_aggregators=6, num_slots=3,
    )
    a = schedule.generate(p)
    for slot in a.slots:
        assert len(slot.aggregators) == p.c
        for ci, aggs in enumerate(slot.aggregators):
            subs = a.subnet_subscribers[slot.subnet_of[ci]]
            assert len(aggs) == min(p.target_aggregators, len(subs))
            assert aggs == sorted(set(aggs))  # sorted, distinct
            assert set(aggs) <= set(subs)  # drawn only from subscribers


def test_aggregators_clamped_to_subscriber_count():
    # target_aggregators (default 16) > subscribers ⇒ every subscriber aggregates.
    p = schedule.Params(n=12, v=24, c=1, sc=2, subscribe_floor=5, num_slots=1)
    a = schedule.generate(p)
    subs = a.subnet_subscribers[0]
    assert len(subs) <= p.target_aggregators
    assert a.slots[0].aggregators[0] == subs


# --- data-column custody (the dissemination + gate phase) ---------------------
#
# Uniform custody: every ordinary node holds custody_floor columns (a seeded-random
# subset), full-custody nodes (F = round(full_custody_fraction · |supers|), a subset of
# the supernodes) hold all columns and form the relay backbone. Proposers are full-custody.


def test_column_subscribers_uniform_custody_and_backbone():
    supers = list(range(8))  # 8 supernodes
    p = schedule.Params(
        n=20, v=40, c=2, sc=4, num_columns=32, custody_floor=8,
        full_custody_fraction=0.5, column_backbone_floor=3, seed=1, num_slots=4,
    )
    a = schedule.generate(p, supers=supers)
    # F = round(0.5 * 8) = 4 full-custody nodes, a sorted subset of the supernodes.
    assert a.full_custody is not None and len(a.full_custody) == 4
    assert set(a.full_custody) <= set(supers)
    assert a.full_custody == sorted(a.full_custody)

    full = set(a.full_custody)
    assert len(a.column_subscribers) == p.num_columns
    for members in a.column_subscribers:
        assert full <= set(members)  # every column has the full-custody backbone
        assert members == sorted(set(members))
        assert all(0 <= m < p.n for m in members)

    held = Counter(m for members in a.column_subscribers for m in members)
    for node in range(p.n):
        want = p.num_columns if node in full else p.custody_floor
        assert held[node] == want  # uniform: full hold all, ordinary hold custody_floor

    for sp in a.slots:
        assert sp.proposer in full  # proposers originate all columns ⇒ full-custody


def test_full_custody_below_backbone_floor_raises():
    # F = round(0.5 * 4) = 2 < column_backbone_floor 3 ⇒ generation errors loudly.
    p = schedule.Params(
        n=10, v=20, c=1, sc=2, num_columns=16, full_custody_fraction=0.5,
        column_backbone_floor=3, num_slots=1,
    )
    with pytest.raises(ValueError):
        schedule.generate(p, supers=list(range(4)))


def test_columns_require_supernodes():
    # No supernodes ⇒ no full-custody backbone ⇒ the column network can't relay.
    p = schedule.Params(n=10, v=20, c=1, sc=2, num_columns=16, num_slots=1)
    with pytest.raises(ValueError):
        schedule.generate(p, supers=[])


def test_columns_off_keeps_committee_json_unchanged():
    # num_columns=0 (off) ⇒ no column keys in schedule.json (back-compat with non-column runs).
    p = schedule.Params(n=8, v=16, c=2, sc=4, num_slots=1)
    d = schedule.generate(p, supers=[0, 1]).to_dict()
    assert "column_subscribers" not in d and "full_custody" not in d and "num_columns" not in d


def test_aggregators_seeded_vary_across_slots_and_with_seed():
    base = schedule.Params(
        n=40, v=80, c=2, sc=4, subscribe_floor=20, target_aggregators=4, seed=7, num_slots=4
    )
    a = schedule.generate(base)
    # Different slots draw different aggregators (not all identical across slots).
    per_slot = [tuple(s.aggregators[0]) for s in a.slots]
    assert len(set(per_slot)) > 1, "aggregators should be re-drawn per slot"
    # Same seed ⇒ identical; seed+1 ⇒ differs.
    assert schedule.generate(base).slots[0].aggregators == a.slots[0].aggregators
    other = schedule.Params(
        n=40, v=80, c=2, sc=4, subscribe_floor=20, target_aggregators=4, seed=8, num_slots=4
    )
    assert schedule.generate(other).slots[0].aggregators != a.slots[0].aggregators
