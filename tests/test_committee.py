"""M0: the committee assignment generator — exact-count invariants from a seed.

The assignment is the highest-leverage layer (every later expected-count derives from
it), so it is tested exhaustively before any network exists. Stdlib + pytest only.
"""

import json
from collections import Counter
from pathlib import Path

import pytest

from simctl import committee

# Params of the committed Go contract-test fixture (committee/testdata/committee.json).
_FIXTURE = Path(__file__).parent.parent / "committee" / "testdata" / "committee.json"
_FIXTURE_PARAMS = committee.Params(
    n=6, v=12, c=2, sc=3, subnets_per_node=1, subscribe_floor=2, seed=1, num_slots=1
)


def _attesters(slot: committee.SlotPlan) -> list[committee.AttesterRef]:
    return [ref for com in slot.committees for ref in com]


def test_v_to_node_is_a_partition():
    # val -> node is uniform i % N: a partition of V over N with no overlap.
    p = committee.Params(n=16, v=32, c=1, sc=8, num_slots=3)
    a = committee.generate(p)
    for slot in a.slots:
        for ref in _attesters(slot):
            assert ref.node == ref.val % p.n
            assert 0 <= ref.val < p.v


def test_each_slot_has_exactly_c_committees_of_sc():
    p = committee.Params(n=16, v=32, c=2, sc=4, num_slots=5)
    a = committee.generate(p)
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
        committee.generate(committee.Params(n=4, v=8, c=2, sc=8))


def test_committee_to_subnet_is_identity_bijection():
    p = committee.Params(n=8, v=64, c=4, sc=8, num_slots=2)
    a = committee.generate(p)
    for slot in a.slots:
        assert slot.subnet_of == list(range(p.c))  # identity into subnets 0..C-1
        for com_idx, com in enumerate(slot.committees):
            for ref in com:
                assert ref.subnet == com_idx


def test_too_many_committees_for_subnets_raise():
    with pytest.raises(ValueError):
        committee.generate(committee.Params(n=200, v=200, c=65, sc=1, subnet_count=64))


def test_subnet_subscribers_meet_floor_and_in_range():
    p = committee.Params(n=30, v=60, c=4, sc=4, subnets_per_node=2, subscribe_floor=10)
    a = committee.generate(p)
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
    a = committee.generate(committee.Params(n=5, v=10, c=1, sc=2, subscribe_floor=10))
    assert len(a.subnet_subscribers[0]) == 5


def test_same_seed_identical_seed_plus_one_differs():
    base = committee.Params(n=16, v=64, c=2, sc=8, seed=7, num_slots=3)
    assert committee.generate(base).to_dict() == committee.generate(base).to_dict()
    other = committee.Params(n=16, v=64, c=2, sc=8, seed=8, num_slots=3)
    assert committee.generate(other).to_dict() != committee.generate(base).to_dict()


def test_direct_knobs_are_honored_not_rederived():
    # A non-mainnet point (V/32 != C*s_c) is allowed and used verbatim.
    p = committee.Params(n=10, v=100, c=3, sc=5, num_slots=1)
    a = committee.generate(p)
    assert len(a.slots[0].committees) == 3
    assert all(len(com) == 5 for com in a.slots[0].committees)


def test_committed_go_fixture_is_current():
    # The Go contract test (committee/committee_test.go) loads this exact file; if the
    # generator changes, regenerate it.
    on_disk = json.loads(_FIXTURE.read_text())
    assert committee.generate(_FIXTURE_PARAMS).to_dict() == on_disk, "stale fixture; regenerate it"


def test_params_from_v_fills_the_mainnet_formula():
    # C = min(64, V/4096); s_c = (V/32)/C.
    p = committee.params_from_v(v=4096, n=64)
    assert p.c == 1 and p.sc == 128
    big = committee.params_from_v(v=64 * 4096, n=1000)  # 262144 validators
    assert big.c == 64 and big.sc == (big.v // 32) // 64
    assert big.c * big.sc <= big.v
