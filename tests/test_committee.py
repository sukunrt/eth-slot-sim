"""M0: the committee assignment generator — exact-count invariants from a seed.

The assignment is the highest-leverage layer (every later expected-count derives from
it), so it is tested exhaustively before any network exists. Stdlib + pytest only.
"""

import json
from pathlib import Path

import pytest

from simctl import committee

# Params of the committed Go contract-test fixture (committee/testdata/committee.json).
# Keep in sync: if this changes, regenerate the fixture (see test below for the hint).
_FIXTURE = Path(__file__).parent.parent / "committee" / "testdata" / "committee.json"
_FIXTURE_PARAMS = committee.Params(
    n=4, v=8, c=1, sc=4, backbone_per_node=2, aggs_per_committee=2, seed=1, num_slots=2
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
        # attesters/slot == C * s_c, all validators distinct within the slot.
        attesters = _attesters(slot)
        assert len(attesters) == p.c * p.sc
        assert len({ref.val for ref in attesters}) == p.c * p.sc


def test_oversized_committees_raise():
    # C * s_c must be <= V: can't seat more committee positions than validators exist.
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


def test_backbone_is_exact_stable_and_in_range():
    p = committee.Params(n=20, v=40, c=1, sc=8, backbone_per_node=2, subnet_count=64)
    a = committee.generate(p)
    assert len(a.backbone) == p.n
    for subnets in a.backbone:
        assert len(subnets) == p.backbone_per_node
        assert len(set(subnets)) == p.backbone_per_node  # distinct
        assert all(0 <= s < p.subnet_count for s in subnets)
    # Stable: regenerating yields the same backbone (it is a function of node-id).
    assert committee.generate(p).backbone == a.backbone


def test_aggregators_are_exact_subset_of_committee():
    p = committee.Params(n=16, v=32, c=1, sc=8, aggs_per_committee=4, num_slots=2)
    a = committee.generate(p)
    for slot in a.slots:
        for com_idx, aggs in enumerate(slot.aggregators):
            assert len(aggs) == p.aggs_per_committee
            members = {ref.val for ref in slot.committees[com_idx]}
            assert {ref.val for ref in aggs} <= members
            assert len({ref.val for ref in aggs}) == len(aggs)  # distinct


def test_aggregators_cap_at_committee_size():
    # AggsPerCommittee > s_c can't seat more aggregators than members; cap at s_c.
    p = committee.Params(n=16, v=32, c=1, sc=4, aggs_per_committee=16, num_slots=1)
    a = committee.generate(p)
    assert len(a.slots[0].aggregators[0]) == p.sc


def test_subscribers_are_backbone_subs_union_aggregator_nodes():
    p = committee.Params(n=24, v=48, c=2, sc=4, backbone_per_node=2, aggs_per_committee=2)
    a = committee.generate(p)
    backbone_subs: dict[int, set[int]] = {}
    for node, subnets in enumerate(a.backbone):
        for s in subnets:
            backbone_subs.setdefault(s, set()).add(node)
    for slot in a.slots:
        for com_idx, subs in enumerate(slot.subscribers):
            subnet = slot.subnet_of[com_idx]
            agg_nodes = {ref.node for ref in slot.aggregators[com_idx]}
            want = backbone_subs.get(subnet, set()) | agg_nodes
            assert set(subs) == want
            assert subs == sorted(set(subs))  # sorted, deduped


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
    # The Go contract test (committee/committee_test.go) loads this exact file. If the
    # generator changes, regenerate it:
    #   uv run python -c "import json; from simctl import committee as c; \
    #     p=c.Params(n=4,v=8,c=1,sc=4,backbone_per_node=2,aggs_per_committee=2,seed=1,num_slots=2); \
    #     open('committee/testdata/committee.json','w').write(json.dumps(c.generate(p).to_dict(),indent=2)+chr(10))"
    on_disk = json.loads(_FIXTURE.read_text())
    assert committee.generate(_FIXTURE_PARAMS).to_dict() == on_disk, "stale fixture; regenerate it"


def test_params_from_v_fills_the_mainnet_formula():
    # C = min(64, V/4096); s_c = (V/32)/C.
    p = committee.params_from_v(v=4096, n=64)
    assert p.c == 1 and p.sc == 128
    big = committee.params_from_v(v=64 * 4096, n=1000)  # 262144 validators
    assert big.c == 64 and big.sc == (big.v // 32) // 64
    assert big.c * big.sc <= big.v
