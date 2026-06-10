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


# Params of the committed decoupled Go contract fixture (schedule/testdata/decoupled_schedule.json).
_DECOUPLED_FIXTURE = Path(__file__).parent.parent / "schedule" / "testdata" / "decoupled_schedule.json"
_DECOUPLED_FIXTURE_PARAMS = schedule.Params(
    n=12, v=24, c=0, sc=0, num_columns=4, full_custody_fraction=0.5, column_backbone_floor=3,
    decoupled=True, ac_vote_size=4, ac_slots_per_finality_slot=2, fs_subnets=3, fs_aggregators=2,
    seed=1, num_slots=4,
)


def test_committed_decoupled_fixture_is_current():
    # The Go contract test (schedule/schedule_test.go) loads this exact file; regenerate if stale.
    on_disk = json.loads(_DECOUPLED_FIXTURE.read_text())
    got = schedule.generate(_DECOUPLED_FIXTURE_PARAMS, supers=list(range(8))).to_dict()
    assert got == on_disk, "stale decoupled fixture; regenerate it"


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


# --- sync-committee membership (node-based, stable for the run) ----------------
#
# A seeded subset of sync_size member nodes, each assigned one of sync_subnets subnets
# round-robin (so subnets are even); per slot, sync_target_aggregators members per subnet
# publish a contribution. Members subscribe their one subnet — no backbone, one message each.


def test_sync_subscribers_partition_even_and_in_range():
    p = schedule.Params(
        n=40, v=80, c=2, sc=4, sync_size=20, sync_subnets=4, num_slots=2
    )
    a = schedule.generate(p)
    assert a.sync_subscribers is not None
    assert len(a.sync_subscribers) == p.sync_subnets
    flat = [m for subs in a.sync_subscribers for m in subs]
    # Exactly sync_size distinct member nodes, in range, each on exactly one subnet.
    assert len(flat) == p.sync_size
    assert len(set(flat)) == p.sync_size
    assert all(0 <= m < p.n for m in flat)
    # Subnets are even: round-robin ⇒ each holds floor or ceil of size/subnets, sorted/distinct.
    lo, hi = divmod(p.sync_size, p.sync_subnets)
    for subs in a.sync_subscribers:
        assert len(subs) in (lo, lo + (1 if hi else 0))
        assert subs == sorted(set(subs))


def test_sync_size_exceeds_n_raises():
    with pytest.raises(ValueError):
        schedule.generate(schedule.Params(n=8, v=16, c=1, sc=2, sync_size=10, sync_subnets=2))


def test_sync_subnets_exceeds_size_raises():
    with pytest.raises(ValueError):
        schedule.generate(schedule.Params(n=20, v=40, c=1, sc=2, sync_size=4, sync_subnets=8))


def test_sync_aggregators_are_subscribers_and_clamped():
    # target > a subnet's member count ⇒ every member aggregates (clamped); else exactly target.
    p = schedule.Params(
        n=40, v=80, c=2, sc=4, sync_size=20, sync_subnets=4,
        sync_target_aggregators=3, num_slots=3,
    )
    a = schedule.generate(p)
    for sp in a.slots:
        assert sp.sync_aggregators is not None
        assert len(sp.sync_aggregators) == p.sync_subnets
        for i, aggs in enumerate(sp.sync_aggregators):
            subs = a.sync_subscribers[i]
            assert len(aggs) == min(p.sync_target_aggregators, len(subs))
            assert aggs == sorted(set(aggs))
            assert set(aggs) <= set(subs)


def test_sync_off_keeps_schedule_json_unchanged():
    # sync_subnets=0 (off) ⇒ no sync keys in schedule.json (back-compat with non-sync runs).
    d = schedule.generate(schedule.Params(n=8, v=16, c=2, sc=4, num_slots=1), supers=[0, 1]).to_dict()
    assert "sync_subscribers" not in d
    assert all("sync_aggregators" not in s for s in d["slots"])


def test_sync_same_seed_identical_seed_plus_one_differs():
    base = schedule.Params(
        n=40, v=80, c=2, sc=4, sync_size=16, sync_subnets=4, seed=7, num_slots=3
    )
    assert schedule.generate(base).to_dict() == schedule.generate(base).to_dict()
    other = schedule.Params(
        n=40, v=80, c=2, sc=4, sync_size=16, sync_subnets=4, seed=8, num_slots=3
    )
    assert schedule.generate(other).to_dict() != schedule.generate(base).to_dict()


# --- decoupled-consensus membership (availability + finality chains) -----------
#
# Decoupled mode (p.decoupled) replaces committees + sync with: a per-slot flat AC-voter
# draw (ac_vote_size validators, one global topic), a node-partition into fs_subnets
# finality subnets (every node on exactly one), and per-finality-slot fc_aggregators on
# the boundary AC slot. Data columns are required (they gate the AC vote).


def _decoupled_params(**kw):
    base = dict(
        n=16, v=32, c=0, sc=0, num_columns=8, full_custody_fraction=0.5,
        column_backbone_floor=3, decoupled=True, ac_vote_size=8,
        ac_slots_per_finality_slot=2, fs_subnets=2, fs_aggregators=2, seed=1, num_slots=4,
    )
    base.update(kw)
    return schedule.Params(**base)


_DECOUPLED_SUPERS = list(range(8))  # F = round(0.5*8) = 4 full-custody nodes (>= backbone floor)


def test_finality_subscribers_partition_all_nodes():
    a = schedule.generate(_decoupled_params(), supers=_DECOUPLED_SUPERS)
    assert a.finality_subscribers is not None
    assert len(a.finality_subscribers) == 4 or len(a.finality_subscribers) == 2  # fs_subnets
    flat = [node for subs in a.finality_subscribers for node in subs]
    # Every node on exactly one subnet — a partition of all N (not a sample).
    assert sorted(flat) == list(range(16))
    for subs in a.finality_subscribers:
        assert subs == sorted(set(subs))
        assert all(0 <= node < 16 for node in subs)


def test_validators_per_subnet_sums_to_v():
    a = schedule.generate(_decoupled_params(), supers=_DECOUPLED_SUPERS)
    assert a.validators_per_subnet is not None
    assert len(a.validators_per_subnet) == 2
    assert sum(a.validators_per_subnet) == 32  # every validator votes on exactly one subnet
    # Each subnet's count = Σ over member nodes of validators that node hosts (uniform v % N).
    for subnet, members in enumerate(a.finality_subscribers):
        want = sum((32 - 1 - node) // 16 + 1 for node in members)
        assert a.validators_per_subnet[subnet] == want


def test_ac_voters_size_and_fresh_per_slot():
    a = schedule.generate(_decoupled_params(), supers=_DECOUPLED_SUPERS)
    seen = []
    for sp in a.slots:
        assert sp.ac_voters is not None
        assert len(sp.ac_voters) == 8  # ac_vote_size distinct validators
        vals = [r.val for r in sp.ac_voters]
        assert len(set(vals)) == 8
        assert all(0 <= r.val < 32 and r.node == r.val % 16 for r in sp.ac_voters)
        seen.append(tuple(sorted(vals)))
        # No committees/subnets on the AC.
        assert sp.committees == [] and sp.subnet_of == [] and sp.aggregators == []
    assert len(set(seen)) > 1, "AC voters should be re-drawn per slot"


def test_fc_aggregators_on_boundary_only_and_clamped():
    a = schedule.generate(_decoupled_params(), supers=_DECOUPLED_SUPERS)
    for sp in a.slots:
        if sp.slot % 2 == 0:  # finality boundary (k=2)
            assert sp.finality_aggregators is not None
            assert len(sp.finality_aggregators) == 2  # fs_subnets
            for subnet, aggs in enumerate(sp.finality_aggregators):
                members = a.finality_subscribers[subnet]
                assert len(aggs) == min(2, len(members))  # fs_aggregators, clamped
                assert aggs == sorted(set(aggs))
                assert set(aggs) <= set(members)
        else:
            assert sp.finality_aggregators is None  # only the boundary slot carries them


def test_decoupled_requires_columns():
    with pytest.raises(ValueError):
        schedule.generate(_decoupled_params(num_columns=0), supers=_DECOUPLED_SUPERS)


def test_ac_vote_size_exceeds_v_raises():
    with pytest.raises(ValueError):
        schedule.generate(_decoupled_params(ac_vote_size=100), supers=_DECOUPLED_SUPERS)


def test_fs_subnets_exceeds_n_raises():
    with pytest.raises(ValueError):
        schedule.generate(_decoupled_params(fs_subnets=100), supers=_DECOUPLED_SUPERS)


def test_decoupled_proposers_are_full_custody():
    a = schedule.generate(_decoupled_params(), supers=_DECOUPLED_SUPERS)
    assert a.full_custody is not None
    for sp in a.slots:
        assert sp.proposer in a.full_custody  # proposer originates all columns


def test_decoupled_off_keeps_schedule_json_unchanged():
    # decoupled=False ⇒ no decoupled keys in schedule.json (back-compat).
    d = schedule.generate(schedule.Params(n=8, v=16, c=2, sc=4, num_slots=1), supers=[0, 1]).to_dict()
    assert "finality_subscribers" not in d and "validators_per_subnet" not in d
    assert all("ac_voters" not in s and "finality_aggregators" not in s for s in d["slots"])
    assert "ac_vote_size" not in d["params"]


def test_decoupled_same_seed_identical_seed_plus_one_differs():
    base = _decoupled_params(seed=7)
    assert (
        schedule.generate(base, supers=_DECOUPLED_SUPERS).to_dict()
        == schedule.generate(base, supers=_DECOUPLED_SUPERS).to_dict()
    )
    other = _decoupled_params(seed=8)
    assert (
        schedule.generate(other, supers=_DECOUPLED_SUPERS).to_dict()
        != schedule.generate(base, supers=_DECOUPLED_SUPERS).to_dict()
    )


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


# --- validator distribution (the Dist seam; skewed-validators-spec.md) ----------
#
# tiered/explicit replace the uniform v % N map: per-node counts ride schedule.json
# (validator_counts), validator ids are contiguous by node, and V is emergent (Σ counts).


def _tiered_params(**kw):
    return _decoupled_params(dist="tiered", **kw)


def test_tiered_counts_tiers_sum_and_emergent_v():
    a = schedule.generate(_tiered_params(), supers=_DECOUPLED_SUPERS)
    counts = a.validator_counts
    assert counts is not None and len(counts) == 16
    supers = set(_DECOUPLED_SUPERS)
    for node, c in enumerate(counts):
        if node in supers:
            assert 1 <= c <= 1000
        else:
            assert c in (1, 2, 3)
    assert a.params.v == sum(counts)  # V is emergent
    d = a.to_dict()
    assert d["validator_counts"] == counts
    assert d["params"]["v"] == sum(counts)


def test_tiered_refs_use_contiguous_ranges():
    # Every drawn ref (AC voters here) maps val -> the node whose contiguous range holds it.
    a = schedule.generate(_tiered_params(), supers=_DECOUPLED_SUPERS)
    counts = a.validator_counts
    starts = [0]
    for c in counts:
        starts.append(starts[-1] + c)
    for sp in a.slots:
        for r in sp.ac_voters:
            assert starts[r.node] <= r.val < starts[r.node + 1]
    # validators_per_subnet sums counts over members (and Σ over subnets = V).
    for subnet, members in enumerate(a.finality_subscribers):
        assert a.validators_per_subnet[subnet] == sum(counts[m] for m in members)
    assert sum(a.validators_per_subnet) == a.params.v


def test_tiered_deterministic_and_dist_seed_varies():
    a = schedule.generate(_tiered_params(), supers=_DECOUPLED_SUPERS)
    b = schedule.generate(_tiered_params(), supers=_DECOUPLED_SUPERS)
    assert a.validator_counts == b.validator_counts
    c = schedule.generate(_tiered_params(dist_seed=8), supers=_DECOUPLED_SUPERS)
    assert a.validator_counts != c.validator_counts


def test_tiered_super_mean_calibrated():
    # Large fleet: the supernode tier's empirical mean lands near super_mean (the μ solve).
    p = _tiered_params(n=1000, fs_subnets=4)
    supers = list(range(500))
    a = schedule.generate(p, supers=supers)
    super_counts = [a.validator_counts[node] for node in supers]
    mean = sum(super_counts) / len(super_counts)
    assert 0.85 * 200 <= mean <= 1.15 * 200, mean


def test_tiered_requires_supers():
    try:
        schedule.generate(_tiered_params(), supers=[])
    except ValueError as e:
        assert "supernodes" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_explicit_counts_verbatim_and_length_checked():
    counts = tuple(range(1, 17))  # node i hosts i+1 validators
    a = schedule.generate(
        _decoupled_params(dist="explicit", explicit_counts=counts), supers=_DECOUPLED_SUPERS
    )
    assert a.validator_counts == list(counts)
    assert a.params.v == sum(counts)
    try:
        schedule.generate(
            _decoupled_params(dist="explicit", explicit_counts=(1, 2)), supers=_DECOUPLED_SUPERS
        )
    except ValueError as e:
        assert "length N" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_uniform_keeps_schedule_json_unchanged():
    a = schedule.generate(_decoupled_params(), supers=_DECOUPLED_SUPERS)
    assert a.validator_counts is None
    assert "validator_counts" not in a.to_dict()
