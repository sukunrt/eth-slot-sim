"""Committee assignment — the topology seam (validators→nodes→committees→subnets).

Pure, seeded source of truth for the attestation phase. Two parts:

- the per-slot **committee draw**: each slot, C committees of s_c validators (who attest,
  on which subnet); and
- the stable **subnet subscribe set**: for each active subnet, ≥``subscribe_floor`` nodes
  (the receivers/relayers), so every active subnet has a real mesh core. Publishers dial
  into this set per slot; see att-subnet.md.

`V`, `C`, `s_c` are independent knobs (only `C·s_c ≤ V`). The result serializes to
``schedule.json`` (consumed by both backends + the analysis), so the assignment is
implemented once, here. Aggregation is out of scope for now.
"""

from __future__ import annotations

import math
import random
from bisect import bisect_right
from dataclasses import dataclass, replace
from itertools import accumulate
from typing import Callable


@dataclass(frozen=True)
class Params:
    n: int  # nodes
    v: int  # validators
    c: int  # committees per slot (= active attestation subnets)
    sc: int  # committee size (attesters per committee per slot)
    subnet_count: int = 64
    subnets_per_node: int = 2  # subnets a node subscribes (capped at C)
    subscribe_floor: int = 10  # min subscribers per active subnet
    target_aggregators: int = 16  # aggregators per committee (TARGET_AGGREGATORS_PER_COMMITTEE)
    seed: int = 1
    num_slots: int = 1
    # Data-columns phase (num_columns 0 ⇒ off). Uniform custody: every ordinary node holds
    # custody_floor columns (a seeded-random subset); full-custody nodes — F = round(
    # full_custody_fraction · |supers|), a subset of the supernodes — hold all columns and
    # form the relay backbone. See data-columns-spec.md.
    num_columns: int = 0
    custody_floor: int = 8
    full_custody_fraction: float = 0.5
    column_backbone_floor: int = 3  # min F so every column subnet has a backbone core
    per_subnet_floor: int = 0  # optional thin-column backstop (0 = off; the backbone covers it)
    # Sync-committee phase (sync_subnets 0 ⇒ off). Node-based membership: sync_size member nodes
    # (a seeded subset of N), each assigned one of sync_subnets subnets round-robin; per slot,
    # sync_target_aggregators members per subnet publish a contribution. See sync-committee-spec.md.
    sync_size: int = 0
    sync_subnets: int = 0
    sync_target_aggregators: int = 16  # TARGET_AGGREGATORS_PER_SYNC_SUBCOMMITTEE; clamped to members
    # Decoupled-consensus phase (decoupled ⇒ on; replaces committees + sync, needs data columns for
    # the AC gate). ac_vote_size validators vote on the availability chain each slot (one global
    # topic); a finality slot spans ac_slots_per_finality_slot AC slots; fs_subnets finality subnets
    # partition the VALIDATOR set (finality_subnet_of, one uniform draw per validator), with a
    # stable node-partition receiver core (finality_subscribers); fs_aggregators validators per
    # subnet per finality slot, sampled from the entire set. See decoupled-consensus-spec.md.
    decoupled: bool = False
    ac_vote_size: int = 0
    ac_slots_per_finality_slot: int = 0
    fs_subnets: int = 0
    fs_aggregators: int = 0
    # ePBS Payload Timeliness Committee (ptc_size 0 ⇒ off). ptc_size validators per slot vote
    # payload timeliness on one global topic (a flat VRF draw like ac_voters, clamped to V).
    ptc_size: int = 0
    # Validator segregation (requires decoupled): k (= ac_slots_per_finality_slot) also becomes
    # the number of round groups — AC slot s is round s % k, only validators with
    # finality_round_of[v] == s % k vote in it, and fc_aggregators are drawn per AC slot instead
    # of per finality slot. See validator-segregation-spec.md.
    validator_segregation: bool = False
    # Validator→node distribution (the Dist seam; see skewed-validators-spec.md). "uniform":
    # validator v → node v % N, no schedule field, V from the v knob — the status quo. "tiered":
    # regular nodes draw count i+1 with regular_weights[i]; supernodes draw a truncated log-normal
    # on [super_min, super_max] with mean super_mean. "explicit": explicit_counts verbatim. Under
    # tiered/explicit, V is EMERGENT (params.v := Σ counts; the v knob is ignored) and validator
    # ids are contiguous by node: node i hosts [Σ counts[:i], Σ counts[:i+1]).
    dist: str = "uniform"
    regular_weights: tuple[float, ...] = (0.65, 0.25, 0.10)  # P(count = index+1)
    super_min: int = 1
    super_max: int = 1000
    super_mean: float = 200.0
    dist_seed: int = 7  # independent of the draw seed
    explicit_counts: tuple[int, ...] | None = None


@dataclass(frozen=True)
class AttesterRef:
    node: int  # publishing node (gossip origin)
    val: int  # global validator index — the stable identity
    subnet: int  # committee → subnet
    position: int  # index within the committee


@dataclass
class SlotPlan:
    slot: int
    committees: list[list[AttesterRef]]  # [committee] → its s_c attesters
    subnet_of: list[int]  # [committee] → subnet id
    aggregators: list[list[int]]  # [committee] → aggregator node ids (subset of its subscribers)
    proposer: int = 0  # node that publishes this slot's block (a supernode; see generate)
    # [sync subnet] → aggregator node ids (subset of its members); None when the sync phase is off.
    sync_aggregators: list[list[int]] | None = None
    # ac_voters = the VRF-selected validators voting on the availability chain this slot — a flat set
    # (no committees/subnets; one global topic). None when the decoupled phase is off.
    ac_voters: list[AttesterRef] | None = None
    # ptc_voters = the VRF-selected Payload Timeliness Committee this slot — a flat set like
    # ac_voters (one global topic). None when PTC is off.
    ptc_voters: list[AttesterRef] | None = None
    # finality_aggregators[i] = the aggregator refs for finality subnet i — fs_aggregators
    # VALIDATORS sampled from the entire set (unrelated to subnet membership or hosting); the
    # host node carries the duty. Base: set only on a finality-boundary slot (slot % k == 0),
    # once per finality slot. Segregated: set on EVERY slot (each AC slot is a round with its
    # own fresh draw). None otherwise.
    finality_aggregators: list[list[AttesterRef]] | None = None


@dataclass
class Assignment:
    params: Params
    subnet_subscribers: list[list[int]]  # [active subnet] → subscribing node ids (stable)
    slots: list[SlotPlan]
    # Data-columns custody (None when the phase is off). column_subscribers[i] = the nodes
    # custodying column i; full_custody = the nodes holding every column (the relay backbone).
    column_subscribers: list[list[int]] | None = None
    full_custody: list[int] | None = None
    # Sync-committee membership (None when off). sync_subscribers[i] = the member nodes on sync
    # subnet i (stable; each member on exactly one subnet) — the subnet's mesh and coverage set.
    sync_subscribers: list[list[int]] | None = None
    # Decoupled-consensus membership (None when off). finality_subscribers[i] = the member nodes on
    # finality subnet i — a partition of ALL N nodes (every node on exactly one), the subnet's
    # stable mesh/receiver core. finality_subnet_of[v] = the subnet validator v votes on (an
    # independent uniform draw per validator — subnets partition the VALIDATOR set, decoupled from
    # where keys live). validators_per_subnet[i] = that draw's per-subnet counts (~V/S ± binomial
    # noise), for the scaled aggregate size and coverage count.
    finality_subscribers: list[list[int]] | None = None
    finality_subnet_of: list[int] | None = None
    validators_per_subnet: list[int] | None = None
    # Validator segregation (None when off). finality_round_of[v] = the round (0..k-1) validator v
    # votes in — an independent uniform draw, stable for the run; its presence in schedule.json IS
    # how Go and the analysis detect the variant. validators_per_round_subnet[r][i] = the
    # (round, subnet) cell counts of the two independent draws (Σ_r per subnet =
    # validators_per_subnet[i], ΣΣ = V), for the cell-scaled aggregate size and coverage counts.
    finality_round_of: list[int] | None = None
    validators_per_round_subnet: list[list[int]] | None = None
    # validator_counts[node] = hosted-validator count (the Dist seam; None under uniform — a
    # uniform schedule.json is byte-identical to before). Ids are contiguous by node.
    validator_counts: list[int] | None = None

    def to_dict(self) -> dict:
        d = {
            "params": {
                "n": self.params.n,
                "v": self.params.v,
                "c": self.params.c,
                "sc": self.params.sc,
                "subnet_count": self.params.subnet_count,
                "subnets_per_node": self.params.subnets_per_node,
                "subscribe_floor": self.params.subscribe_floor,
                "target_aggregators": self.params.target_aggregators,
                "seed": self.params.seed,
                "num_slots": self.params.num_slots,
            },
            "subnet_subscribers": self.subnet_subscribers,
            "slots": [_slot_dict(s) for s in self.slots],
        }
        # Column custody keys appear only when the phase is on (back-compat: a non-column
        # schedule.json is byte-identical to before).
        if self.params.num_columns > 0:
            d["num_columns"] = self.params.num_columns
            d["column_subscribers"] = self.column_subscribers
            d["full_custody"] = self.full_custody
        # Sync membership appears only when the phase is on (the per-slot sync_aggregators ride
        # each slot dict via _slot_dict).
        if self.params.sync_subnets > 0:
            d["sync_subscribers"] = self.sync_subscribers
        # Decoupled-consensus membership + knobs appear only when the phase is on (the per-slot
        # ac_voters / finality_aggregators ride each slot dict via _slot_dict).
        if self.params.decoupled:
            d["params"]["ac_vote_size"] = self.params.ac_vote_size
            d["params"]["ac_slots_per_finality_slot"] = self.params.ac_slots_per_finality_slot
            d["params"]["fs_subnets"] = self.params.fs_subnets
            d["params"]["fs_aggregators"] = self.params.fs_aggregators
            d["finality_subscribers"] = self.finality_subscribers
            d["finality_subnet_of"] = self.finality_subnet_of
            d["validators_per_subnet"] = self.validators_per_subnet
            # Segregation keys appear only under the variant (back-compat: a non-segregated
            # decoupled schedule.json is byte-identical to before).
            if self.params.validator_segregation:
                d["finality_round_of"] = self.finality_round_of
                d["validators_per_round_subnet"] = self.validators_per_round_subnet
        # The PTC knob appears only when PTC is on (the per-slot ptc_voters ride each slot
        # dict via _slot_dict; back-compat: a non-PTC schedule.json is byte-identical).
        if self.params.ptc_size > 0:
            d["params"]["ptc_size"] = self.params.ptc_size
        # The Dist seam: present only under tiered/explicit (back-compat: a uniform
        # schedule.json is unchanged; absent ⇒ consumers fall back to v % N).
        if self.validator_counts is not None:
            d["validator_counts"] = self.validator_counts
        return d


def _ref_dict(r: AttesterRef) -> dict:
    return {"node": r.node, "val": r.val, "subnet": r.subnet, "position": r.position}


def _slot_dict(s: SlotPlan) -> dict:
    d = {
        "slot": s.slot,
        "committees": [[_ref_dict(r) for r in com] for com in s.committees],
        "subnet_of": s.subnet_of,
        "aggregators": s.aggregators,
        "proposer": s.proposer,
    }
    if s.sync_aggregators is not None:  # only when the sync phase is on
        d["sync_aggregators"] = s.sync_aggregators
    if s.ac_voters is not None:  # only when the decoupled phase is on
        d["ac_voters"] = [_ref_dict(r) for r in s.ac_voters]
    if s.ptc_voters is not None:  # only when PTC is on
        d["ptc_voters"] = [_ref_dict(r) for r in s.ptc_voters]
    if s.finality_aggregators is not None:  # boundary slots (base) / every slot (segregated)
        d["finality_aggregators"] = [
            [_ref_dict(r) for r in aggs] for aggs in s.finality_aggregators
        ]
    return d


def _rng(seed: int, *stream: int) -> random.Random:
    """A deterministic RNG keyed by integer scalars only — no str/tuple seeds, whose
    hashes are salted per-process (PYTHONHASHSEED) and would break reproducibility."""
    mixed = seed
    for s in stream:
        mixed = mixed * 1_000_003 + s
    return random.Random(mixed)


def params_from_v(v: int, n: int) -> Params:
    """Optional convenience: fill C, s_c from the mainnet formula
    ``C = min(64, V/4096)``, ``s_c = (V/32)/C`` for a realistic once-per-epoch point."""
    c = max(1, min(64, v // 4096))
    sc = (v // 32) // c
    return Params(n=n, v=v, c=c, sc=sc)


def generate(p: Params, supers: list[int] | None = None) -> Assignment:
    """Build the assignment. ``supers`` is the supernode id set (from
    topology.supernode_ids): block proposers are drawn from it round-robin over its sorted
    ids. Empty/None ⇒ cyclic over all nodes (``slot % n``), preserving the pre-supernode
    behavior for runs without supernodes.

    With ``num_columns > 0`` the data-columns phase is added: a full-custody backbone (a
    subset of ``supers``) plus per-node custody, and proposers are drawn from the full-custody
    set instead (a proposer originates all columns, so it must hold them all).

    With ``decoupled`` the attestation committees + sync are replaced by the availability +
    finality chains: per-slot ac_voters, a per-validator finality-subnet draw (finality_subnet_of)
    over fs_subnets subnets with a stable node-partition receiver core (finality_subscribers), and
    per-finality-slot fc_aggregators sampled from the whole validator set. Data columns are
    required (they gate the AC vote).

    With ``dist`` tiered/explicit, per-node validator counts are drawn FIRST and V is replaced
    by their sum (emergent V) before any V-dependent validation or draw."""
    counts: list[int] | None = None
    if p.dist != "uniform":
        counts = _validator_counts(p, supers)
        p = replace(p, v=sum(counts))
    node_of = _node_of(p, counts)
    if not p.decoupled:
        if p.c * p.sc > p.v:
            raise ValueError(f"C*s_c ({p.c * p.sc}) > V ({p.v}): too many committee positions")
        if p.c > p.subnet_count:
            raise ValueError(f"C ({p.c}) > subnet_count ({p.subnet_count}): no committee→subnet map")
    subscribers = [] if p.decoupled else _subnet_subscribers(p)

    column_subscribers: list[list[int]] | None = None
    full_custody: list[int] | None = None
    proposer_pool = sorted(supers) if supers else list(range(p.n))
    if p.num_columns > 0:
        if not supers:
            raise ValueError("data columns need supernodes (super_node_fraction > 0) for the backbone")
        full_custody = _full_custody(p, sorted(supers))
        column_subscribers = _column_subscribers(p, full_custody)
        proposer_pool = full_custody  # proposers originate all columns ⇒ must be full-custody

    sync_subscribers: list[list[int]] | None = None
    if p.sync_subnets > 0 and not p.decoupled:
        if p.sync_size > p.n:
            raise ValueError(f"sync_size ({p.sync_size}) > N ({p.n})")
        if p.sync_subnets > p.sync_size:
            raise ValueError(f"sync_subnets ({p.sync_subnets}) > sync_size ({p.sync_size})")
        sync_subscribers = _sync_subscribers(p)

    finality_subscribers: list[list[int]] | None = None
    finality_subnet_of: list[int] | None = None
    validators_per_subnet: list[int] | None = None
    finality_round_of: list[int] | None = None
    validators_per_round_subnet: list[list[int]] | None = None
    if p.validator_segregation and not p.decoupled:
        raise ValueError("validator_segregation requires decoupled")
    if p.decoupled:
        if p.num_columns <= 0:
            raise ValueError("decoupled consensus needs data columns (num_columns > 0) — they gate the AC vote")
        if p.ac_vote_size > p.v:
            raise ValueError(f"ac_vote_size ({p.ac_vote_size}) > V ({p.v})")
        if p.fs_subnets > p.n:
            raise ValueError(f"fs_subnets ({p.fs_subnets}) > N ({p.n})")
        finality_subscribers = _finality_subscribers(p)
        finality_subnet_of = _finality_subnet_of(p)  # after the Dist draw, so V is known
        validators_per_subnet = [0] * p.fs_subnets
        for s in finality_subnet_of:
            validators_per_subnet[s] += 1
        if p.validator_segregation:
            if p.ac_slots_per_finality_slot < 1:
                raise ValueError("validator_segregation needs ac_slots_per_finality_slot >= 1")
            finality_round_of = _finality_round_of(p)
            # The two draws are independent: cell (r, i) = |{v: round r, subnet i}|.
            validators_per_round_subnet = [
                [0] * p.fs_subnets for _ in range(p.ac_slots_per_finality_slot)
            ]
            for r, s in zip(finality_round_of, finality_subnet_of):
                validators_per_round_subnet[r][s] += 1

    slots = [
        _slot_plan(p, slot, subscribers, proposer_pool[slot % len(proposer_pool)],
                   node_of, sync_subscribers)
        for slot in range(p.num_slots)
    ]
    return Assignment(
        params=p, subnet_subscribers=subscribers, slots=slots,
        column_subscribers=column_subscribers, full_custody=full_custody,
        sync_subscribers=sync_subscribers, finality_subscribers=finality_subscribers,
        finality_subnet_of=finality_subnet_of, validators_per_subnet=validators_per_subnet,
        finality_round_of=finality_round_of,
        validators_per_round_subnet=validators_per_round_subnet,
        validator_counts=counts,
    )


_SUPER_SIGMA = 1.0  # log-normal shape for the supernode tier; μ is solved for the mean


def _validator_counts(p: Params, supers: list[int] | None) -> list[int]:
    """Per-node hosted-validator counts (the Dist seam). tiered: regular nodes draw count i+1
    with probability regular_weights[i]; supernodes draw a log-normal rejection-sampled into
    [super_min, super_max], with μ solved so the truncated mean is super_mean. explicit:
    explicit_counts verbatim. Seeded by dist_seed, independent of the draw seed."""
    if p.dist == "explicit":
        if p.explicit_counts is None or len(p.explicit_counts) != p.n:
            raise ValueError("explicit dist needs explicit_counts of length N")
        return list(p.explicit_counts)
    if p.dist != "tiered":
        raise ValueError(f"unknown dist {p.dist!r}")
    if not supers:
        raise ValueError("tiered dist needs supernodes (super_node_fraction > 0)")
    if not p.super_min <= p.super_mean <= p.super_max:
        raise ValueError(
            f"super_mean ({p.super_mean}) must lie in [super_min, super_max] "
            f"[{p.super_min}, {p.super_max}]"
        )
    rng = _rng(p.dist_seed, 11)
    mu = _lognorm_mu(p.super_min, p.super_max, p.super_mean, _SUPER_SIGMA)
    sup = set(supers)
    regular_counts = range(1, len(p.regular_weights) + 1)
    counts: list[int] = []
    for node in range(p.n):
        if node in sup:
            while True:  # rejection-sample into range; μ is calibrated for this truncation
                x = rng.lognormvariate(mu, _SUPER_SIGMA)
                if p.super_min <= x <= p.super_max:
                    counts.append(round(x))
                    break
        else:
            counts.append(rng.choices(regular_counts, weights=p.regular_weights)[0])
    return counts


def _lognorm_mu(lo: float, hi: float, mean: float, sigma: float) -> float:
    """μ such that log-normal(μ, σ) truncated to [lo, hi] has the given mean. The truncated
    mean is monotone increasing in μ, so bisection converges; 100 halvings is exact to well
    under one validator."""
    def phi(z: float) -> float:
        return 0.5 * (1 + math.erf(z / math.sqrt(2)))

    def trunc_mean(mu: float) -> float:
        a, b = (math.log(lo) - mu) / sigma, (math.log(hi) - mu) / sigma
        denom = phi(b) - phi(a)
        if denom <= 0:  # numerically degenerate: the window is in a far tail
            return lo if a > 0 else hi
        return math.exp(mu + sigma * sigma / 2) * (phi(b - sigma) - phi(a - sigma)) / denom

    lo_mu, hi_mu = math.log(lo) - 4 * sigma, math.log(hi) + 4 * sigma
    for _ in range(100):
        mid = (lo_mu + hi_mu) / 2
        if trunc_mean(mid) < mean:
            lo_mu = mid
        else:
            hi_mu = mid
    return (lo_mu + hi_mu) / 2


def _node_of(p: Params, counts: list[int] | None) -> Callable[[int], int]:
    """The validator→node map: uniform v % N, or (with counts) the contiguous-range lookup —
    node i hosts ids [Σ counts[:i], Σ counts[:i+1])."""
    if counts is None:
        return lambda val: val % p.n
    bounds = list(accumulate(counts))
    return lambda val: bisect_right(bounds, val)


def _full_custody(p: Params, supers: list[int]) -> list[int]:
    """The full-custody backbone: a seeded subset of the supernodes that hold every column
    (they have the 1 Gbit pipe). F = round(full_custody_fraction · |supers|), validated
    ≥ column_backbone_floor so every column subnet has a relay core."""
    f = round(p.full_custody_fraction * len(supers))
    if f < p.column_backbone_floor:
        raise ValueError(
            f"full-custody F={f} < column_backbone_floor ({p.column_backbone_floor}): "
            "raise full_custody_fraction or super_node_fraction"
        )
    return sorted(_rng(p.seed, 4).sample(supers, f))


def _column_subscribers(p: Params, full_custody: list[int]) -> list[list[int]]:
    """column_subscribers[i] = the nodes custodying column i = every full-custody node (the
    relay backbone) ∪ the ordinary nodes that drew i. Each ordinary node draws custody_floor
    random columns (seeded), mirroring _subnet_subscribers. per_subnet_floor optionally tops
    up any thin column (off by default — the backbone already covers every column)."""
    rng = _rng(p.seed, 5)
    full = set(full_custody)
    cols: list[set[int]] = [set(full_custody) for _ in range(p.num_columns)]
    custody = min(p.custody_floor, p.num_columns)
    for node in range(p.n):
        if node in full:
            continue  # full-custody nodes already hold every column
        for c in rng.sample(range(p.num_columns), custody):
            cols[c].add(node)
    floor = min(p.per_subnet_floor, p.n)
    if floor > 0:
        for c in range(p.num_columns):
            if len(cols[c]) < floor:
                extra = [node for node in range(p.n) if node not in cols[c]]
                rng.shuffle(extra)
                cols[c].update(extra[: floor - len(cols[c])])
    return [sorted(s) for s in cols]


def _subnet_subscribers(p: Params) -> list[list[int]]:
    """Stable subscribe set: every node subscribes ``min(subnets_per_node, C)`` random
    active subnets, then any active subnet below the floor is topped up — so each active
    subnet has ≥ ``min(subscribe_floor, N)`` subscribers (its mesh core)."""
    rng = _rng(p.seed, 1)
    spn = min(p.subnets_per_node, p.c)
    subs: list[set[int]] = [set() for _ in range(p.c)]
    for node in range(p.n):
        for s in rng.sample(range(p.c), spn):
            subs[s].add(node)
    floor = min(p.subscribe_floor, p.n)
    for s in range(p.c):
        if len(subs[s]) < floor:
            extra = [node for node in range(p.n) if node not in subs[s]]
            rng.shuffle(extra)
            subs[s].update(extra[: floor - len(subs[s])])
    return [sorted(s) for s in subs]


def _sync_subscribers(p: Params) -> list[list[int]]:
    """sync_subscribers[i] = the member nodes on sync subnet i. Draw sync_size member nodes from
    0..N-1 (seeded), assign each a subnet round-robin (member j → j % sync_subnets) so subnets are
    even; a member is on exactly one subnet. Stable for the run — both the subnet's mesh and its
    coverage set. Node-based (like column custody), not the per-node subnet subscribe."""
    members = _rng(p.seed, 6).sample(range(p.n), p.sync_size)
    subs: list[list[int]] = [[] for _ in range(p.sync_subnets)]
    for j, node in enumerate(members):
        subs[j % p.sync_subnets].append(node)
    return [sorted(s) for s in subs]


def _finality_subscribers(p: Params) -> list[list[int]]:
    """finality_subscribers[i] = the member nodes on finality subnet i — the subnet's STABLE
    mesh/receiver core, not who votes there (that's _finality_subnet_of). Assign every node to one
    subnet by a stable random draw (seeded) — a partition of ALL N nodes (each on exactly one), not
    a sample. Sizes need not be exactly even; coverage stays exact (it reads these sets)."""
    rng = _rng(p.seed, 9)
    subs: list[list[int]] = [[] for _ in range(p.fs_subnets)]
    for node in range(p.n):
        subs[rng.randrange(p.fs_subnets)].append(node)
    return [sorted(s) for s in subs]


def _finality_subnet_of(p: Params) -> list[int]:
    """finality_subnet_of[v] = the finality subnet validator v votes on — an independent seeded
    uniform draw per validator, so subnets partition the VALIDATOR set (~V/S ± binomial noise)
    regardless of where keys live. Drawn once here (the one-plan principle: Go never re-derives
    it). Stream 12: 11 is taken by the dist_seed-keyed count draw, and seed == dist_seed must
    not alias the two."""
    rng = _rng(p.seed, 12)
    return [rng.randrange(p.fs_subnets) for _ in range(p.v)]


def _finality_round_of(p: Params) -> list[int]:
    """finality_round_of[v] = the round (0..k-1) validator v votes in under segregation — an
    independent uniform draw per validator, exactly parallel to _finality_subnet_of and stable
    for the run (a validator votes in the same round of every finality slot). Stream 13: the
    next free after 8/9/10/12 (11 is the dist_seed-keyed count draw)."""
    rng = _rng(p.seed, 13)
    return [rng.randrange(p.ac_slots_per_finality_slot) for _ in range(p.v)]


def _slot_plan(
    p: Params,
    slot: int,
    subscribers: list[list[int]],
    proposer: int,
    node_of: Callable[[int], int],
    sync_subscribers: list[list[int]] | None = None,
) -> SlotPlan:
    committees: list[list[AttesterRef]] = []
    subnet_of: list[int] = []
    aggregators: list[list[int]] = []
    if not p.decoupled:
        # Independent per-slot draw: s_c·C distinct validators, chunked into C committees.
        vals = _rng(p.seed, 2, slot).sample(range(p.v), p.c * p.sc)
        agg_rng = _rng(p.seed, 3, slot)  # aggregator draw, independent of the committee draw
        for ci in range(p.c):
            subnet = ci  # identity: committee ci → subnet ci (C ≤ subnet_count)
            members = [
                AttesterRef(node=node_of(v), val=v, subnet=subnet, position=pos)
                for pos, v in enumerate(vals[ci * p.sc : (ci + 1) * p.sc])
            ]
            committees.append(members)
            subnet_of.append(subnet)
            # Aggregators are drawn from the subnet's stable subscribers (they already receive
            # the attestations); ~target_aggregators of them, clamped to the subscriber count.
            subs = subscribers[subnet]
            k = min(p.target_aggregators, len(subs))
            aggregators.append(sorted(agg_rng.sample(subs, k)))
    # Sync aggregators: per subnet, sync_target_aggregators members (clamped), drawn from that
    # subnet's stable membership — exactly as attestation aggregators are drawn from subscribers.
    sync_aggregators: list[list[int]] | None = None
    if sync_subscribers is not None:
        sync_rng = _rng(p.seed, 7, slot)
        sync_aggregators = [
            sorted(sync_rng.sample(subs, min(p.sync_target_aggregators, len(subs))))
            for subs in sync_subscribers
        ]
    # Decoupled consensus: a flat per-slot AC-voter draw (ac_vote_size validators, one global topic),
    # plus per-finality-slot aggregators on the finality-boundary slot (slot % k == 0).
    ac_voters: list[AttesterRef] | None = None
    finality_aggregators: list[list[AttesterRef]] | None = None
    if p.decoupled:
        voters = _rng(p.seed, 8, slot).sample(range(p.v), p.ac_vote_size)
        ac_voters = [
            AttesterRef(node=node_of(v), val=v, subnet=0, position=pos)  # subnet unused (global)
            for pos, v in enumerate(voters)
        ]
        # Per subnet, fs_aggregators VALIDATORS from the ENTIRE set — unrelated to subnet
        # membership or hosting. The host node carries the duty (it pre-joins the subnet's mesh
        # one AC slot ahead, generally as a non-member). Base: drawn once per finality slot,
        # keyed by fslot, on the boundary slot. Segregated: a fresh draw EVERY slot (each AC
        # slot is a round) — same stream (10); the modes are mutually exclusive, so the two
        # keyings can't collide within a run.
        agg_rng = None
        if p.validator_segregation:
            agg_rng = _rng(p.seed, 10, slot)
        elif slot % p.ac_slots_per_finality_slot == 0:  # a finality-slot boundary
            agg_rng = _rng(p.seed, 10, slot // p.ac_slots_per_finality_slot)
        if agg_rng is not None:
            finality_aggregators = [
                [
                    AttesterRef(node=node_of(v), val=v, subnet=subnet, position=pos)
                    for pos, v in enumerate(agg_rng.sample(range(p.v), min(p.fs_aggregators, p.v)))
                ]
                for subnet in range(p.fs_subnets)
            ]
    # ePBS PTC: a flat per-slot draw like ac_voters, clamped to V. Stream 14: the next free
    # after 13 (see _finality_round_of).
    ptc_voters: list[AttesterRef] | None = None
    if p.ptc_size > 0:
        ptc = _rng(p.seed, 14, slot).sample(range(p.v), min(p.ptc_size, p.v))
        ptc_voters = [
            AttesterRef(node=node_of(v), val=v, subnet=0, position=pos)  # subnet unused (global)
            for pos, v in enumerate(ptc)
        ]
    return SlotPlan(
        slot=slot, committees=committees, subnet_of=subnet_of, aggregators=aggregators,
        proposer=proposer, sync_aggregators=sync_aggregators, ac_voters=ac_voters,
        ptc_voters=ptc_voters, finality_aggregators=finality_aggregators,
    )
