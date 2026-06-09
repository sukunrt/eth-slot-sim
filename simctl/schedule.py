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

import random
from dataclasses import dataclass


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


@dataclass
class Assignment:
    params: Params
    subnet_subscribers: list[list[int]]  # [active subnet] → subscribing node ids (stable)
    slots: list[SlotPlan]
    # Data-columns custody (None when the phase is off). column_subscribers[i] = the nodes
    # custodying column i; full_custody = the nodes holding every column (the relay backbone).
    column_subscribers: list[list[int]] | None = None
    full_custody: list[int] | None = None

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
            "slots": [
                {
                    "slot": s.slot,
                    "committees": [[_ref_dict(r) for r in com] for com in s.committees],
                    "subnet_of": s.subnet_of,
                    "aggregators": s.aggregators,
                    "proposer": s.proposer,
                }
                for s in self.slots
            ],
        }
        # Column custody keys appear only when the phase is on (back-compat: a non-column
        # schedule.json is byte-identical to before).
        if self.params.num_columns > 0:
            d["num_columns"] = self.params.num_columns
            d["column_subscribers"] = self.column_subscribers
            d["full_custody"] = self.full_custody
        return d


def _ref_dict(r: AttesterRef) -> dict:
    return {"node": r.node, "val": r.val, "subnet": r.subnet, "position": r.position}


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
    set instead (a proposer originates all columns, so it must hold them all)."""
    if p.c * p.sc > p.v:
        raise ValueError(f"C*s_c ({p.c * p.sc}) > V ({p.v}): too many committee positions")
    if p.c > p.subnet_count:
        raise ValueError(f"C ({p.c}) > subnet_count ({p.subnet_count}): no committee→subnet map")
    subscribers = _subnet_subscribers(p)

    column_subscribers: list[list[int]] | None = None
    full_custody: list[int] | None = None
    proposer_pool = sorted(supers) if supers else list(range(p.n))
    if p.num_columns > 0:
        if not supers:
            raise ValueError("data columns need supernodes (super_node_fraction > 0) for the backbone")
        full_custody = _full_custody(p, sorted(supers))
        column_subscribers = _column_subscribers(p, full_custody)
        proposer_pool = full_custody  # proposers originate all columns ⇒ must be full-custody

    slots = [
        _slot_plan(p, slot, subscribers, proposer_pool[slot % len(proposer_pool)])
        for slot in range(p.num_slots)
    ]
    return Assignment(
        params=p, subnet_subscribers=subscribers, slots=slots,
        column_subscribers=column_subscribers, full_custody=full_custody,
    )


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


def _slot_plan(p: Params, slot: int, subscribers: list[list[int]], proposer: int) -> SlotPlan:
    # Independent per-slot draw: s_c·C distinct validators, chunked into C committees.
    vals = _rng(p.seed, 2, slot).sample(range(p.v), p.c * p.sc)
    agg_rng = _rng(p.seed, 3, slot)  # aggregator draw, independent of the committee draw
    committees: list[list[AttesterRef]] = []
    subnet_of: list[int] = []
    aggregators: list[list[int]] = []
    for ci in range(p.c):
        subnet = ci  # identity: committee ci → subnet ci (C ≤ subnet_count)
        members = [
            AttesterRef(node=v % p.n, val=v, subnet=subnet, position=pos)
            for pos, v in enumerate(vals[ci * p.sc : (ci + 1) * p.sc])
        ]
        committees.append(members)
        subnet_of.append(subnet)
        # Aggregators are drawn from the subnet's stable subscribers (they already receive
        # the attestations); ~target_aggregators of them, clamped to the subscriber count.
        subs = subscribers[subnet]
        k = min(p.target_aggregators, len(subs))
        aggregators.append(sorted(agg_rng.sample(subs, k)))
    return SlotPlan(slot=slot, committees=committees, subnet_of=subnet_of, aggregators=aggregators, proposer=proposer)
