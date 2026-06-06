"""Committee assignment — the topology seam (validators→nodes→committees→subnets).

Pure, seeded source of truth for the attestation phase: from the scalar knobs
``(N, V, C, s_c, seed, …)`` it derives the V→N mapping, each node's stable backbone
subnets, and per slot the committees → subnets → aggregators → subscriber sets.

`V`, `C`, `s_c` are independent knobs (no forced ``V/32 = C·s_c``); the only hard
constraint is ``C·s_c ≤ V``. Validators are a count + pure functions, never V structs,
so memory scales with N. The result serializes to ``committee.json`` and is consumed by
both backends (Go reads its slice) and the analysis — so the assignment is implemented
once, here, not re-derived per language.
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
    backbone_per_node: int = 2
    aggs_per_committee: int = 16
    seed: int = 1
    num_slots: int = 1


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
    aggregators: list[list[AttesterRef]]  # [committee] → its aggregator refs (⊆ committee)
    subscribers: list[list[int]]  # [committee] → node ids subscribing its subnet this slot


@dataclass
class Assignment:
    params: Params
    backbone: list[list[int]]  # [node] → its backbone subnets (stable for the run)
    slots: list[SlotPlan]

    def to_dict(self) -> dict:
        return {
            "params": {
                "n": self.params.n,
                "v": self.params.v,
                "c": self.params.c,
                "sc": self.params.sc,
                "subnet_count": self.params.subnet_count,
                "backbone_per_node": self.params.backbone_per_node,
                "aggs_per_committee": self.params.aggs_per_committee,
                "seed": self.params.seed,
                "num_slots": self.params.num_slots,
            },
            "backbone": self.backbone,
            "slots": [
                {
                    "slot": s.slot,
                    "committees": [[_ref_dict(r) for r in com] for com in s.committees],
                    "subnet_of": s.subnet_of,
                    "aggregators": [[_ref_dict(r) for r in agg] for agg in s.aggregators],
                    "subscribers": s.subscribers,
                }
                for s in self.slots
            ],
        }


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
    ``C = min(64, V/4096)``, ``s_c = (V/32)/C`` for a realistic once-per-epoch point.
    A helper, not the authoritative path — direct (V, C, s_c) is."""
    c = max(1, min(64, v // 4096))
    sc = (v // 32) // c
    return Params(n=n, v=v, c=c, sc=sc)


def generate(p: Params) -> Assignment:
    if p.c * p.sc > p.v:
        raise ValueError(f"C*s_c ({p.c * p.sc}) > V ({p.v}): too many committee positions")
    if p.c > p.subnet_count:
        raise ValueError(f"C ({p.c}) > subnet_count ({p.subnet_count}): no committee→subnet map")
    if p.backbone_per_node > p.subnet_count:
        raise ValueError(f"backbone_per_node ({p.backbone_per_node}) > subnet_count ({p.subnet_count})")

    backbone = _backbone(p)
    backbone_subs: dict[int, set[int]] = {}
    for node, subnets in enumerate(backbone):
        for s in subnets:
            backbone_subs.setdefault(s, set()).add(node)

    slots = [_slot_plan(p, slot, backbone_subs) for slot in range(p.num_slots)]
    return Assignment(params=p, backbone=backbone, slots=slots)


def _backbone(p: Params) -> list[list[int]]:
    """Each node's backbone_per_node long-lived subnets — a stable function of node-id
    (mainnet: from node-id, 256 epochs), independent of validator count and slot."""
    subnets = range(p.subnet_count)
    return [sorted(_rng(p.seed, 1, node).sample(subnets, p.backbone_per_node)) for node in range(p.n)]


def _slot_plan(p: Params, slot: int, backbone_subs: dict[int, set[int]]) -> SlotPlan:
    # Independent per-slot draw: s_c·C distinct validators, chunked into C committees.
    vals = _rng(p.seed, 2, slot).sample(range(p.v), p.c * p.sc)
    committees: list[list[AttesterRef]] = []
    subnet_of: list[int] = []
    aggregators: list[list[AttesterRef]] = []
    subscribers: list[list[int]] = []
    for ci in range(p.c):
        subnet = ci  # identity: committee ci → subnet ci (C ≤ subnet_count)
        members = [
            AttesterRef(node=v % p.n, val=v, subnet=subnet, position=pos)
            for pos, v in enumerate(vals[ci * p.sc : (ci + 1) * p.sc])
        ]
        n_agg = min(p.aggs_per_committee, p.sc)
        agg_positions = sorted(_rng(p.seed, 3, slot, ci).sample(range(p.sc), n_agg))
        aggs = [members[pos] for pos in agg_positions]
        subs = backbone_subs.get(subnet, set()) | {r.node for r in aggs}

        committees.append(members)
        subnet_of.append(subnet)
        aggregators.append(aggs)
        subscribers.append(sorted(subs))
    return SlotPlan(
        slot=slot,
        committees=committees,
        subnet_of=subnet_of,
        aggregators=aggregators,
        subscribers=subscribers,
    )
