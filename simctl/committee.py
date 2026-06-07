"""Committee assignment — the topology seam (validators→nodes→committees→subnets).

Pure, seeded source of truth for the attestation phase. Two parts:

- the per-slot **committee draw**: each slot, C committees of s_c validators (who attest,
  on which subnet); and
- the stable **subnet subscribe set**: for each active subnet, ≥``subscribe_floor`` nodes
  (the receivers/relayers), so every active subnet has a real mesh core. Publishers dial
  into this set per slot; see att-subnet.md.

`V`, `C`, `s_c` are independent knobs (only `C·s_c ≤ V`). The result serializes to
``committee.json`` (consumed by both backends + the analysis), so the assignment is
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


@dataclass
class Assignment:
    params: Params
    subnet_subscribers: list[list[int]]  # [active subnet] → subscribing node ids (stable)
    slots: list[SlotPlan]

    def to_dict(self) -> dict:
        return {
            "params": {
                "n": self.params.n,
                "v": self.params.v,
                "c": self.params.c,
                "sc": self.params.sc,
                "subnet_count": self.params.subnet_count,
                "subnets_per_node": self.params.subnets_per_node,
                "subscribe_floor": self.params.subscribe_floor,
                "seed": self.params.seed,
                "num_slots": self.params.num_slots,
            },
            "subnet_subscribers": self.subnet_subscribers,
            "slots": [
                {
                    "slot": s.slot,
                    "committees": [[_ref_dict(r) for r in com] for com in s.committees],
                    "subnet_of": s.subnet_of,
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
    ``C = min(64, V/4096)``, ``s_c = (V/32)/C`` for a realistic once-per-epoch point."""
    c = max(1, min(64, v // 4096))
    sc = (v // 32) // c
    return Params(n=n, v=v, c=c, sc=sc)


def generate(p: Params) -> Assignment:
    if p.c * p.sc > p.v:
        raise ValueError(f"C*s_c ({p.c * p.sc}) > V ({p.v}): too many committee positions")
    if p.c > p.subnet_count:
        raise ValueError(f"C ({p.c}) > subnet_count ({p.subnet_count}): no committee→subnet map")
    subscribers = _subnet_subscribers(p)
    slots = [_slot_plan(p, slot) for slot in range(p.num_slots)]
    return Assignment(params=p, subnet_subscribers=subscribers, slots=slots)


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


def _slot_plan(p: Params, slot: int) -> SlotPlan:
    # Independent per-slot draw: s_c·C distinct validators, chunked into C committees.
    vals = _rng(p.seed, 2, slot).sample(range(p.v), p.c * p.sc)
    committees: list[list[AttesterRef]] = []
    subnet_of: list[int] = []
    for ci in range(p.c):
        subnet = ci  # identity: committee ci → subnet ci (C ≤ subnet_count)
        members = [
            AttesterRef(node=v % p.n, val=v, subnet=subnet, position=pos)
            for pos, v in enumerate(vals[ci * p.sc : (ci + 1) * p.sc])
        ]
        committees.append(members)
        subnet_of.append(subnet)
    return SlotPlan(slot=slot, committees=committees, subnet_of=subnet_of)
