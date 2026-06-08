"""Verify message receipt and print arrival-delay CDFs for a Shadow run.

Each node logs JSON lines on stdout (slog): a `publish` per message it originates and
an `arrival` per message it receives, with absolute nanosecond timestamps (comparable
across hosts — Shadow shares one clock) and the identity fields
(kind, slot, subnet, attester, origin). Blocks are kind=1 (subnet/attester = -1);
attestations are kind=2; aggregates are kind=3 (attester carries agg_idx, origin = -1).

Blocks must reach every other node. Attestations must reach exactly their subnet's
subscribers (from committee.json) and nobody else. Aggregates (global topic, multi-source)
must reach every node except their committee's aggregators, exactly once — missing/leaked/
duplicate are all failures. The headline attestation metric is the fraction that voted for
the block. Stdlib only.

Usage: python analysis/check_arrivals.py <run-dir>
"""

import csv
import json
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

BLOCK_KIND = 1
ATTEST_KIND = 2
AGGREGATE_KIND = 3

# pub key: (kind, slot, subnet, attester, origin) -> (t_ns, voted_block)
PubKey = tuple[int, int, int, int, int]
# arrival: (node, kind, slot, subnet, attester, origin, t_ns)
Arrival = tuple[int, int, int, int, int, int, int]


def parse_events(lines: Iterable[str]) -> tuple[dict[PubKey, tuple[int, bool]], list[Arrival]]:
    """Parse slog JSON lines into publishes and arrivals; ignore other lines."""
    pubs: dict[PubKey, tuple[int, bool]] = {}
    arrs: list[Arrival] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        match ev.get("msg"):
            case "publish":
                key = (ev["kind"], ev["slot"], ev["subnet"], ev["attester"], ev["origin"])
                pubs[key] = (ev["t_ns"], ev.get("voted_block", False))
            case "arrival":
                arrs.append(
                    (ev["node"], ev["kind"], ev["slot"], ev["subnet"], ev["attester"], ev["origin"], ev["t_ns"])
                )
    return pubs, arrs


@dataclass
class Result:
    """Block-receipt check: every node != origin receives each block once."""

    arrivals: int
    expected: int
    missing: list[tuple[int, int, int]] = field(default_factory=list)
    duplicates: list[tuple[int, int, int]] = field(default_factory=list)
    delays_ms: list[float] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing and not self.duplicates and self.arrivals == self.expected


def analyze(pubs: dict[PubKey, tuple[int, bool]], arrs: list[Arrival], node_nums: set[int]) -> Result:
    """Cross-check block arrivals against publishes: every node != origin should
    receive each published block exactly once."""
    block_pubs = {(s, o): t for (k, s, _sub, _a, o), (t, _v) in pubs.items() if k == BLOCK_KIND}
    block_arrs = [(n, s, o, t) for (n, k, s, _sub, _a, o, t) in arrs if k == BLOCK_KIND]

    counts = Counter((n, s, o) for n, s, o, _ in block_arrs)
    received = set(counts)
    duplicates = sorted(k for k, c in counts.items() if c > 1)

    missing: list[tuple[int, int, int]] = []
    for slot, origin in block_pubs:
        for node in node_nums:
            if node != origin and (node, slot, origin) not in received:
                missing.append((node, slot, origin))
    missing.sort()

    delays_ms = sorted(
        (t - block_pubs[(s, o)]) / 1e6 for n, s, o, t in block_arrs if (s, o) in block_pubs
    )
    expected = len(block_pubs) * (len(node_nums) - 1)
    return Result(len(block_arrs), expected, missing, duplicates, delays_ms)


@dataclass
class AttestResult:
    """Attestation check: each attestation reaches exactly subscribers(subnet) \\ {origin}."""

    arrivals: int
    expected: int
    missing: list[tuple[int, int, int, int, int]] = field(default_factory=list)
    leaked: list[tuple[int, int, int, int, int]] = field(default_factory=list)
    duplicates: list[tuple[int, int, int, int, int]] = field(default_factory=list)
    delays_ms: list[float] = field(default_factory=list)
    fraction_voted_block: float = 0.0
    published: int = 0

    @property
    def ok(self) -> bool:
        return (
            not self.missing
            and not self.leaked
            and not self.duplicates
            and self.arrivals == self.expected
        )


def analyze_attestations(
    pubs: dict[PubKey, tuple[int, bool]],
    arrs: list[Arrival],
    subscribers: dict[int, set[int]],
) -> AttestResult:
    """Cross-check attestation arrivals against the subscriber sets: each attestation
    reaches exactly subscribers(slot, subnet) \\ {origin} — no missing, leaked, or dup."""
    att_pubs = {(s, sub, a, o): (t, v) for (k, s, sub, a, o), (t, v) in pubs.items() if k == ATTEST_KIND}
    att_arrs = [(n, s, sub, a, o, t) for (n, k, s, sub, a, o, t) in arrs if k == ATTEST_KIND]

    counts = Counter((n, s, sub, a, o) for n, s, sub, a, o, _ in att_arrs)
    received = set(counts)
    duplicates = sorted(k for k, c in counts.items() if c > 1)

    missing: list[tuple[int, int, int, int, int]] = []
    expected = 0
    for slot, subnet, attester, origin in att_pubs:
        for node in subscribers.get(subnet, set()):
            if node == origin:
                continue
            expected += 1
            if (node, slot, subnet, attester, origin) not in received:
                missing.append((node, slot, subnet, attester, origin))
    missing.sort()

    leaked = sorted(
        (n, s, sub, a, o)
        for n, s, sub, a, o, _ in att_arrs
        if n not in subscribers.get(sub, set())
    )
    delays_ms = sorted(
        (t - att_pubs[(s, sub, a, o)][0]) / 1e6
        for n, s, sub, a, o, t in att_arrs
        if (s, sub, a, o) in att_pubs
    )
    block_votes = sum(1 for _t, voted in att_pubs.values() if voted)
    fraction = block_votes / len(att_pubs) if att_pubs else 0.0
    return AttestResult(
        len(att_arrs), expected, missing, leaked, duplicates, delays_ms, fraction, len(att_pubs)
    )


@dataclass
class AggregateResult:
    """Aggregate check: each (slot, subnet, agg_idx) reaches every node EXCEPT that committee's
    aggregators (which publish it; their loopback is skipped), exactly once. A committee's
    aggregators publish byte-identical copies that gossipsub dedups, so a node receiving it
    twice (duplicate) or an aggregator receiving its own (leaked) is a failure."""

    arrivals: int
    expected: int
    missing: list[tuple[int, int, int, int]] = field(default_factory=list)
    leaked: list[tuple[int, int, int, int]] = field(default_factory=list)
    duplicates: list[tuple[int, int, int, int]] = field(default_factory=list)
    delays_ms: list[float] = field(default_factory=list)
    published: int = 0  # distinct logical aggregates (m·C per slot)

    @property
    def ok(self) -> bool:
        return (
            not self.missing
            and not self.leaked
            and not self.duplicates
            and self.arrivals == self.expected
        )


def _aggregate_logical(committee_data: dict) -> tuple[dict[tuple[int, int, int], set[int]], int]:
    """Map each logical aggregate (slot, subnet, agg_idx) to its committee's aggregator node
    set, with N. m aggregates per committee, both from committee.json."""
    n = committee_data["params"]["n"]
    m = committee_data["params"]["m"]
    logical: dict[tuple[int, int, int], set[int]] = {}
    for sp in committee_data["slots"]:
        for ci, aggs in enumerate(sp["aggregators"]):
            subnet = sp["subnet_of"][ci]
            for agg_idx in range(m):
                logical[(sp["slot"], subnet, agg_idx)] = set(aggs)
    return logical, n


def _aggregate_result(
    logical: dict[tuple[int, int, int], set[int]],
    n: int,
    counts: Counter,
    received: dict[tuple[int, int, int], set[int]],
    delays_ms: list[float],
) -> AggregateResult:
    """Shared core: expected = Σ (N − |aggregators|) over logical aggregates; an arrival at an
    own-aggregator is a leak; a repeat arrival is a duplicate (dedup failed)."""
    duplicates = sorted(k for k, c in counts.items() if c > 1)
    missing: list[tuple[int, int, int, int]] = []
    expected = 0
    for (slot, subnet, agg_idx), aggs in logical.items():
        for node in range(n):
            if node in aggs:
                continue
            expected += 1
            if node not in received.get((slot, subnet, agg_idx), set()):
                missing.append((node, slot, subnet, agg_idx))
    leaked = sorted(
        (node, slot, subnet, agg_idx)
        for (node, slot, subnet, agg_idx) in counts
        if node in logical.get((slot, subnet, agg_idx), set())
    )
    return AggregateResult(
        sum(counts.values()), expected, sorted(missing), leaked, duplicates,
        sorted(delays_ms), len(logical),
    )


def analyze_aggregates(
    pubs: dict[PubKey, tuple[int, bool]], arrs: list[Arrival], committee_data: dict
) -> AggregateResult:
    """Cross-check aggregate arrivals (Shadow slog) against the aggregator sets."""
    logical, n = _aggregate_logical(committee_data)
    agg_pubs = {(s, sub, a): t for (k, s, sub, a, _o), (t, _v) in pubs.items() if k == AGGREGATE_KIND}
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    for node, k, slot, subnet, agg_idx, _o, t in arrs:
        if k != AGGREGATE_KIND:
            continue
        counts[(node, slot, subnet, agg_idx)] += 1
        received.setdefault((slot, subnet, agg_idx), set()).add(node)
        if (slot, subnet, agg_idx) in agg_pubs:
            delays_ms.append((t - agg_pubs[(slot, subnet, agg_idx)]) / 1e6)
    return _aggregate_result(logical, n, counts, received, delays_ms)


def analyze_aggregates_csv(path: Path, committee_data: dict) -> AggregateResult:
    """Aggregate coverage/dedup for the simnet backend. The CSV is keyed by
    (slot, subnet, attester=agg_idx) with no origin column."""
    logical, n = _aggregate_logical(committee_data)
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != AGGREGATE_KIND:
                continue
            slot, subnet, agg_idx = int(r["slot"]), int(r["subnet"]), int(r["attester"])
            node = int(r["node"])
            counts[(node, slot, subnet, agg_idx)] += 1
            received.setdefault((slot, subnet, agg_idx), set()).add(node)
            delays_ms.append(float(r["delay_ms"]))
    return _aggregate_result(logical, n, counts, received, delays_ms)


def load_committee(run_dir: Path) -> dict[int, set[int]] | None:
    """Subscriber sets keyed by subnet (stable for the run) from committee.json, or None
    if absent (a block-only run)."""
    path = run_dir / "committee.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return {subnet: set(members) for subnet, members in enumerate(data["subnet_subscribers"])}


def load_proposers(run_dir: Path) -> list[int] | None:
    """Per-slot block proposer (a supernode) from committee.json, or None (block-only run)."""
    path = run_dir / "committee.json"
    if not path.exists():
        return None
    return [sp["proposer"] for sp in json.loads(path.read_text())["slots"]]


def load_supernodes(run_dir: Path) -> set[int] | None:
    """Node ids with supernode (>=1024 Mbit) upload from topology.json, or None if absent."""
    path = run_dir / "topology.json"
    if not path.exists():
        return None
    nodes = json.loads(path.read_text())["nodes"]
    return {n["num"] for n in nodes if n["upload_bw_mbps"] >= 1024}


def block_origins(pubs: dict[PubKey, tuple[int, bool]]) -> dict[int, int]:
    """slot -> publishing node for each block publish (the Shadow event path)."""
    return {s: o for (k, s, _sub, _a, o) in pubs if k == BLOCK_KIND}


def check_proposers(
    proposers: list[int],
    supernodes: set[int],
    block_origins: dict[int, int] | None = None,
) -> list[str]:
    """Cross-backend proposer guard: every scheduled proposer must be a supernode, and — when
    per-slot block origins are available (the Shadow event path) — each block must be published
    by its slot's scheduled proposer. Returns human-readable violations ([] means OK), so both
    backends fail loudly if they, or committee.json and topology.json, ever disagree."""
    problems = []
    for slot, p in enumerate(proposers):
        if p not in supernodes:
            problems.append(f"slot {slot}: proposer {p} is not a supernode")
    for slot, origin in (block_origins or {}).items():
        if slot < len(proposers) and origin != proposers[slot]:
            problems.append(
                f"slot {slot}: block origin {origin} != scheduled proposer {proposers[slot]}"
            )
    return problems


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile (matches the Go metrics package)."""
    if not values:
        return 0.0
    s = sorted(values)
    rank = max(math.ceil(p / 100 * len(s)), 1)
    return s[rank - 1]


def cdf(delays_ms: list[float]) -> dict[str, float]:
    """Headline arrival CDF (count + p50/p90/p99/p100). One place computes
    percentiles so the Shadow and simnet backends are summarized identically."""
    return {
        "count": len(delays_ms),
        "p50": percentile(delays_ms, 50),
        "p90": percentile(delays_ms, 90),
        "p99": percentile(delays_ms, 99),
        "p100": percentile(delays_ms, 100),
    }


def delays_from_csv(path: Path, kind: int = BLOCK_KIND) -> list[float]:
    """Arrival delays (ms) of the given kind from a slot-sim CSV — the simnet backend's
    output, read in the same units as the Shadow path's delays. Pre-attestation CSVs
    have no `kind` column; those rows are all blocks."""
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    return [float(r["delay_ms"]) for r in rows if int(r.get("kind", BLOCK_KIND)) == kind]


def analyze_attestations_csv(path: Path, committee_data: dict) -> AttestResult:
    """Coverage/no-leak for the simnet backend (the real cross-backend graph). The arrival
    CSV is keyed by (slot, subnet, attester) with no origin column, so each attestation's
    origin comes from committee.json's draw. Each published attestation must reach exactly
    subscribers(subnet) \\ {origin} — missing/leaked/duplicate all fail."""
    subscribers = {subnet: set(m) for subnet, m in enumerate(committee_data["subnet_subscribers"])}
    origin_of: dict[tuple[int, int, int], int] = {}  # (slot,subnet,attester) -> publishing node
    for sp in committee_data["slots"]:
        for com in sp["committees"]:
            for ref in com:
                origin_of[(sp["slot"], ref["subnet"], ref["val"])] = ref["node"]

    received: dict[tuple[int, int, int], set[int]] = {}
    counts: Counter = Counter()
    voted: dict[tuple[int, int, int], bool] = {}
    delays_ms: list[float] = []
    leaked: list[tuple[int, int, int, int, int]] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != ATTEST_KIND:
                continue
            slot, subnet, attester = int(r["slot"]), int(r["subnet"]), int(r["attester"])
            node = int(r["node"])
            key = (slot, subnet, attester)
            origin = origin_of.get(key, -1)
            counts[(node, slot, subnet, attester, origin)] += 1
            received.setdefault(key, set()).add(node)
            voted[key] = r["voted_block"] == "true"
            delays_ms.append(float(r["delay_ms"]))
            if node not in subscribers.get(subnet, set()):
                leaked.append((node, slot, subnet, attester, origin))

    missing: list[tuple[int, int, int, int, int]] = []
    expected = 0
    for (slot, subnet, attester), origin in origin_of.items():
        for node in subscribers.get(subnet, set()):
            if node == origin:
                continue
            expected += 1
            if node not in received.get((slot, subnet, attester), set()):
                missing.append((node, slot, subnet, attester, origin))
    duplicates = sorted(k for k, c in counts.items() if c > 1)
    block_votes = sum(1 for v in voted.values() if v)
    fraction = block_votes / len(voted) if voted else 0.0
    return AttestResult(
        sum(counts.values()), expected, sorted(missing), sorted(leaked), duplicates,
        sorted(delays_ms), fraction, len(origin_of),
    )


def load_run(run_dir: Path) -> tuple[dict[PubKey, tuple[int, bool]], list[Arrival], set[int]]:
    """Read every host's stdout under run_dir/shadow.data/hosts/node*/."""
    hosts_dir = run_dir / "shadow.data" / "hosts"
    node_nums: set[int] = set()
    all_lines: list[str] = []
    for host in sorted(hosts_dir.glob("node*")):
        m = re.fullmatch(r"node(\d+)", host.name)
        if not m:
            continue
        node_nums.add(int(m.group(1)))
        for out in host.glob("slot-sim-node.*.stdout"):
            all_lines.extend(out.read_text().splitlines())
    pubs, arrs = parse_events(all_lines)
    return pubs, arrs, node_nums


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <run-dir>", file=sys.stderr)
        return 2
    run_dir = Path(argv[1])
    pubs, arrs, node_nums = load_run(run_dir)
    res = analyze(pubs, arrs, node_nums)

    print(f"nodes: {len(node_nums)}  blocks published: {sum(1 for k, *_ in pubs if k == BLOCK_KIND)}")
    print(f"block arrivals: {res.arrivals} (expected {res.expected})")
    print(f"  missing: {len(res.missing)}  duplicates: {len(res.duplicates)}")
    if res.delays_ms:
        c = cdf(res.delays_ms)
        print(f"  block CDF (ms): p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}")

    ok = res.ok
    committee_path = run_dir / "committee.json"
    if committee_path.exists():
        committee_data = json.loads(committee_path.read_text())
        subscribers = {sub: set(mem) for sub, mem in enumerate(committee_data["subnet_subscribers"])}
        ares = analyze_attestations(pubs, arrs, subscribers)
        ok = ok and ares.ok
        print(f"attestations published: {ares.published}")
        print(f"attestation arrivals: {ares.arrivals} (expected {ares.expected})")
        print(f"  missing: {len(ares.missing)}  leaked: {len(ares.leaked)}  duplicates: {len(ares.duplicates)}")
        if ares.leaked:
            print("  leaked (node,slot,subnet,attester,origin):", ares.leaked[:10])
        if ares.delays_ms:
            c = cdf(ares.delays_ms)
            print(f"  attestation CDF (ms): p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}")
        print(f"  fraction voted block: {ares.fraction_voted_block:.3f}")

        if committee_data["params"].get("m", 0) > 0 and committee_data["slots"][0].get("aggregators"):
            gres = analyze_aggregates(pubs, arrs, committee_data)
            ok = ok and gres.ok
            print(f"aggregates published (distinct): {gres.published}")
            print(f"aggregate arrivals: {gres.arrivals} (expected {gres.expected})")
            print(f"  missing: {len(gres.missing)}  leaked: {len(gres.leaked)}  duplicates: {len(gres.duplicates)}")
            if gres.delays_ms:
                c = cdf(gres.delays_ms)
                print(f"  aggregate CDF (ms): p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}")

    proposers = load_proposers(run_dir)
    supernodes = load_supernodes(run_dir)
    if proposers is not None and supernodes is not None:
        problems = check_proposers(proposers, supernodes, block_origins(pubs))
        ok = ok and not problems
        print(f"proposer guard: {'OK' if not problems else 'FAIL'} (all proposers are supernodes)")
        for p in problems[:10]:
            print("  ", p)
    print("RESULT:", "OK" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
