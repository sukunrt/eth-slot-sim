"""Verify message receipt and print arrival-delay CDFs for a Shadow run.

Each node logs JSON lines on stdout (slog): a `publish` per message it originates and
an `arrival` per message it receives, with absolute nanosecond timestamps (comparable
across hosts — Shadow shares one clock) and the identity fields
(kind, slot, subnet, attester, origin). Blocks are kind=1 (subnet/attester = -1);
attestations are kind=2; aggregates are kind=3 (attester carries the aggregator, origin = -1).

Blocks must reach every other node. Attestations must reach exactly their subnet's
subscribers (from schedule.json) and nobody else. Aggregates (global topic): each
aggregator publishes one distinct aggregate that must reach every node except that
aggregator, exactly once — missing/leaked/duplicate are all failures. The headline
attestation metric is the fraction that voted for the block. Stdlib only.

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
COLUMN_KIND = 4
SYNC_MESSAGE_KIND = 5
SYNC_CONTRIBUTION_KIND = 6

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
class ColumnResult:
    """Column check: each column reaches exactly its custodiers \\ {proposer}, once."""

    arrivals: int
    expected: int
    missing: list[tuple[int, int, int, int]] = field(default_factory=list)
    leaked: list[tuple[int, int, int, int]] = field(default_factory=list)
    duplicates: list[tuple[int, int, int, int]] = field(default_factory=list)
    delays_ms: list[float] = field(default_factory=list)
    published: int = 0  # distinct columns (one per active column per slot)

    @property
    def ok(self) -> bool:
        return (
            not self.missing
            and not self.leaked
            and not self.duplicates
            and self.arrivals == self.expected
        )


def analyze_columns(
    pubs: dict[PubKey, tuple[int, bool]],
    arrs: list[Arrival],
    custodiers: dict[int, set[int]],
) -> ColumnResult:
    """Cross-check column arrivals (Shadow slog) against the custody sets: each column reaches
    exactly custodiers(column) \\ {origin} — no missing, leaked, or dup. The column index is in
    the subnet field; origin is the proposer (attester is unused, -1)."""
    col_pubs = {(s, col, o): t for (k, s, col, _a, o), (t, _v) in pubs.items() if k == COLUMN_KIND}
    col_arrs = [(n, s, col, o, t) for (n, k, s, col, _a, o, t) in arrs if k == COLUMN_KIND]

    counts = Counter((n, s, col, o) for n, s, col, o, _ in col_arrs)
    received = set(counts)
    duplicates = sorted(k for k, c in counts.items() if c > 1)

    missing: list[tuple[int, int, int, int]] = []
    expected = 0
    for slot, col, origin in col_pubs:
        for node in custodiers.get(col, set()):
            if node == origin:
                continue
            expected += 1
            if (node, slot, col, origin) not in received:
                missing.append((node, slot, col, origin))
    missing.sort()

    leaked = sorted(
        (n, s, col, o) for n, s, col, o, _ in col_arrs if n not in custodiers.get(col, set())
    )
    delays_ms = sorted(
        (t - col_pubs[(s, col, o)]) / 1e6 for n, s, col, o, t in col_arrs if (s, col, o) in col_pubs
    )
    return ColumnResult(len(col_arrs), expected, missing, leaked, duplicates, delays_ms, len(col_pubs))


def analyze_columns_csv(path: Path, schedule_data: dict) -> ColumnResult:
    """Column coverage for the simnet backend. The CSV is keyed by (slot, subnet=column) with
    no origin column, so each column's origin is its slot's proposer (which originates every
    column). Custodiers come from schedule.json's column_subscribers; all columns are active
    each slot."""
    custodiers = {col: set(m) for col, m in enumerate(schedule_data["column_subscribers"])}
    proposer_of = {sp["slot"]: sp["proposer"] for sp in schedule_data["slots"]}

    received: dict[tuple[int, int], set[int]] = {}
    counts: Counter = Counter()
    delays_ms: list[float] = []
    leaked: list[tuple[int, int, int, int]] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != COLUMN_KIND:
                continue
            slot, col, node = int(r["slot"]), int(r["subnet"]), int(r["node"])
            origin = proposer_of.get(slot, -1)
            counts[(node, slot, col, origin)] += 1
            received.setdefault((slot, col), set()).add(node)
            delays_ms.append(float(r["delay_ms"]))
            if node not in custodiers.get(col, set()):
                leaked.append((node, slot, col, origin))

    missing: list[tuple[int, int, int, int]] = []
    expected = 0
    for slot, origin in proposer_of.items():
        for col in range(len(custodiers)):
            for node in custodiers.get(col, set()):
                if node == origin:
                    continue
                expected += 1
                if node not in received.get((slot, col), set()):
                    missing.append((node, slot, col, origin))
    duplicates = sorted(k for k, c in counts.items() if c > 1)
    published = len(proposer_of) * len(custodiers)
    return ColumnResult(
        sum(counts.values()), expected, sorted(missing), sorted(leaked), duplicates,
        sorted(delays_ms), published,
    )


@dataclass
class SyncMessageResult:
    """Sync-message check: each member's message reaches exactly its subnet's members \\ {member}.
    Carries the head-vote fraction (the sync analogue of an attestation's block-vote fraction)."""

    arrivals: int
    expected: int
    missing: list[tuple[int, int, int, int]] = field(default_factory=list)
    leaked: list[tuple[int, int, int, int]] = field(default_factory=list)
    duplicates: list[tuple[int, int, int, int]] = field(default_factory=list)
    delays_ms: list[float] = field(default_factory=list)
    fraction_voted_head: float = 0.0
    published: int = 0  # distinct messages (one per member per slot)

    @property
    def ok(self) -> bool:
        return (
            not self.missing
            and not self.leaked
            and not self.duplicates
            and self.arrivals == self.expected
        )


def analyze_sync_messages(
    pubs: dict[PubKey, tuple[int, bool]],
    arrs: list[Arrival],
    subscribers: dict[int, set[int]],
) -> SyncMessageResult:
    """Cross-check sync-message arrivals (Shadow slog) against the subnet member sets: each
    member's message reaches exactly subscribers(subnet) \\ {member} — no missing, leaked, or dup.
    The member is the attester field (origin -1), like an aggregate's aggregator."""
    sm_pubs = {(s, sub, a): (t, v) for (k, s, sub, a, _o), (t, v) in pubs.items() if k == SYNC_MESSAGE_KIND}
    sm_arrs = [(n, s, sub, a, t) for (n, k, s, sub, a, _o, t) in arrs if k == SYNC_MESSAGE_KIND]

    counts = Counter((n, s, sub, a) for n, s, sub, a, _ in sm_arrs)
    received = set(counts)
    duplicates = sorted(k for k, c in counts.items() if c > 1)

    missing: list[tuple[int, int, int, int]] = []
    expected = 0
    for slot, subnet, member in sm_pubs:
        for node in subscribers.get(subnet, set()):
            if node == member:
                continue
            expected += 1
            if (node, slot, subnet, member) not in received:
                missing.append((node, slot, subnet, member))
    missing.sort()

    leaked = sorted(
        (n, s, sub, a) for n, s, sub, a, _ in sm_arrs if n not in subscribers.get(sub, set())
    )
    delays_ms = sorted(
        (t - sm_pubs[(s, sub, a)][0]) / 1e6 for n, s, sub, a, t in sm_arrs if (s, sub, a) in sm_pubs
    )
    head_votes = sum(1 for _t, voted in sm_pubs.values() if voted)
    fraction = head_votes / len(sm_pubs) if sm_pubs else 0.0
    return SyncMessageResult(
        len(sm_arrs), expected, missing, leaked, duplicates, delays_ms, fraction, len(sm_pubs)
    )


def analyze_sync_messages_csv(path: Path, schedule_data: dict) -> SyncMessageResult:
    """Sync-message coverage for the simnet backend. Keyed by (slot, subnet, attester=member) with
    no origin column; the member is its own origin (excluded from coverage). Members come from
    schedule.json's sync_subscribers (stable across slots); every slot every member publishes one."""
    subscribers = {i: set(m) for i, m in enumerate(schedule_data["sync_subscribers"])}
    slots = [sp["slot"] for sp in schedule_data["slots"]]

    received: dict[tuple[int, int, int], set[int]] = {}
    counts: Counter = Counter()
    voted: dict[tuple[int, int, int], bool] = {}
    delays_ms: list[float] = []
    leaked: list[tuple[int, int, int, int]] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != SYNC_MESSAGE_KIND:
                continue
            slot, subnet, member = int(r["slot"]), int(r["subnet"]), int(r["attester"])
            node = int(r["node"])
            counts[(node, slot, subnet, member)] += 1
            received.setdefault((slot, subnet, member), set()).add(node)
            voted[(slot, subnet, member)] = r["voted_block"] == "true"
            delays_ms.append(float(r["delay_ms"]))
            if node not in subscribers.get(subnet, set()):
                leaked.append((node, slot, subnet, member))

    missing: list[tuple[int, int, int, int]] = []
    expected = 0
    for slot in slots:
        for subnet, members in subscribers.items():
            for member in members:
                for node in members:
                    if node == member:
                        continue
                    expected += 1
                    if node not in received.get((slot, subnet, member), set()):
                        missing.append((node, slot, subnet, member))
    duplicates = sorted(k for k, c in counts.items() if c > 1)
    head_votes = sum(1 for v in voted.values() if v)
    fraction = head_votes / len(voted) if voted else 0.0
    published = len(slots) * sum(len(m) for m in subscribers.values())
    return SyncMessageResult(
        sum(counts.values()), expected, sorted(missing), sorted(leaked), duplicates,
        sorted(delays_ms), fraction, published,
    )


@dataclass
class AggregateResult:
    """Aggregate check: each aggregator publishes one distinct aggregate (signed with its key)
    on the global topic, which reaches every node EXCEPT that aggregator, exactly once. A node
    receiving it twice (duplicate) or the aggregator receiving its own (leaked) is a failure.
    The aggregator is carried in the attester field (the aggregates are NOT deduped)."""

    arrivals: int
    expected: int
    missing: list[tuple[int, int, int, int]] = field(default_factory=list)
    leaked: list[tuple[int, int, int, int]] = field(default_factory=list)
    duplicates: list[tuple[int, int, int, int]] = field(default_factory=list)
    delays_ms: list[float] = field(default_factory=list)
    published: int = 0  # distinct aggregates (one per aggregator, Σ_c |A_c|)

    @property
    def ok(self) -> bool:
        return (
            not self.missing
            and not self.leaked
            and not self.duplicates
            and self.arrivals == self.expected
        )


def _aggregate_published(schedule_data: dict) -> tuple[set[tuple[int, int, int]], int]:
    """The published aggregates (slot, subnet, aggregator) — one per aggregator — and N."""
    n = schedule_data["params"]["n"]
    published: set[tuple[int, int, int]] = set()
    for sp in schedule_data["slots"]:
        for ci, aggs in enumerate(sp["aggregators"]):
            subnet = sp["subnet_of"][ci]
            for aggregator in aggs:
                published.add((sp["slot"], subnet, aggregator))
    return published, n


def _aggregate_result(
    published: set[tuple[int, int, int]],
    n: int,
    counts: Counter,
    received: dict[tuple[int, int, int], set[int]],
    delays_ms: list[float],
) -> AggregateResult:
    """Shared core: expected = Σ (N − 1) over published aggregates (each reaches every node
    but its aggregator); an arrival at the aggregate's own aggregator is a leak (loopback not
    skipped); a repeat arrival is a duplicate."""
    duplicates = sorted(k for k, c in counts.items() if c > 1)
    missing: list[tuple[int, int, int, int]] = []
    expected = 0
    for slot, subnet, aggregator in published:
        for node in range(n):
            if node == aggregator:
                continue
            expected += 1
            if node not in received.get((slot, subnet, aggregator), set()):
                missing.append((node, slot, subnet, aggregator))
    leaked = sorted(
        (node, slot, subnet, aggregator)
        for (node, slot, subnet, aggregator) in counts
        if node == aggregator
    )
    return AggregateResult(
        sum(counts.values()), expected, sorted(missing), leaked, duplicates,
        sorted(delays_ms), len(published),
    )


def analyze_aggregates(
    pubs: dict[PubKey, tuple[int, bool]], arrs: list[Arrival], schedule_data: dict
) -> AggregateResult:
    """Cross-check aggregate arrivals (Shadow slog) against the aggregator sets. The aggregator
    is the attester field; origin is unused (-1)."""
    published, n = _aggregate_published(schedule_data)
    agg_pubs = {(s, sub, a): t for (k, s, sub, a, _o), (t, _v) in pubs.items() if k == AGGREGATE_KIND}
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    for node, k, slot, subnet, aggregator, _o, t in arrs:
        if k != AGGREGATE_KIND:
            continue
        counts[(node, slot, subnet, aggregator)] += 1
        received.setdefault((slot, subnet, aggregator), set()).add(node)
        if (slot, subnet, aggregator) in agg_pubs:
            delays_ms.append((t - agg_pubs[(slot, subnet, aggregator)]) / 1e6)
    return _aggregate_result(published, n, counts, received, delays_ms)


def analyze_aggregates_csv(path: Path, schedule_data: dict) -> AggregateResult:
    """Aggregate coverage for the simnet backend. The CSV is keyed by
    (slot, subnet, attester=aggregator); there is no origin column."""
    published, n = _aggregate_published(schedule_data)
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != AGGREGATE_KIND:
                continue
            slot, subnet, aggregator = int(r["slot"]), int(r["subnet"]), int(r["attester"])
            node = int(r["node"])
            counts[(node, slot, subnet, aggregator)] += 1
            received.setdefault((slot, subnet, aggregator), set()).add(node)
            delays_ms.append(float(r["delay_ms"]))
    return _aggregate_result(published, n, counts, received, delays_ms)


def _sync_contribution_published(schedule_data: dict) -> tuple[set[tuple[int, int, int]], int]:
    """The published contributions (slot, subnet, aggregator) — one per aggregator — and N.
    sync_aggregators is indexed by subnet directly (no subnet_of map)."""
    n = schedule_data["params"]["n"]
    published: set[tuple[int, int, int]] = set()
    for sp in schedule_data["slots"]:
        for subnet, aggs in enumerate(sp.get("sync_aggregators") or []):
            for aggregator in aggs:
                published.add((sp["slot"], subnet, aggregator))
    return published, n


def analyze_sync_contributions(
    pubs: dict[PubKey, tuple[int, bool]], arrs: list[Arrival], schedule_data: dict
) -> AggregateResult:
    """Cross-check sync-contribution arrivals (Shadow slog) against the aggregator sets — the same
    global-topic, distinct-per-aggregator shape as aggregates (aggregator in the attester field)."""
    published, n = _sync_contribution_published(schedule_data)
    cont_pubs = {(s, sub, a): t for (k, s, sub, a, _o), (t, _v) in pubs.items() if k == SYNC_CONTRIBUTION_KIND}
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    for node, k, slot, subnet, aggregator, _o, t in arrs:
        if k != SYNC_CONTRIBUTION_KIND:
            continue
        counts[(node, slot, subnet, aggregator)] += 1
        received.setdefault((slot, subnet, aggregator), set()).add(node)
        if (slot, subnet, aggregator) in cont_pubs:
            delays_ms.append((t - cont_pubs[(slot, subnet, aggregator)]) / 1e6)
    return _aggregate_result(published, n, counts, received, delays_ms)


def analyze_sync_contributions_csv(path: Path, schedule_data: dict) -> AggregateResult:
    """Sync-contribution coverage for the simnet backend (kind=6, attester=aggregator, no origin)."""
    published, n = _sync_contribution_published(schedule_data)
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != SYNC_CONTRIBUTION_KIND:
                continue
            slot, subnet, aggregator = int(r["slot"]), int(r["subnet"]), int(r["attester"])
            node = int(r["node"])
            counts[(node, slot, subnet, aggregator)] += 1
            received.setdefault((slot, subnet, aggregator), set()).add(node)
            delays_ms.append(float(r["delay_ms"]))
    return _aggregate_result(published, n, counts, received, delays_ms)


def load_committee(run_dir: Path) -> dict[int, set[int]] | None:
    """Subscriber sets keyed by subnet (stable for the run) from schedule.json, or None
    if absent (a block-only run)."""
    path = run_dir / "schedule.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return {subnet: set(members) for subnet, members in enumerate(data["subnet_subscribers"])}


def load_column_subscribers(run_dir: Path) -> dict[int, set[int]] | None:
    """Custodier sets keyed by column (stable for the run) from schedule.json's
    column_subscribers, or None if absent (a run without the data-columns phase)."""
    path = run_dir / "schedule.json"
    if not path.exists():
        return None
    cols = json.loads(path.read_text()).get("column_subscribers")
    if cols is None:
        return None
    return {col: set(members) for col, members in enumerate(cols)}


def load_sync_subscribers(run_dir: Path) -> dict[int, set[int]] | None:
    """Sync-committee member sets keyed by subnet (stable for the run) from schedule.json's
    sync_subscribers, or None if absent (a run without the sync phase)."""
    path = run_dir / "schedule.json"
    if not path.exists():
        return None
    subs = json.loads(path.read_text()).get("sync_subscribers")
    if subs is None:
        return None
    return {i: set(members) for i, members in enumerate(subs)}


def load_proposers(run_dir: Path) -> list[int] | None:
    """Per-slot block proposer (a supernode) from schedule.json, or None (block-only run)."""
    path = run_dir / "schedule.json"
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
    backends fail loudly if they, or schedule.json and topology.json, ever disagree."""
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


def analyze_attestations_csv(path: Path, schedule_data: dict) -> AttestResult:
    """Coverage/no-leak for the simnet backend (the real cross-backend graph). The arrival
    CSV is keyed by (slot, subnet, attester) with no origin column, so each attestation's
    origin comes from schedule.json's draw. Each published attestation must reach exactly
    subscribers(subnet) \\ {origin} — missing/leaked/duplicate all fail."""
    subscribers = {subnet: set(m) for subnet, m in enumerate(schedule_data["subnet_subscribers"])}
    origin_of: dict[tuple[int, int, int], int] = {}  # (slot,subnet,attester) -> publishing node
    for sp in schedule_data["slots"]:
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
    schedule_path = run_dir / "schedule.json"
    if schedule_path.exists():
        schedule_data = json.loads(schedule_path.read_text())
        subscribers = {sub: set(mem) for sub, mem in enumerate(schedule_data["subnet_subscribers"])}
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

        if schedule_data["slots"] and schedule_data["slots"][0].get("aggregators"):
            gres = analyze_aggregates(pubs, arrs, schedule_data)
            ok = ok and gres.ok
            print(f"aggregates published (distinct): {gres.published}")
            print(f"aggregate arrivals: {gres.arrivals} (expected {gres.expected})")
            print(f"  missing: {len(gres.missing)}  leaked: {len(gres.leaked)}  duplicates: {len(gres.duplicates)}")
            if gres.delays_ms:
                c = cdf(gres.delays_ms)
                print(f"  aggregate CDF (ms): p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}")

        if schedule_data.get("column_subscribers"):
            custodiers = {col: set(m) for col, m in enumerate(schedule_data["column_subscribers"])}
            cres = analyze_columns(pubs, arrs, custodiers)
            ok = ok and cres.ok
            print(f"columns published (distinct): {cres.published}")
            print(f"column arrivals: {cres.arrivals} (expected {cres.expected})")
            print(f"  missing: {len(cres.missing)}  leaked: {len(cres.leaked)}  duplicates: {len(cres.duplicates)}")
            if cres.delays_ms:
                c = cdf(cres.delays_ms)
                print(f"  column CDF (ms): p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}")

        if schedule_data.get("sync_subscribers"):
            sync_subs = {i: set(m) for i, m in enumerate(schedule_data["sync_subscribers"])}
            smres = analyze_sync_messages(pubs, arrs, sync_subs)
            ok = ok and smres.ok
            print(f"sync messages published: {smres.published}")
            print(f"sync message arrivals: {smres.arrivals} (expected {smres.expected})")
            print(f"  missing: {len(smres.missing)}  leaked: {len(smres.leaked)}  duplicates: {len(smres.duplicates)}")
            if smres.delays_ms:
                c = cdf(smres.delays_ms)
                print(f"  sync message CDF (ms): p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}")
            print(f"  fraction voted head: {smres.fraction_voted_head:.3f}")

            if schedule_data["slots"] and schedule_data["slots"][0].get("sync_aggregators"):
                scres = analyze_sync_contributions(pubs, arrs, schedule_data)
                ok = ok and scres.ok
                print(f"sync contributions published (distinct): {scres.published}")
                print(f"sync contribution arrivals: {scres.arrivals} (expected {scres.expected})")
                print(f"  missing: {len(scres.missing)}  leaked: {len(scres.leaked)}  duplicates: {len(scres.duplicates)}")
                if scres.delays_ms:
                    c = cdf(scres.delays_ms)
                    print(f"  sync contribution CDF (ms): p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}")

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
