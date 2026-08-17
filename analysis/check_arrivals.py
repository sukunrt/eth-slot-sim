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

Decoupled consensus (opt-in) adds three kinds: AC votes (kind=7, global topic, one per
VRF-selected validator reaching every node but its publisher, carrying a voted-block bool);
finality votes (kind=8, per finality-subnet, one per hosted validator reaching the other
subnet members); and finality aggregates (kind=9, global topic, one distinct aggregate per
aggregator reaching every node but itself) — the AC/finality twins of aggregates/sync.

ePBS (on by default) replaces the single block with two block-shaped kinds: consensus
blocks (kind=10) and execution payloads (kind=11), both must reach every other node; the
payload-lag metric is the per-(node, slot) gap between their first arrivals.

Usage: python analysis/check_arrivals.py <run-dir> [--parquet [DIR]]

--parquet switches to the DuckDB fast path over the parquet event tables written by
analysis/to_parquet.py (DIR defaults to <run-dir>/parquet) — same report, minutes faster
on big runs. This module itself stays stdlib-only: it is the REFERENCE implementation the
parquet path (analysis/duck_report.py) is equivalence-tested against.

Output: the human summary on stdout AND the full machine-readable report in
<run-dir>/analysis.json — per-kind and per-slot CDFs (fine percentile grid), both loss
structures (publisher-side drops vs missing-by-receiver), validator skew, proposer guard.
The JSON is the durable artifact: keep extending it, never reshape it, so old runs stay
comparable and shadow.data never needs re-parsing.
"""

import argparse
import csv
import json
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

BLOCK_KIND = 1
ATTEST_KIND = 2
AGGREGATE_KIND = 3
COLUMN_KIND = 4
SYNC_MESSAGE_KIND = 5
SYNC_CONTRIBUTION_KIND = 6
AC_VOTE_KIND = 7
FINALITY_VOTE_KIND = 8
FINALITY_AGGREGATE_KIND = 9
CONSENSUS_BLOCK_KIND = 10
EXECUTION_PAYLOAD_KIND = 11

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


def analyze(
    pubs: dict[PubKey, tuple[int, bool]],
    arrs: list[Arrival],
    node_nums: set[int],
    kind: int = BLOCK_KIND,
) -> Result:
    """Cross-check a block-shaped kind (blocks, ePBS consensus blocks / execution payloads)
    against publishes: every node != origin should receive each published message once."""
    block_pubs = {(s, o): t for (k, s, _sub, _a, o), (t, _v) in pubs.items() if k == kind}
    block_arrs = [(n, s, o, t) for (n, k, s, _sub, _a, o, t) in arrs if k == kind]

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


def payload_lag(arrs: list[Arrival]) -> list[float]:
    """ePBS headline metric: per (node, slot), the extra wait the payload costs — first
    execution-payload arrival minus first consensus-block arrival, in ms, over the nodes
    that received both. Sorted; empty when the run is not ePBS."""
    first: dict[tuple[int, int, int], int] = {}  # (kind, node, slot) -> min t_ns
    for n, k, s, _sub, _a, _o, t in arrs:
        if k in (CONSENSUS_BLOCK_KIND, EXECUTION_PAYLOAD_KIND):
            key = (k, n, s)
            if key not in first or t < first[key]:
                first[key] = t
    return sorted(
        (t - first[(CONSENSUS_BLOCK_KIND, n, s)]) / 1e6
        for (k, n, s), t in first.items()
        if k == EXECUTION_PAYLOAD_KIND and (CONSENSUS_BLOCK_KIND, n, s) in first
    )


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


def _ac_vote_result(
    published: dict[tuple[int, int, int], bool],
    n: int,
    counts: Counter,
    received: dict[tuple[int, int, int], set[int]],
    delays_ms: list[float],
) -> AttestResult:
    """Shared core: each published AC vote (slot, val, origin) — one per voter on the single global
    topic — must reach every node EXCEPT its publisher (origin), exactly once; a repeat arrival is a
    duplicate and the publisher recording its own copy (loopback not skipped) is a leak. There are no
    subnets, so any other node may receive it (no membership leak). Carries the voted-block fraction
    over the published votes, like an attestation's."""
    duplicates = sorted(k for k, c in counts.items() if c > 1)
    missing: list[tuple[int, int, int, int, int]] = []
    expected = 0
    for slot, val, origin in published:
        for node in range(n):
            if node == origin:
                continue
            expected += 1
            if node not in received.get((slot, val, origin), set()):
                missing.append((node, slot, -1, val, origin))
    leaked = sorted(
        (node, slot, -1, val, origin)
        for (node, slot, val, origin) in counts
        if node == origin
    )
    block_votes = sum(1 for v in published.values() if v)
    fraction = block_votes / len(published) if published else 0.0
    return AttestResult(
        sum(counts.values()), expected, sorted(missing), leaked, duplicates,
        sorted(delays_ms), fraction, len(published),
    )


def analyze_ac_votes(
    pubs: dict[PubKey, tuple[int, bool]], arrs: list[Arrival], schedule_data: dict
) -> AttestResult:
    """Cross-check AC-vote arrivals (Shadow slog) against the per-slot voter sets — the global-topic,
    N−1 coverage shape of an aggregate, but the publisher rides Origin (not Attester) and the vote
    carries a voted-block bool. Each voter publishes one vote, identified by (slot, val, origin)."""
    n = schedule_data["params"]["n"]
    published: dict[tuple[int, int, int], bool] = {}
    for sp in schedule_data["slots"]:
        for r in sp.get("ac_voters") or []:
            key = (sp["slot"], r["val"], r["node"])
            published[key] = pubs.get((AC_VOTE_KIND, sp["slot"], -1, r["val"], r["node"]), (0, False))[1]
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    av_pubs = {(s, a, o): t for (k, s, _sub, a, o), (t, _v) in pubs.items() if k == AC_VOTE_KIND}
    for node, k, slot, _sub, val, origin, t in arrs:
        if k != AC_VOTE_KIND:
            continue
        counts[(node, slot, val, origin)] += 1
        received.setdefault((slot, val, origin), set()).add(node)
        if (slot, val, origin) in av_pubs:
            delays_ms.append((t - av_pubs[(slot, val, origin)]) / 1e6)
    return _ac_vote_result(published, n, counts, received, delays_ms)


def analyze_ac_votes_csv(path: Path, schedule_data: dict) -> AttestResult:
    """AC-vote coverage for the simnet backend. The CSV is keyed by (slot, attester=val) with no
    origin column, so each vote's publisher comes from schedule.json's ac_voters draw; the voted-block
    bool rides the CSV's voted_block column."""
    n = schedule_data["params"]["n"]
    origin_of: dict[tuple[int, int], int] = {}  # (slot, val) -> publishing node
    for sp in schedule_data["slots"]:
        for r in sp.get("ac_voters") or []:
            origin_of[(sp["slot"], r["val"])] = r["node"]

    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    voted: dict[tuple[int, int, int], bool] = {}
    delays_ms: list[float] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != AC_VOTE_KIND:
                continue
            slot, val, node = int(r["slot"]), int(r["attester"]), int(r["node"])
            origin = origin_of.get((slot, val), -1)
            counts[(node, slot, val, origin)] += 1
            received.setdefault((slot, val, origin), set()).add(node)
            voted[(slot, val, origin)] = r["voted_block"] == "true"
            delays_ms.append(float(r["delay_ms"]))

    published = {(s, val, o): voted.get((s, val, o), False) for (s, val), o in origin_of.items()}
    return _ac_vote_result(published, n, counts, received, delays_ms)


def _finality_receivers(
    schedule_data: dict,
) -> tuple[Callable[[int, int], set[int]], Callable[[int, int], set[int]]]:
    """(required, allowed) receiver sets of a vote keyed by (round key, subnet) — the round key
    is the finality slot in base mode, the AC slot under segregation (finality_round_of present).
    required = the subnet's stable members ∪ that round's aggregator HOST nodes: the hosts are
    subscribed from the pre-join through the aggregation deadline, and collecting the round's
    votes is their whole job — REQUIRED receivers, not leaks. Under segregation `allowed` also
    includes the NEXT slot's aggregator hosts: they pre-join at the start of slot s (for s+1),
    BEFORE slot s's votes publish, so they hear the round's tail by construction — legitimate
    receivers, but not required (their mesh may still be forming). (The publisher is excluded by
    the callers, which know each vote's host.)"""
    members = {i: set(m) for i, m in enumerate(schedule_data["finality_subscribers"])}
    k = schedule_data["params"]["ac_slots_per_finality_slot"]
    segregated = "finality_round_of" in schedule_data
    agg_hosts: dict[tuple[int, int], set[int]] = {}
    for sp in schedule_data["slots"]:
        if not segregated and sp["slot"] % k != 0:
            continue
        key = sp["slot"] if segregated else sp["slot"] // k
        for subnet, refs in enumerate(sp.get("finality_aggregators") or []):
            agg_hosts[(key, subnet)] = {r["node"] for r in refs}

    def required(key: int, subnet: int) -> set[int]:
        return members.get(subnet, set()) | agg_hosts.get((key, subnet), set())

    def allowed(key: int, subnet: int) -> set[int]:
        out = required(key, subnet)
        if segregated:
            out = out | agg_hosts.get((key + 1, subnet), set())
        return out

    return required, allowed


def analyze_finality_votes(
    pubs: dict[PubKey, tuple[int, bool]],
    arrs: list[Arrival],
    schedule_data: dict,
) -> SyncMessageResult:
    """Cross-check finality-vote arrivals (Shadow slog) against the expected receiver sets: each
    validator's vote reaches the required receivers (_finality_receivers) \\ {host} — no missing,
    leaked (beyond the allowed set), or dup. The validator is the attester field and the host is
    the origin (which it's excluded from), like an attestation; the slot field carries the
    finality slot (base) or the AC slot (segregation — the expected sets derive from the actual
    publishes, so the round filter is implicit). Dissemination-only (no voted bool)."""
    required, allowed = _finality_receivers(schedule_data)
    fv_pubs = {(s, sub, a, o): t for (k, s, sub, a, o), (t, _v) in pubs.items() if k == FINALITY_VOTE_KIND}
    fv_arrs = [(n, s, sub, a, o, t) for (n, k, s, sub, a, o, t) in arrs if k == FINALITY_VOTE_KIND]

    # Arrivals at allowed-but-not-required receivers are real receipts but outside the strict
    # got==want contract — dropped from the accounting (not leaks, not counted); beyond the
    # allowed set is a leak.
    leaked = sorted(
        (n, s, sub, a) for n, s, sub, a, _o, _t in fv_arrs if n not in allowed(s, sub)
    )
    strict = [(n, s, sub, a, o, t) for n, s, sub, a, o, t in fv_arrs if n in required(s, sub)]

    counts = Counter((n, s, sub, a, o) for n, s, sub, a, o, _ in strict)
    received = set(counts)
    duplicates = sorted((n, s, sub, a) for (n, s, sub, a, _o), c in counts.items() if c > 1)

    missing: list[tuple[int, int, int, int]] = []
    expected = 0
    for slot, subnet, val, host in fv_pubs:
        for node in required(slot, subnet):
            if node == host:
                continue
            expected += 1
            if (node, slot, subnet, val, host) not in received:
                missing.append((node, slot, subnet, val))
    missing.sort()

    delays_ms = sorted(
        (t - fv_pubs[(s, sub, a, o)]) / 1e6 for n, s, sub, a, o, t in strict if (s, sub, a, o) in fv_pubs
    )
    return SyncMessageResult(
        len(strict), expected, missing, leaked, duplicates, delays_ms, 0.0, len(fv_pubs)
    )


def _finality_votes(schedule_data: dict) -> list[tuple[int, int, int]]:
    """The (subnet, val, host) of every potential vote, on ITS validator's drawn subnet
    (finality_subnet_of — subnets partition the validator set, so a host publishes wherever its
    keys landed, fanning out beyond its own membership). One per validator per finality slot in
    base mode; under segregation the caller filters to the round's validators per AC slot. A
    validator's host comes from validator_counts when present (the Dist seam: node i hosts the
    contiguous ids [Σ counts[:i], Σ counts[:i+1])), else uniform V→N (val % N == host) — the
    same duties FinalityVoteDuties produces."""
    n = schedule_data["params"]["n"]
    subnet_of = schedule_data["finality_subnet_of"]
    counts = schedule_data.get("validator_counts")
    if counts is not None:
        host_of = []
        for host, c in enumerate(counts):
            host_of += [host] * c
    else:
        host_of = [val % n for val in range(len(subnet_of))]
    return [(subnet, val, host_of[val]) for val, subnet in enumerate(subnet_of)]


def analyze_finality_votes_csv(path: Path, schedule_data: dict) -> SyncMessageResult:
    """Finality-vote coverage for the simnet backend. Keyed by (slot=round key, subnet,
    attester=val) with no origin column; a vote's host (publisher, excluded from its coverage)
    comes from _finality_votes (validator_counts when present, else val % N). Expected receivers
    come from _finality_receivers (stable members ∪ the round's aggregator hosts). Base: every
    validator publishes once per finality slot. Segregated: the expected voters of AC slot s are
    the round-(s % k) validators, one slot per fslot each; allowed-but-not-required arrivals
    (the next round's pre-joined aggregator hosts) are dropped from the strict accounting."""
    votes = _finality_votes(schedule_data)
    required, allowed = _finality_receivers(schedule_data)
    k = schedule_data["params"]["ac_slots_per_finality_slot"]
    rounds = schedule_data.get("finality_round_of")
    if rounds is None:  # base: one vote burst per finality slot, keyed by it
        keys = sorted({sp["slot"] // k for sp in schedule_data["slots"]})
    else:  # segregated: every AC slot is a round, keyed by the AC slot itself
        keys = [sp["slot"] for sp in schedule_data["slots"]]

    received: dict[tuple[int, int, int], set[int]] = {}
    counts: Counter = Counter()
    delays_ms: list[float] = []
    leaked: list[tuple[int, int, int, int]] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != FINALITY_VOTE_KIND:
                continue
            slot, subnet, val = int(r["slot"]), int(r["subnet"]), int(r["attester"])
            node = int(r["node"])
            if node not in allowed(slot, subnet):
                leaked.append((node, slot, subnet, val))
                continue
            if node not in required(slot, subnet):
                continue  # the next round's pre-joined aggregator host: real, outside the contract
            counts[(node, slot, subnet, val)] += 1
            received.setdefault((slot, subnet, val), set()).add(node)
            delays_ms.append(float(r["delay_ms"]))

    missing: list[tuple[int, int, int, int]] = []
    expected = 0
    published = 0
    for key in keys:
        for subnet, val, host in votes:
            if rounds is not None and rounds[val] != key % k:
                continue  # not this round's validator
            published += 1
            for node in required(key, subnet):
                if node == host:
                    continue
                expected += 1
                if node not in received.get((key, subnet, val), set()):
                    missing.append((node, key, subnet, val))
    duplicates = sorted(k for k, c in counts.items() if c > 1)
    return SyncMessageResult(
        sum(counts.values()), expected, sorted(missing), sorted(leaked), duplicates,
        sorted(delays_ms), 0.0, published,
    )


def _finality_aggregate_published(schedule_data: dict) -> tuple[set[tuple[int, int, int]], int]:
    """The published finality aggregates (round key, subnet, aggregator host node) — one per
    distinct HOST (two selected validators on one node dedupe to one aggregate) — and N. Base:
    finality_aggregators rides each finality-BOUNDARY slot (slot % k == 0), keyed by the
    finality slot (slot // k). Segregated (finality_round_of present): EVERY slot carries a
    fresh draw, keyed by the AC slot itself."""
    n = schedule_data["params"]["n"]
    k = schedule_data["params"]["ac_slots_per_finality_slot"]
    segregated = "finality_round_of" in schedule_data
    published: set[tuple[int, int, int]] = set()
    for sp in schedule_data["slots"]:
        if not segregated and sp["slot"] % k != 0:
            continue
        key = sp["slot"] if segregated else sp["slot"] // k
        for subnet, refs in enumerate(sp.get("finality_aggregators") or []):
            for ref in refs:
                published.add((key, subnet, ref["node"]))
    return published, n


def finality_coverage_at_deadline(
    pubs: dict[PubKey, tuple[int, bool]], arrs: list[Arrival], schedule_data: dict
) -> dict[tuple[int, int], float]:
    """Per (round key, subnet): the fraction of the round's published votes that reached the
    round's aggregator HOSTS by the aggregation deadline — "how much of the round's vote does
    each aggregate capture?", the Python twin of the Go Recorder's FinalityCoverageAtDeadline
    (a headline metric, NOT an invariant: a late tail lowers it without being a failure). The
    deadline is observed, not configured: each host's own FinalityAggregate publish instant
    (all of a round's aggregates publish at one fixed-time deadline by construction). A vote is
    not expected back at its own publisher; (key, subnet) cells whose hosts published no
    aggregate are skipped (no deadline to measure against). Keys are finality slots in base
    mode, AC slots under segregation — whatever rides the slot field. Shadow-slog only: the
    simnet CSV carries per-message delays, not the absolute instants this comparison needs."""
    agg_pub_t = {(s, sub, a): t for (k, s, sub, a, _o), (t, _v) in pubs.items()
                 if k == FINALITY_AGGREGATE_KIND}
    first_arr: dict[tuple[int, int, int, int, int], int] = {}
    for n, k, s, sub, a, o, t in arrs:
        if k != FINALITY_VOTE_KIND:
            continue
        key = (n, s, sub, a, o)
        if key not in first_arr or t < first_arr[key]:
            first_arr[key] = t
    published, _n = _finality_aggregate_published(schedule_data)
    hosts: dict[tuple[int, int], set[int]] = {}
    for key, subnet, host in published:
        hosts.setdefault((key, subnet), set()).add(host)
    total: Counter = Counter()
    covered: Counter = Counter()
    for (k, s, sub, val, origin), (_t, _v) in pubs.items():
        if k != FINALITY_VOTE_KIND:
            continue
        for host in hosts.get((s, sub), ()):
            if host == origin:
                continue
            due = agg_pub_t.get((s, sub, host))
            if due is None:
                continue  # this host never published: no deadline for the pair
            total[(s, sub)] += 1
            at = first_arr.get((host, s, sub, val, origin))
            if at is not None and at <= due:
                covered[(s, sub)] += 1
    return {cell: covered[cell] / n for cell, n in total.items()}


def analyze_finality_aggregates(
    pubs: dict[PubKey, tuple[int, bool]], arrs: list[Arrival], schedule_data: dict
) -> AggregateResult:
    """Cross-check finality-aggregate arrivals (Shadow slog) against the aggregator sets — the same
    global-topic, distinct-per-aggregator shape as aggregates (aggregator in the attester field,
    origin -1). The finality slot rides the slot field."""
    published, n = _finality_aggregate_published(schedule_data)
    agg_pubs = {(s, sub, a): t for (k, s, sub, a, _o), (t, _v) in pubs.items() if k == FINALITY_AGGREGATE_KIND}
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    for node, k, slot, subnet, aggregator, _o, t in arrs:
        if k != FINALITY_AGGREGATE_KIND:
            continue
        counts[(node, slot, subnet, aggregator)] += 1
        received.setdefault((slot, subnet, aggregator), set()).add(node)
        if (slot, subnet, aggregator) in agg_pubs:
            delays_ms.append((t - agg_pubs[(slot, subnet, aggregator)]) / 1e6)
    return _aggregate_result(published, n, counts, received, delays_ms)


def analyze_finality_aggregates_csv(path: Path, schedule_data: dict) -> AggregateResult:
    """Finality-aggregate coverage for the simnet backend (kind=9, slot=finality slot,
    attester=aggregator, no origin)."""
    published, n = _finality_aggregate_published(schedule_data)
    counts: Counter = Counter()
    received: dict[tuple[int, int, int], set[int]] = {}
    delays_ms: list[float] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if int(r.get("kind", BLOCK_KIND)) != FINALITY_AGGREGATE_KIND:
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


def load_finality_subscribers(run_dir: Path) -> dict[int, set[int]] | None:
    """Finality-subnet member sets keyed by subnet (the partition of all N, stable for the run) from
    schedule.json's finality_subscribers, or None if absent (a run without decoupled consensus)."""
    path = run_dir / "schedule.json"
    if not path.exists():
        return None
    subs = json.loads(path.read_text()).get("finality_subscribers")
    if subs is None:
        return None
    return {i: set(members) for i, members in enumerate(subs)}


def load_finality_rounds(run_dir: Path) -> list[int] | None:
    """finality_round_of (validator → round, the per-AC-slot segregation draw) from
    schedule.json, or None when the run is not segregated — the variant detector."""
    path = run_dir / "schedule.json"
    if not path.exists():
        return None
    return json.loads(path.read_text()).get("finality_round_of")


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


PERCENTILES = (1, 5, 10, 25, 50, 75, 90, 95, 99, 99.9, 100)


def percentile_grid(delays_ms: list[float]) -> dict[str, float]:
    """Fine percentile grid, enough to plot a CDF from analysis.json without ever
    re-parsing the run's shadow.data."""
    return {f"p{g:g}": percentile(delays_ms, g) for g in PERCENTILES}


def delay_details(
    pubs: dict[PubKey, tuple[int, bool]], arrs: list[Arrival], kind: int
) -> tuple[dict[int, list[float]], list[PubKey]]:
    """Per-slot arrival delays and publisher-side drops (publishes nobody received) for one
    kind. Publish and arrival log the same identity fields, so the exact-key match is
    uniform across kinds; for the finality kinds the slot field is the finality slot."""
    pub_t = {key: t for key, (t, _v) in pubs.items() if key[0] == kind}
    per_slot: dict[int, list[float]] = {}
    reached: Counter = Counter()
    for n, k, s, sub, a, o, t in arrs:
        key = (k, s, sub, a, o)
        if k != kind or key not in pub_t:
            continue
        per_slot.setdefault(s, []).append((t - pub_t[key]) / 1e6)
        reached[key] += 1
    drops = sorted(key for key in pub_t if not reached[key])
    return per_slot, drops


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


def _kind_report(
    label: str,
    plural: str,
    res,
    pubs: dict[PubKey, tuple[int, bool]],
    arrs: list[Arrival],
    kind: int,
    published: int | None = None,
    voted: tuple[str, float] | None = None,
) -> dict:
    """Print one kind's section and return its JSON-ready summary: counts, headline +
    fine-grid CDFs, per-slot CDFs (the boundary-slot contention view), and both loss
    structures — publisher-side drops vs missing-by-receiver."""
    per_slot, drops = delay_details(pubs, arrs, kind)
    if published is None:
        published = getattr(res, "published", 0)
    leaked = getattr(res, "leaked", [])
    missing_by_node = Counter(m[0] for m in res.missing)
    rep = {
        "published": published,
        "arrivals": res.arrivals,
        "expected": res.expected,
        "missing": len(res.missing),
        "leaked": len(leaked),
        "duplicates": len(res.duplicates),
        "publisher_drops": len(drops),
        "ok": res.ok,
        "cdf_ms": cdf(res.delays_ms),
        "percentiles_ms": percentile_grid(res.delays_ms),
        "per_slot": {str(slot): cdf(d) for slot, d in sorted(per_slot.items())},
        "missing_by_node": {str(n): c for n, c in missing_by_node.most_common(20)},
        "missing_examples": [list(m) for m in res.missing[:20]],
        "leaked_examples": [list(m) for m in leaked[:20]],
        "drop_examples": [list(d) for d in drops[:20]],
    }
    if voted is not None:
        rep[voted[0]] = voted[1]

    def fmt(c: dict[str, float]) -> str:
        return f"p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}"

    print(f"{plural} published: {published}")
    print(f"{label} arrivals: {res.arrivals} (expected {res.expected})")
    print(
        f"  missing: {len(res.missing)}  leaked: {len(leaked)}  "
        f"duplicates: {len(res.duplicates)}  publisher drops: {len(drops)}"
    )
    if res.delays_ms:
        print(f"  {label} CDF (ms): {fmt(rep['cdf_ms'])}")
    if len(per_slot) > 1:
        print("  per-slot CDF (ms):")
        for slot, c in rep["per_slot"].items():
            print(f"    slot {slot}: n={c['count']} {fmt(c)}")
    if missing_by_node:
        top = "  ".join(f"node {n}: {c}" for n, c in missing_by_node.most_common(10))
        print(f"  top missing receivers: {top}")
    if drops:
        print(f"  publisher drops (kind,slot,subnet,attester,origin): {drops[:10]}")
    if leaked:
        print(f"  leaked (first 10): {leaked[:10]}")
    if voted is not None:
        print(f"  {voted[0].replace('_', ' ')}: {voted[1]:.3f}")
    return rep


def build_report(run_dir: Path) -> dict:
    """Analyze a Shadow run dir: print the human summary AND write the full
    machine-readable report to <run_dir>/analysis.json. The JSON carries everything the
    run comparisons need — per-kind/per-slot CDFs, loss structure, validator skew — so a
    question about an old run never requires re-parsing its shadow.data."""
    pubs, arrs, node_nums = load_run(run_dir)
    report: dict = {"run_dir": str(run_dir), "nodes": len(node_nums), "kinds": {}}
    kinds = report["kinds"]

    print(f"nodes: {len(node_nums)}")
    res = analyze(pubs, arrs, node_nums)
    ok = res.ok
    n_blocks = sum(1 for k, *_ in pubs if k == BLOCK_KIND)
    kinds["blocks"] = _kind_report(
        "block", "blocks", res, pubs, arrs, BLOCK_KIND, published=n_blocks
    )

    # ePBS (present when any consensus block was published): both halves are block-shaped
    # (every node != origin receives once), plus the payload-lag headline — the extra wait
    # the payload reveal costs each node beyond its consensus-block arrival.
    n_cb = sum(1 for k, *_ in pubs if k == CONSENSUS_BLOCK_KIND)
    if n_cb:
        cres = analyze(pubs, arrs, node_nums, kind=CONSENSUS_BLOCK_KIND)
        ok = ok and cres.ok
        kinds["consensus_blocks"] = _kind_report(
            "consensus block", "consensus blocks", cres, pubs, arrs,
            CONSENSUS_BLOCK_KIND, published=n_cb,
        )
        pres = analyze(pubs, arrs, node_nums, kind=EXECUTION_PAYLOAD_KIND)
        ok = ok and pres.ok
        n_ep = sum(1 for k, *_ in pubs if k == EXECUTION_PAYLOAD_KIND)
        kinds["execution_payloads"] = _kind_report(
            "execution payload", "execution payloads", pres, pubs, arrs,
            EXECUTION_PAYLOAD_KIND, published=n_ep,
        )
        lag = payload_lag(arrs)
        lag_cdf = cdf(lag)
        kinds["execution_payloads"]["payload_lag_ms"] = lag_cdf
        if lag:
            print(
                f"  payload lag CDF (ms): p50={lag_cdf['p50']:.1f} p90={lag_cdf['p90']:.1f} "
                f"p99={lag_cdf['p99']:.1f} p100={lag_cdf['p100']:.1f}"
            )

    schedule_path = run_dir / "schedule.json"
    if schedule_path.exists():
        schedule_data = json.loads(schedule_path.read_text())
        subscribers = {sub: set(mem) for sub, mem in enumerate(schedule_data["subnet_subscribers"])}
        ares = analyze_attestations(pubs, arrs, subscribers)
        ok = ok and ares.ok
        kinds["attestations"] = _kind_report(
            "attestation", "attestations", ares, pubs, arrs, ATTEST_KIND,
            voted=("fraction_voted_block", ares.fraction_voted_block),
        )

        if schedule_data["slots"] and schedule_data["slots"][0].get("aggregators"):
            gres = analyze_aggregates(pubs, arrs, schedule_data)
            ok = ok and gres.ok
            kinds["aggregates"] = _kind_report(
                "aggregate", "aggregates (distinct)", gres, pubs, arrs, AGGREGATE_KIND
            )

        if schedule_data.get("column_subscribers"):
            custodiers = {col: set(m) for col, m in enumerate(schedule_data["column_subscribers"])}
            cres = analyze_columns(pubs, arrs, custodiers)
            ok = ok and cres.ok
            kinds["columns"] = _kind_report(
                "column", "columns (distinct)", cres, pubs, arrs, COLUMN_KIND
            )

        if schedule_data.get("sync_subscribers"):
            sync_subs = {i: set(m) for i, m in enumerate(schedule_data["sync_subscribers"])}
            smres = analyze_sync_messages(pubs, arrs, sync_subs)
            ok = ok and smres.ok
            kinds["sync_messages"] = _kind_report(
                "sync message", "sync messages", smres, pubs, arrs, SYNC_MESSAGE_KIND,
                voted=("fraction_voted_head", smres.fraction_voted_head),
            )

            if schedule_data["slots"] and schedule_data["slots"][0].get("sync_aggregators"):
                scres = analyze_sync_contributions(pubs, arrs, schedule_data)
                ok = ok and scres.ok
                kinds["sync_contributions"] = _kind_report(
                    "sync contribution", "sync contributions (distinct)",
                    scres, pubs, arrs, SYNC_CONTRIBUTION_KIND,
                )

        if schedule_data.get("finality_subscribers"):
            if schedule_data["slots"] and schedule_data["slots"][0].get("ac_voters"):
                avres = analyze_ac_votes(pubs, arrs, schedule_data)
                ok = ok and avres.ok
                kinds["ac_votes"] = _kind_report(
                    "AC vote", "AC votes", avres, pubs, arrs, AC_VOTE_KIND,
                    voted=("fraction_voted_block", avres.fraction_voted_block),
                )

            fvres = analyze_finality_votes(pubs, arrs, schedule_data)
            ok = ok and fvres.ok
            kinds["finality_attestations"] = _kind_report(
                "finality attestation", "finality attestations",
                fvres, pubs, arrs, FINALITY_VOTE_KIND,
            )

            # Boundary slots in base mode, every slot under segregation — either way the
            # aggregator draws ride the slot dicts.
            if any(sp.get("finality_aggregators") for sp in schedule_data["slots"]):
                fares = analyze_finality_aggregates(pubs, arrs, schedule_data)
                ok = ok and fares.ok
                kinds["finality_aggregates"] = _kind_report(
                    "finality aggregate", "finality aggregates (distinct)",
                    fares, pubs, arrs, FINALITY_AGGREGATE_KIND,
                )
                # Coverage at the aggregation deadline: a headline metric (per AC slot under
                # segregation — k points per fslot), not a pass/fail invariant.
                cov = finality_coverage_at_deadline(pubs, arrs, schedule_data)
                if cov:
                    by_slot: dict[str, dict[str, float]] = {}
                    for (slot, subnet), frac in sorted(cov.items()):
                        by_slot.setdefault(str(slot), {})[str(subnet)] = round(frac, 4)
                    kinds["finality_attestations"]["coverage_at_deadline"] = by_slot
                    print("  vote coverage at the aggregation deadline (per slot/subnet):")
                    for slot, subs in by_slot.items():
                        line = "  ".join(f"subnet {s}: {f:.3f}" for s, f in subs.items())
                        print(f"    slot {slot}: {line}")

        counts = schedule_data.get("validator_counts")
        if counts:
            top = sorted(enumerate(counts), key=lambda x: -x[1])[:10]
            report["validators"] = {
                "v": sum(counts),
                "max_per_node": max(counts),
                "top_hosts": [[n, c] for n, c in top],
                "counts_per_node": counts,
            }
            print(
                f"validators: V={sum(counts)} max/node={max(counts)} "
                "top hosts: " + " ".join(f"{n}={c}" for n, c in top[:5])
            )

    proposers = load_proposers(run_dir)
    supernodes = load_supernodes(run_dir)
    if proposers is not None and supernodes is not None:
        problems = check_proposers(proposers, supernodes, block_origins(pubs))
        ok = ok and not problems
        report["proposer_guard"] = {"ok": not problems, "problems": problems[:20]}
        print(f"proposer guard: {'OK' if not problems else 'FAIL'} (all proposers are supernodes)")
        for p in problems[:10]:
            print("  ", p)

    report["result"] = "OK" if ok else "FAIL"
    (run_dir / "analysis.json").write_text(json.dumps(report, indent=2) + "\n")
    print("RESULT:", report["result"])
    return report


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Verify message receipt for a Shadow run.")
    parser.add_argument("run_dir", type=Path)
    parser.add_argument(
        "--parquet",
        nargs="?",
        const="",
        default=None,
        metavar="DIR",
        help="analyze the parquet event tables (DuckDB fast path; see analysis/to_parquet.py) "
        "instead of re-parsing the raw slog text; DIR defaults to <run-dir>/parquet",
    )
    args = parser.parse_args(argv[1:])
    if args.parquet is not None:
        try:
            from analysis import duck_report
        except ImportError:  # invoked as a script: the repo root is not on sys.path
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
            from analysis import duck_report
        parquet_dir = Path(args.parquet) if args.parquet else args.run_dir / "parquet"
        report = duck_report.build_report(args.run_dir, parquet_dir)
    else:
        report = build_report(args.run_dir)
    return 0 if report["result"] == "OK" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
