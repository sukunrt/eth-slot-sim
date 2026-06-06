"""Verify block receipt and print the arrival-delay CDF for a Shadow run.

Each node logs JSON lines on stdout (slog): one `publish` per block it proposes
and one `arrival` per block it receives, with absolute nanosecond timestamps
(comparable across hosts — Shadow shares one clock). This reassembles them by
(slot, origin) to confirm every non-proposer received every block, and prints
the arrival-delay percentiles. Stdlib only.

Usage: python analysis/check_arrivals.py <run-dir>
"""

import json
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

# (slot, origin) -> publish timestamp (ns)
Publishes = dict[tuple[int, int], int]
# (node, slot, origin, t_ns)
Arrival = tuple[int, int, int, int]


def parse_events(lines: Iterable[str]) -> tuple[Publishes, list[Arrival]]:
    """Parse slog JSON lines into publishes and arrivals; ignore other lines."""
    pubs: Publishes = {}
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
                pubs[(ev["slot"], ev["origin"])] = ev["t_ns"]
            case "arrival":
                arrs.append((ev["node"], ev["slot"], ev["origin"], ev["t_ns"]))
    return pubs, arrs


@dataclass
class Result:
    arrivals: int
    expected: int
    missing: list[tuple[int, int, int]] = field(default_factory=list)
    duplicates: list[tuple[int, int, int]] = field(default_factory=list)
    delays_ms: list[float] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing and not self.duplicates and self.arrivals == self.expected


def analyze(pubs: Publishes, arrs: list[Arrival], node_nums: set[int]) -> Result:
    """Cross-check arrivals against publishes: every node != origin should
    receive each published block exactly once."""
    counts = Counter((node, slot, origin) for node, slot, origin, _ in arrs)
    received = set(counts)
    duplicates = sorted(k for k, c in counts.items() if c > 1)

    missing: list[tuple[int, int, int]] = []
    for slot, origin in pubs:
        for node in node_nums:
            if node != origin and (node, slot, origin) not in received:
                missing.append((node, slot, origin))
    missing.sort()

    delays_ms = sorted(
        (t_ns - pubs[(slot, origin)]) / 1e6
        for node, slot, origin, t_ns in arrs
        if (slot, origin) in pubs
    )
    expected = len(pubs) * (len(node_nums) - 1)
    return Result(len(arrs), expected, missing, duplicates, delays_ms)


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile (matches the Go metrics package)."""
    if not values:
        return 0.0
    s = sorted(values)
    rank = max(math.ceil(p / 100 * len(s)), 1)
    return s[rank - 1]


def load_run(run_dir: Path) -> tuple[Publishes, list[Arrival], set[int]]:
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

    print(f"nodes: {len(node_nums)}  blocks published: {len(pubs)}")
    print(f"arrivals: {res.arrivals} (expected {res.expected})")
    print(f"missing: {len(res.missing)}  duplicates: {len(res.duplicates)}")
    if res.missing:
        print("  missing (node,slot,origin):", res.missing[:20], "..." if len(res.missing) > 20 else "")
    if res.duplicates:
        print("  duplicates (node,slot,origin):", res.duplicates[:20])
    if res.delays_ms:
        print(
            "arrival-delay CDF (ms): "
            f"p50={percentile(res.delays_ms, 50):.1f} "
            f"p90={percentile(res.delays_ms, 90):.1f} "
            f"p99={percentile(res.delays_ms, 99):.1f} "
            f"p100={percentile(res.delays_ms, 100):.1f}"
        )
    print("RESULT:", "OK" if res.ok else "FAIL")
    return 0 if res.ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
