# Decoupled-consensus run ladder — results (2026-06-10)

Shadow runs on `sukun@ethp2p`, one rung per network size, all on the same honest model
(`configs/decoupled_n*.yaml`): tiered validator skew (V emergent ≈ 100·N), 50% supernodes
(1024 Mbit; regulars 25 Mbit), 128 columns / custody floor 8, k=10 AC slots per finality
slot, fs_subnets = N/100 so every rung carries **~10k finality attestations per subnet**,
offset 2 s + jitter 0.5 s, 20 slots = 2 finality slots. Verification model: 300-item
batches, 10 ms window, 10 ms base + 10 µs/item (≈23k att/s when backlogged); own publishes
skip verification. All four gossipsub inbound limits sit above modeled backlog (see
"model fixes"). Full per-slot CDFs, percentile grids and loss structure per rung:
`results/ladder/<rung>.analysis.json` (the durable artifact of `analysis/check_arrivals.py`).

## Finality attestations (the flood under test)

| | n500 | n1000 | n2000 |
|---|---|---|---|
| V (emergent) | 46 400 | 102 035 | 201 032 |
| votes/subnet/fslot | ~9.3k | ~10.2k | ~10.1k |
| delivery | 100% | 100% − 2 msgs | 99.964% (16 565 missing) |
| publisher drops | 0 | 0 | **55** |
| p50 | 389 ms | 420 ms | 412 ms |
| p90 | 1.09 s | 1.27 s | 1.35 s |
| p99 | 2.44 s | 3.05 s | 3.56 s |
| p100 | 6.4 s | 11.6 s | **65.8 s** |

Per-subnet load is constant by design, so the deltas are pure network-size effects:
medians flat, p99 grows gently (~+0.5 s per doubling), only the extreme tail stretches.
Aggregation deadline is 60 s into the finality slot — p99 clears it 17×; at n2000 the
p100 sliver (~0.04%) crosses it for the first time.

The tail belongs to the home nodes: at n1000, the slowest 1% of arrivals are 99.6%
25 Mbit receivers (baseline mix 45%) — the supernode core absorbs the flood in ~1–3 s,
the extreme tail is 25 Mbit last-hop delivery.

## Other message families (p99, all 100% delivered on every rung)

| kind | n500 | n1000 | n2000 |
|---|---|---|---|
| blocks (128 KiB) | 938 ms | 1.05 s | 1.39 s |
| data columns | 333 ms | 347 ms | 358 ms |
| AC votes (512/slot) | 451 ms | 491 ms | 532 ms |
| finality aggregates | 359 ms | 480 ms | 871 ms |

`fraction voted block: 1.000` everywhere; proposer guard OK. Boundary-slot contention
(AC slots 0/10, where the vote burst lands on blocks+columns+AC votes) is real but mild:
e.g. n2000 blocks p50 1.0 s on slot 0 vs ~520 ms quiet; AC-vote p99 847 ms vs ~530 ms.
AC-vote deadline (4 s) has ≥4× margin on every rung.

## Model fixes the ladder forced (all committed + pinned by tests)

1. **Own publishes bypass the verify hook** — gossipsub validates local publishes, so a
   multi-key host's burst serialized at one vote per batch window (738-key host ⇒ 37 s
   tail). `TestPublishBurstNotSelfVerified`.
2. **Four stacked pubsub inbound limits raised above modeled backlog** — validate queue
   20k, global throttle 50k, per-topic validator concurrency 50k, outbound 10k. Each
   drops silently (debug-only logs) and `markSeen` runs before validation, so every drop
   is permanent. The per-topic 1024 cap caused 15% loss at multi-subnet supernodes
   (regulars 0%) in the first n500 runs, invariant to the other knobs.
   `TestFloodBeyondTopicValidatorConcurrency` (exactly 1025/1500 delivered pre-fix).

## Open items

- **n2000's two new signals** (both ~0.04% of votes, expected to grow at n4000):
  55 publisher-side drops at multi-key hosts (outbound-queue pressure during the fan-out
  burst — per-peer queue 10k is the one limit still near modeled backlog), and the
  >60 s straggler sliver. With 16 aggregators/subnet neither should affect aggregation.
- n4000 rung pending.
- Sensitivity rung pending: `super_node_fraction: 0.2` at n500 — 50% supernodes is
  generous, and the tail/backbone both lean on it.
- Earlier rungs (n50, n100) ran pre-fix; their numbers are not comparable and are
  superseded by this table.
