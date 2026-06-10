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

| | n500 | n1000 | n2000 | n4000 |
|---|---|---|---|---|
| V (emergent) | 46 400 | 102 035 | 201 032 | 409 785 |
| votes/subnet/fslot | ~9.3k | ~10.2k | ~10.1k | ~10.2k |
| delivery | 100% | 100% − 2 msgs | 99.964% | **98.69%** (1.24M missing) |
| publisher drops | 0 | 0 | 55 | **709** (10k outbound knob) |
| p50 | 389 ms | 420 ms | 412 ms | 331 ms |
| p90 | 1.09 s | 1.27 s | 1.35 s | 1.53 s |
| p99 | 2.44 s | 3.05 s | 3.56 s | **12.3 s** |
| p100 | 6.4 s | 11.6 s | **65.8 s** | **77.8 s** |

Per-subnet load is constant by design, so the deltas are pure network-size effects:
medians flat through n4000, p99 gentle (~+0.5 s per doubling) through n2000 — then a
regime change at n4000 (p99 12.3 s, 1.3% loss). Aggregation deadline is 60 s into the
finality slot — p99 clears it on every rung; the p100 sliver crosses it from n2000 on.

**n4000 is where the network strains**: `fraction voted block` drops to **0.850** (every
other rung: 1.000) — 15% of AC voters missed the block+DA gate by the 4 s deadline,
because boundary-slot block delivery collapses under the ~410k-vote burst (slot-0 block
p99 10.7 s vs ~0.7 s quiet; AC-vote p100 45.6 s, block p100 42.8 s). Medians stay
sub-second everywhere — the supernode core absorbs the flood; the 25 Mbit home-node edge
is what saturates. Publisher drops grew 13× on the unchanged 10k outbound knob (run
launched before the 20k bump).

The tail belongs to the home nodes: at n1000, the slowest 1% of arrivals are 99.6%
25 Mbit receivers (baseline mix 45%) — the supernode core absorbs the flood in ~1–3 s,
the extreme tail is 25 Mbit last-hop delivery.

## Other message families (p99, all 100% delivered on every rung)

| kind | n500 | n1000 | n2000 | n4000 |
|---|---|---|---|---|
| blocks (128 KiB) | 938 ms | 1.05 s | 1.39 s | 1.34 s |
| data columns | 333 ms | 347 ms | 358 ms | 373 ms |
| AC votes (512/slot) | 451 ms | 491 ms | 532 ms | 556 ms |
| finality aggregates | 359 ms | 480 ms | 871 ms | 2.25 s |

`fraction voted block: 1.000` through n2000 (n4000: 0.850 — see above); proposer guard
OK everywhere. Boundary-slot contention (AC slots 0/10, where the vote burst lands on
blocks+columns+AC votes) is mild through n2000 (e.g. n2000 blocks p50 1.0 s on slot 0 vs
~520 ms quiet) and severe at n4000 (slot-0 block p99 10.7 s). AC-vote deadline (4 s) has
≥4× margin on p99 at every rung; at n4000 the boundary-slot tail blows through it.

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

## Validator segregation at n4000 (the fix for the boundary collapse)

`configs/decoupled_n4000_seg.yaml` = the base rung + `validator_segregation: true`
(k=10 per-AC-slot finality rounds: round s % k votes in slot s, ~41k votes/slot spread
over 40 subnets ≈ 1k/subnet/slot, round aggregates at 67% of the AC slot). Run on the
20k outbound queue (base ran on 10k — minor confound for the drop comparison).
Artifact: `results/ladder/n4000-seg.analysis.json`.

| n4000 | base | segregated |
|---|---|---|
| fraction voted block | 0.850 | **1.000** |
| slot-0 block p99 | 10.7 s | **1.43 s** |
| finality attestations p50 / p90 / p99 | 331 ms / 1.53 s / 12.3 s | 178 ms / 280 ms / **489 ms** |
| finality attestations p100 | 77.8 s | 11.8 s |
| FA loss | 1.31% | **0.021%** |
| publisher drops | 709 | 12 |
| vote coverage @ aggregation deadline | — | ≥0.980 every slot×subnet (1.000 nearly everywhere after slot 0) |

The boundary collapse disappears: spreading the burst restores the AC gate (voted
1.000), block delivery is healthy on every slot, and each round's votes reach their
aggregators with margin inside the per-round deadline (~8 s at 67%). The first round
(slot 0, mesh still warming + block+columns) is the only soft spot: a few subnets at
0.98–0.997 coverage.

Why segregation and not transport priorities: the base-n4000 collapse is a priority
inversion (one 128 KiB block queued behind ~410k 226 B votes in FIFO per-peer queues at
the 25 Mbit edge). Topic/stream priorities would protect the block path without
segregation, but gossipsub multiplexes all topics FIFO over one stream per peer — no QoS
lever exists today, so segregation is the practical fix. It also buys what priorities
would not: publisher-side relief (a ~1000-key host emits ~100 votes/slot instead of
~1000 at one instant) and smooth aggregator load (1k/subnet/slot vs 10k spikes).

Remaining trace-level items: 0.02% missing (scattered), 12 publisher drops, and 7 023
"leaked" arrivals all at ONE cell (node 58, slot 17, subnet 9) — an aggregator-host
subscribe-window edge (over-delivery into a round it wasn't an expected receiver for);
worth a look at the round subscribe/unsubscribe timing.

## Open items (no further simulations without explicit go-ahead)

- **n4000 findings to dig into**: which mechanism gates the 0.850 voted fraction (block
  late vs columns late at boundary slots); whether the 1.24M missing finality
  attestations share the home-node-edge profile; whether 709 publisher drops vanish on
  the 20k outbound queue (bumped after this run launched — a rerun would isolate it).
- Candidate mitigations for the n4000 regime: vote-burst jitter across the finality
  slot, larger fs_subnets at fixed N (smaller per-subnet bursts), supernode-biased mesh
  for boundary-slot block delivery.
- Sensitivity rung parked: `super_node_fraction: 0.2` at n500 — 50% supernodes is
  generous, and the tail/backbone both lean on it.
- Earlier rungs (n50, n100) ran pre-fix; their numbers are not comparable and are
  superseded by this table.
