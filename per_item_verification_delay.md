# Task: per-item verification cost for the flood classes (+ the honest n4000 rerun)

## Why (context from the 2026-06-10 session)

The decoupled-consensus realism runs look *too good* (n4000: finality-attestation p99 ≈ 6 s,
boundary-slot contention ~10%). The single biggest modeled-away cost is **verification CPU**:
every flood message is forwarded the instant it arrives because the batched verifier's
per-item cost is 0 — a node "verifies" a batch of 10 000 finality attestations in one
`verify_delay_ms` (10 ms) sleep per 50 ms window. Reality: ~0.5–1 ms per BLS signature even
batched ⇒ 5–10 s of CPU per subnet member per finality slot, sitting right on top of the
gossip tail. Gossipsub validates BEFORE forwarding, so the cost compounds per hop.

## Current state (verified in code)

- `node/node.go` `batchVerifierFor`: every flood class — `consensus` (attestations +
  aggregates + sync), `ac` (AC votes), `fcvote` (finality attestations), `fcagg` (finality
  aggregates) — gets its **own** single-server batched queue, but **all share the same
  knobs**: base = `AttestVerifyDelay` (→ `attestation.verify_delay_ms`), per-item =
  `AttestPerItem` (→ `attestation.per_item_ms`, default 0), window = `AttestBatchWindow`
  (→ `attestation.batch_window_ms`, default 50).
- Wiring exists end-to-end: `attestation.per_item_ms` → `-attest-per-item` flag
  (`simctl/runner.py:177`, `cmd/slot-sim-node/main.go:91`) → `newBatchVerifier`. **Setting
  `per_item_ms: 1` in the config is enough — no code needed** unless per-class costs are
  wanted.
- Blocks use a fixed per-hop sleep (`verify_delay_ms`); columns a width-P semaphore
  (`verify_service_ms`, `verify_parallelism_*`). Those are already non-zero.

## The task

1. Decide: shared knob (config-only) vs per-class `per_item_ms` (small code change in
   `node/node.go` + flags + config). Shared at 1 ms is a fine first cut — AC votes and
   finality attestations are both ~1 BLS verify; aggregates are worth more like 2–3 ms
   (n pairings) but there are only 1 280 of them.
2. Set `per_item_ms: 1` (in the `attestation:` block) in `configs/decoupled_n4000.yaml`.
3. Rerun n4000 on `sukun@ethp2p` with ALL the realism fixes now in the config/repo:
   - 128 columns / custody floor 8 (committed — sparse column subnets, real backbone),
   - `offset_ms: 2000` + `jitter_ms: 500` (committed),
   - **tiered validator skew** (implemented + tested this session; add
     `validator_distribution: {type: tiered}` to the config and drop
     `attestation.validators` — V is emergent ≈ 100N ≈ 400k),
   - per_item_ms 1 (this task).
   `uv run simctl run --config configs/decoupled_n4000.yaml --remote sukun@ethp2p
   --output-dir runs/decoupled-n4000-honest`
4. Compare against the easy run (numbers below): finality-attestation tail, boundary-slot
   (AC slots 0 and 10) block/AC-vote CDFs, `fraction voted block`, loss structure. Watch
   the verify queue: a single-server queue at 1 ms/item with ~10k items/subnet member means
   ~10 s of queue drain — if the tail explodes past the aggregation deadline, that is a
   real finding about ac_slots_per_finality_slot (k=6 was looking comfortable).

## Baseline (the "easy" n4000 run, 2026-06-10, 8 columns, no skew, per_item 0)

- blocks p50/p99/p100: 509/1170/1866 ms · AC votes: 426/641/2191 ms (100%, voted 1.000)
- finality attestations: p50 3.11 s, p90 5.01 s, p99 5.94 s, p100 10.8 s; 99.92%
  delivered, all loss on 39 slow receivers, zero publisher-side drops
- finality aggregates (1 280 × ~1.6 KB): 100%, p50 392 ms
- boundary-slot contention (slot 10 vs quiet): ~8–12% on blocks/AC votes
- remote artifacts: `~/eth-slot-sim/runs/decoupled-n4000.tar.gz` + `runs/n4000-analysis/result.txt`

## Gotchas

- Analysis at this size: don't scp + extract locally (/tmp is a 31 G tmpfs and the
  extraction is ~20 GB) — extract + `uv run python analysis/check_arrivals.py` on the
  remote (fish shell: no heredocs; scp helper scripts to /tmp and run them).
- Skewed runs put up to 1 000 finality attestations on one node's burst (outbound queue is
  4096 now; overflow under skew is a finding, not a bug — see skewed-validators-spec.md §6).
- `attestation.verify_delay_ms` (the batch base) also exists — leave at 10 ms; only
  per-item changes.
