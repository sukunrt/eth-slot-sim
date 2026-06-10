# Skewed validator distribution — design spec

## 1. Motivation

Today validator v lives on node `v % N` — every node hosts ⌈V/N⌉ validators. Real Ethereum
is nothing like this: large operators run thousands of keys per node, solo stakers run a
handful. Skew changes per-node *message multiplicity* everywhere a duty is per-validator:
attestation committees, AC votes, and above all the finality-attestation burst (a
5 000-validator node emits 5 000 votes at `fc_vote_offset` in one instant). The phases are
otherwise indifferent: decoupled and non-decoupled both consume the same V→N map, so one
seam serves both.

## 2. Where uniform is baked in (current state)

| Site | What it does |
|---|---|
| `simctl/schedule.py:350` | committee members: `AttesterRef(node=v % p.n, val=v, ...)` |
| `simctl/schedule.py:376` | AC voters: `AttesterRef(node=v % p.n, ...)` |
| `simctl/schedule.py:327` (`_validators_per_subnet`) | closed-form hosted-count per node: `(V-1-node)//N + 1` |
| `schedule/schedule.go:309` (`FinalityVoteDuties`) | Go derives hosting: `val := node; val < V; val += N` |
| `analysis/check_arrivals.py:680` | expected votes per host: `range(host, v, n)` |
| `analysis/check_arrivals.py:715` | csv variant: `host = val % n` |

Everything else already reads explicit (node, val) pairs or node-based membership from
schedule.json — committees, AC voters, finality subnets, aggregators, sync — so skew never
touches them.

## 3. Wiring options — how the map travels

### Option A (recommended): per-node counts in schedule.json, contiguous id ranges

Python draws `validator_counts[node]` (Σ = V) from the configured distribution and writes
it to schedule.json. Validator ids are assigned contiguously by node: with
`C = cumsum(counts)`, node i hosts validators `[C[i], C[i+1])`.

- `FinalityVoteDuties` becomes a range over `[C[node], C[node+1])`; the analyzer builds the
  same cumsum; `AttesterRef.node` becomes `bisect(C, v)`. `_validators_per_subnet` sums
  counts over members. Four small edits, one new field.
- Cost: N ints in schedule.json (4 000, not 400 000) — matters because every Shadow host
  parses schedule.json at startup.
- Validator ids are opaque in every message (only multiplicity matters), so re-ordering
  ids by node is observable only by the analyzer — which reads the same field.
- Absent field ⇒ uniform `v % N`: old schedules and the simnet tests keep working
  unchanged; the Go loader defaults the cumsum to the uniform one.

### Option B: full explicit map `validator_node[v]` in schedule.json

Maximally general (arbitrary interleavings, future per-validator attributes like stake
weight). Rejected for now: V ints (≈2.5 MB at V=400k) parsed by each of 4 000 Shadow
processes, and no current experiment needs more than counts. Option A's schema doesn't
preclude adding this later (a `validator_node` field would simply override).

### Option C: parametric derivation in both backends (no schedule change)

Config carries (distribution, seed); Python and Go each derive the counts with identical
PRNGs. Rejected: violates the repo's one-plan principle (membership is drawn once, in
Python; both backends read the same schedule.json), and cross-language PRNG parity is a
standing footgun.

## 4. Distribution menu — how counts are drawn (Python only)

A `validator_distribution:` block on SimConfig (top-level; it is a property of the fleet,
not of one phase):

```yaml
validator_distribution:
  type: uniform | tiered | explicit   # default uniform = status quo
  # tiered (the realistic one): tier by the node class the topology already has
  regular: {min: 1, max: 3}               # uniform over {1,2,3}
  super: {min: 1, max: 1000, mean: 200}   # heavy-tailed, calibrated to the mean (below)
  # explicit: counts file or inline list (escape hatch for experiments)
  counts: [...]
  seed: 7                             # independent of topology seed
```

- **uniform** — status quo, emits the uniform counts explicitly (one code path).
- **tiered** — the realistic shape, keyed off `supernode_ids(...)` (no new node classes):
  regular nodes draw uniformly from `[regular.min, regular.max]` (solo stakers, 1–3 keys);
  supernodes draw from a heavy-tailed distribution on `[super.min, super.max]` with the
  given mean — log-normal with σ as an implementation default, μ solved numerically so the
  truncated mean hits `super.mean`, rejection-sampled into range. At
  `super_node_fraction: 0.5` and the defaults above, E[V] ≈ (2 + 200)/2 · N ≈ 100N.
  **V is emergent in this mode**: schedule.json's `params.v` := Σ counts, and
  `attestation.validators` is ignored (warn when set and different). Go and the analyzer
  already read V from schedule.json, so nothing downstream changes.
- **explicit** — hand-written counts for targeted experiments (V := Σ counts here too).

## 5. Touchpoints (the whole diff)

- `simctl/config.py`: the block above + validation (§6).
- `simctl/schedule.py`: draw counts, emit `validator_counts` in schedule.json; replace the
  three `v % p.n` sites with the cumsum lookup.
- `schedule/schedule.go`: load `validator_counts` (default uniform), precompute cumsum;
  `FinalityVoteDuties` reads its range. (`AttestDuties`/`ACVoteDuties` need nothing — they
  already read explicit refs.)
- `analysis/check_arrivals.py`: read `validator_counts` from schedule.json; replace the two
  derivation sites. CSV path identical.
- Tests: pytest generator invariants (Σ=V, determinism, tier shares, floor); a Go/Python
  round-trip pinning `FinalityVoteDuties(node) == [C[node], C[node+1])`; one driver
  full-run e2e with mixture skew (the identity tests in metrics/node are unaffected —
  kinds and MsgID encoding don't change).

## 6. Constraints and interactions

- **V := Σ counts** in tiered/explicit modes (uniform keeps the configured V). Counts ≥ 1
  everywhere (`regular.min`/`super.min` ≥ 1, validated): data-columns custody and the
  "every node validates ⇒ uniform custody" assumption stay intact, and `fs` membership
  semantics (every node votes on its subnet) keep a non-empty duty everywhere. Count 0 is
  allowed only in explicit mode with data_columns off (validated).
- **Queue sizing**: the finality burst from a node is `counts[node]` messages at one
  instant — up to `super.max` (1 000) from a single supernode, under the 4 096 outbound
  queue but 10× today's uniform 100. The spec deliberately does NOT auto-scale queues —
  overflow under skew is a *finding*, not a bug. Surface `counts.max` in the run banner
  so it is visible per run.
- **VRF/committee realism falls out for free**: AC voters and committee members are drawn
  uniformly over validator ids, so a node's duty probability becomes proportional to its
  hosted count — exactly the real-world behavior — with no further changes.
- **vps / aggregate size**: `validators_per_subnet` (and so `FinalityAggregateSize`)
  varies across subnets with skew; formula unchanged, only inputs.
- **Proposer selection (open, out of scope)**: proposers cycle over supernodes today; in
  reality proposer probability ∝ hosted validators. With `on_supernodes: true` the two are
  roughly consistent; weighting the proposer draw by counts is a separate follow-up.

## 7. Naming

Glossary additions (improvements.md): "validator distribution" / `validator_counts`
(schedule.json) / `ValidatorCounts` + cumsum (Go). The old prose alias is "the Dist seam"
(`schedule.go:308`).
