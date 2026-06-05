# Scaling Model — extrapolating slot dissemination from small node counts

Companion to `slot-messages.md`. Same notation (`N` nodes, `V` validators, `C` committees/
slot, `s_c` committee size, `B` blobs, `D≈8` mesh degree).

**Goal:** dissemination-time CDFs (per message type) at **`V=10⁶` validators, `N=10⁴`
nodes**. We can't run 10k real nodes.

**Method:** run a **sweep of small `N`** and extrapolate. Dissemination is a sum over hops,
so the trick is to measure the **per-hop delay** (which is N-stable) from small runs and
**sum it over the depth `H(N)` that 10k nodes require**.

`V` is a **free input**, not a fixed choice — see §4. So is the validator→node distribution
(§5). The harness takes `(N, V, distribution)` and derives everything else; the scaling
*schedule* is decided at experiment time, not baked into the code.

---

## 0. Decomposition: depth vs. per-hop delay

```
T_arrival(node) = Σ_{h=1..depth(node)} X_h ,   X = link_latency + transmission(size/bw) + processing
```

| Factor              | Scales with `N` as           | Notes                                  |
|---------------------|------------------------------|----------------------------------------|
| **Depth** `H(N)`    | `log_{D-1}(N)` → `log₇(N)`   | hops to cover the graph                 |
| **Per-hop** `X`     | ~constant                    | the mesh is locally identical at any `N`|

The mesh is locally identical at any `N` (every node keeps ~`D` peers), so `X` measured in a
small run is what mainnet nodes see — only the *number* of hops grows. Mainnet time = the
small-run per-hop delay summed `H(N_target)` times.

`H(N) = log₇(N)`:

| `N`    | 100  | 250  | 500  | 1k   | 2k   | 5k   | **10k** |
|--------|------|------|------|------|------|------|---------|
| `H(N)` | 2.37 | 2.84 | 3.19 | 3.55 | 3.91 | 4.38 | **4.73**|

1k→10k is **+1.18 hops**; 2k→10k is **+0.83**. Use `H` as the functional form; take the
actual slope from the sweep (real gossipsub runs deeper than the ideal tree — overlap, tails).

Compose two ways: **linear fit** `T_p = α_p + β_p·H(N)` per percentile, extended to `H(10⁴)`;
or **convolution** of the per-hop `X` over the depth distribution when you need the full CDF
tail rather than just `p50/p90/p99`.

---

## 1. Subnet structure (the part that bit us)

**The number of subnets is fixed**: `ATTESTATION_SUBNET_COUNT = 64`,
`DATA_COLUMN_SIDECAR_SUBNET_COUNT = 128`. Independent of `N` and `V`. What varies per run is
**how many of them a node joins (meshes on / relays + receives)** — and the two domains
behave oppositely:

**Attestation subnets — membership ≈ 2, independent of validator count.**
- **Backbone:** `SUBNETS_PER_NODE = 2` long-lived subnets from node-id, stable 256 epochs.
- **Aggregator duty:** a validator selected as aggregator (`is_aggregator()`, ~16/committee)
  makes its node join that committee subnet *for the slot*.
- **Publishing ≠ joining:** a plain attester only needs peers to *publish* (fan-out); it does
  **not** join the mesh (`phase0/validator.md`: "does not need to subscribe and listen to all
  messages on the topic"). So joins ≈ **2 + aggregator** ≈ 2 at mainnet.

**Data-column subnets — membership scales with stake (the mirror image).**
```
custody = min( max(stake_on_node // 32ETH, 8), 128 )      # 4 if non-validating
```
≈ one column subnet per 32-ETH validator on the node, floored at 8 (validating), capped at
128 (supernode). Plus `max(8, custody)` columns *sampled* per slot over **req/resp**, not
gossip. So the "each validator pulls in another subnet" intuition is **false for attestations,
true for columns.**

| domain        | total | a node joins                | driven by                  |
|---------------|-------|-----------------------------|----------------------------|
| attestation   | 64    | ~2 (+aggregator)            | node-id backbone; agg duty |
| data column   | 128   | 4 → 128                     | stake on the node          |

**Per-node load consequences:**
- Attestation **receive ≈ `2 · s_c`** — set by committee size, *not* by `V/N`.
- Column **receive ≈ `custody · (B·2144+356)`** — set by stake-on-node, i.e. by the
  validator **distribution** (§5), *not* by `N`.

---

## 2. What's clean, what ramps

| Quantity                                   | vs `N`                | Verdict                              |
|--------------------------------------------|-----------------------|--------------------------------------|
| Block / aggregate spread (global)          | per-node cost N-indep | **Clean** → depth extrapolation      |
| One message's spread *within* its subnet   | depth on `n_subnet`   | **Clean** → depth extrapolation      |
| Per-subnet contention / CPU (attestations) | scales with `s_c`     | ramps with committee size (§4)       |
| Column download at t=0                     | scales with custody   | set by distribution (§5), not `N`    |

Block and aggregates are the headline metric (does the block beat the 4s deadline?) and they
are exactly the clean part.

---

## 3. The sweep, and the stationarity check

1. Sweep integrated full-slot runs at `N = 250, 500, 1k, 2k, …` up to the Shadow ceiling.
2. **Check stationarity:** the per-hop delay must be flat across `N` — equivalently
   `T_p` vs `H(N)` is a straight line. A bend signals an uncontrolled variable (usually the
   per-node load drifting with the sweep policy, §4).
3. Measure the per-hop delay *distribution* (incl. tail) per message type.
4. Extrapolate to `H(10⁴)` (§0).

Treat tiny points (`N`=10, 100) as near-complete-graph endpoints, not curve points.

---

## 4. `V` is a free input — strong vs. weak scaling is an experiment choice

The harness derives everything from `V` and per-node stake — `C = min(64, V/4096)`,
`s_c = (V/32)/C`, aggregate bitfield length, custody, aggregator/backbone subscriptions —
**nothing hardcoded to mainnet**. So one binary runs any `(N, V)` point, and the only
decision is *which points you sweep*:

| | per-node footprint across the sweep | contention bias | cost at small `N` |
|---|---|---|---|
| **Strong** (`V=10⁶` fixed) | over-subscribed (agg subs + custody cap saturate) | **pessimistic** | heavy (full mainnet traffic through few nodes) |
| **Weak** (`V=100·N`) | faithful mainnet node at every `N` (~2 attn subnets, ~100 cols at uniform); `s_c` ramps 128→512 | **optimistic** (per-subnet load only full at the target) | cheap |

Same code either way. **Recommended:** weak (`V=100·N`) as primary for the clean self-similar
trend, plus one or two strong (`V=10⁶`) runs at your top feasible `N` to upper-bound the
contention weak under-shows. Weak gives the optimistic trend, strong the pessimistic bound —
mainnet is between, and you've bracketed it.

**Punt the schedule.** The ceiling isn't something you benchmark up front — there's nothing
to measure until the harness exists, and it's discovered *by* the sweep, not before it: start
at the cheap end (`N`≈250 always fits), climb `N` until Shadow chokes (RAM / wall-clock /
event-rate), and the last `N` that finished is your ceiling. Then weak runs the small-`N`
sweep and strong cross-checks at the top. Build the harness V-parameterized and let the climb
set the schedule.

Fundamental constraint behind all this: **full `s_c=512` + correct fan-out + `N`<10k cannot
coexist** under real committee math. Strong sacrifices fan-out; weak sacrifices `s_c`.

---

## 5. The validator→node distribution — the knob that matters for columns

How you spread `V` over the `N` nodes sets per-node stake, which sets custody:

- **Uniform** `V/N` (e.g. 100/node) → every node custodies ~100 of 128 columns → each pulls
  ~2 MB of column data at t=0 (B=9). Heavy, barely sharded.
- **Realistic skew** (most home stakers 1–5 validators → custody 8; a few big operators →
  128) → median node pulls ~8 columns (~160 KB); the load concentrates on big nodes.

That's a **~10× swing on median t=0 column load** — far bigger than anything `N` does.
Attestations are insensitive (~2 either way). So: expose the distribution as a knob; for
columns, the skew *is* the model. (Big nodes also have fatter pipes — couple bandwidth to
the distribution.)

---

## 6. Invariants — hold fixed across the sweep

(`V` and the distribution are *inputs*, not invariants; everything below stays at mainnet so
the per-hop `X` only moves with depth.)

- **GossipSub:** `D=8`, `D_low=6`, `D_high`, `D_lazy`, heartbeat, mcache, flood-publish, and
  **`IDONTWANT`** (≥v1.2 — dominates duplicate suppression for block/columns).
- **Message sizes** (per `slot-messages.md`).
- **Per-link latency distribution** — geographic RTT model; most of `X` for small messages.
- **Per-node bandwidth distribution** — incl. operator skew, coupled to §5.
- **Processing-delay model** (§7).

---

## 7. Processing delay (no crypto) — the dominant tunable

Validation is stripped, so the per-message service delay is synthetic and sits **on the
critical path every hop**:

```
service_time = fixed_validate + queue
```

- `fixed_validate`: stand-in for sig-verify + checks; calibrate to real client numbers.
- `queue`: burst arrivals wait single-file (attestation flood at t≈4s, aggregate flood at
  t≈8s) — model as a per-node single-server queue (≈ M/D/1) fed by ingress rate. This is
  where high `s_c` actually bites — as CPU, not bandwidth.

---

## 8. Calibration & validation (Fulu → mainnet)

Free primitives in `X`: latency geo-distribution, bandwidth profiles, `fixed_validate`. Fit
them so the **clean** composed Fulu CDFs (block-arrival above all) match your beaconprobe /
mainnet arrival CDFs, then lock them. Calibrate off the clean metrics, since the contended
ones carry the §4 bias.

```
fit primitives ─▶ compose clean Fulu CDFs ─▶ compare to mainnet (beaconprobe) ─▶ adjust ─▶ lock
                                                                                            │
                                                        sweep counterfactuals ◀─────────────┘
```

Your plan: **Fulu first, compare with mainnet**, then explore.

---

## 9. What to record each run

Per message, per node: **receive time** (rel. to publish), **hop count** (depth tag or
relay-tree reconstruction), **ingress/egress bytes**, **queue depth at arrival**. Derived:
per-hop `X`; arrival CDF; `p50 / p90 / p99 / p100`. Slot-timing decisions come off
**`p99–p100`**, not the mean — the last 1–5% of nodes set the safe deadline.

---

## 10. Study knobs (vary *after* calibration is locked)

- **Slot timing** — `SLOT_DURATION_MS` and the BPS deadlines (the headline question).
- **`B`** — blob/column count → column size/volume (the rising-blob roadmap).
- **`V` / distribution** — validator-set size and concentration.
- **`N`** — extrapolate further as the network grows.
- **Fork** — Fulu → Gloas (ePBS): block splits into bid + payload envelope + PTC votes, with
  earlier GLOAS deadlines (`slot-messages.md §8`). New message shapes, same machinery.
