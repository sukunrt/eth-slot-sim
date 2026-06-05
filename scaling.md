# Scaling Model — simulating slot dissemination below mainnet size

Companion to `slot-messages.md`. Same notation (`N` nodes, `V` validators, `C`, `s_c`,
`B`, `D≈8`).

**Goal:** dissemination-time CDFs (per message type) for **`N≈10⁴` nodes, `V≈10⁶`
validators**. We can't run that. **Method (chosen):** integrated full-slot sims at a few
*small* `N`, then compose up to mainnet by **summing per-hop delays to the required depth**.

---

## 0. Why a small sim can speak for a big one

Dissemination time of one message along the mesh is a **sum over hops**:

```
T_arrival(node) = Σ_{h=1..depth(node)} X_h
X (one hop) = link_latency + transmission(size / bandwidth) + processing(validate + queue)
```

Two factors, with completely different `N`-scaling:

| Factor                    | Scales with `N` as            | Notes                                              |
|---------------------------|-------------------------------|----------------------------------------------------|
| **Depth** `H(N)`          | `log_{D-1}(N)` → `log₇(N)`    | hops to cover the graph                             |
| **Per-hop delay** `X`     | **constant** (N-independent)  | *iff* per-node conditions are held fixed (§3)       |

The mesh is **locally identical at any `N`** — every node keeps ~`D` peers whether the
network is 1k or 10k. So `X` measured in a small run is the same `X` mainnet nodes see;
only the *number* of hops grows. Mainnet time = the small-run per-hop delay, summed
`H(N_target)` times.

`H(N) = log₇(N)`:

| `N`    | 250  | 500  | 1k   | 2k   | 5k   | **10k** | 20k  | 50k  |
|--------|------|------|------|------|------|---------|------|------|
| `H(N)` | 2.84 | 3.19 | 3.55 | 3.91 | 4.38 | **4.73**| 5.09 | 5.56 |

So **1k→10k is +1.18 hops; 2k→10k is +0.83 hops.** `N` is the cheap axis — it enters
logarithmically. (Use `H` as the *functional form*; take the actual slope from the sweep,
§2 — real gossipsub runs deeper than the ideal tree because of mesh overlap and tails.)

---

## 1. Decouple **workload** from **node count**

Messages are synthetic blobs, so the message *workload* need not be self-consistent with
`N`. Drive every run with the **mainnet per-slot message set** (counts + sizes + schedule
from `slot-messages.md`), independent of how many nodes you run:

| Workload knob (hold at mainnet, fixed across the whole sweep) | Value                |
|--------------------------------------------------------------|----------------------|
| block size                                                   | set explicitly       |
| aggregates (global)                                          | 1024                 |
| sync contributions (global)                                  | 64                   |
| columns × size                                               | 128 × `(B·2144+356)` |
| attestations per subnet `s_c`                                | `V/2048` (≈512)      |
| sync msgs per subnet                                         | 128                  |

**Do not derive these from `V` via committee math** — at small `V` the committee count
`C` collapses (`C=min(64, V/4096)`), silently shrinking the aggregate count and attestation
volume. Inject the counts directly; vary **only `N`** across the sweep.

Why this keeps `X` constant: a node's **receive + processing** load on a topic is
`(distinct msgs) × (≈D dups)` — set by the workload, *not* by `N`. Hold the workload fixed
→ per-node load is identical at `N=500` and `N=10k` → `X` is N-stable → the sum is valid.
(Per-node *send* load is `workload/N`, higher at small `N`, but it's a handful of tiny
self-originated messages — negligible bytes.)

---

## 2. The method: "few small Ns, then sum"

1. **Sweep.** Run integrated full-slot sims at `N = 250, 500, 1k, 2k, …` up to the Shadow
   ceiling, workload fixed (§1), everything in §3 fixed.
2. **Validate stationarity.** Overlay the per-hop delay distribution across the sweep — it
   must be flat in `N`. Equivalently, `T_p` vs `H(N)` must be a **straight line**. If it
   isn't, an uncontrolled variable is leaking (usual culprit: subnet degeneracy, §5) — fix
   before extrapolating.
3. **Measure the building block.** Per message type, record the per-hop delay
   *distribution* (not just the mean) including the upper tail.
4. **Compose to target.** Two equivalent routes:
   - **Linear fit:** `T_p(N) = α_p + β_p · H(N)` per percentile `p`; extend to `H(10⁴)`.
   - **Convolution:** arrival at depth `d` is the `d`-fold convolution of `X`; mix over the
     depth distribution for `N_target` to get the full CDF. Use this when you need the
     tail/CDF shape, not just `p50/p90/p99`.

Do this **per message type and per graph** — block & aggregates on the global graph
(`n=N`), attestations/columns/sync on their subnet graph (`n=n_subnet`, §5). Each has its
own `X` (sizes/processing differ) and its own depth.

---

## 3. Invariants — fix these or the sum is invalid

If any of these changes with `N`, `X` stops being N-stable and step-2 stationarity breaks:

- **GossipSub:** `D`, `D_low`, `D_high`, `D_lazy`, heartbeat, mcache, flood-publish, and
  **`IDONTWANT`** (gossipsub ≥1.2 — dominates duplicate suppression for block/columns; a
  wrong setting here moves block timing by hops).
- **Message sizes** (per `slot-messages.md`).
- **Per-link latency distribution** — a geographic RTT model; this is most of `X` for small
  messages. Same distribution at every `N`.
- **Per-node bandwidth distribution** — incl. the heavy validator-operator skew (big nodes =
  fatter pipes + more injection); same distribution at every `N`.
- **Per-node subscription counts** — 2 backbone attestation subnets + duty; ≥4 custody
  column subnets.
- **Processing-delay model** (§6) — the "process x seconds, then forward" delay.
- **Workload counts** (§1).

---

## 4. Coupling — the payoff of integrated runs

A node has **one uplink shared across all its topics**, and load is bursty at three
instants: **t=0** (block + custody columns), **t≈4s** (attestation flood), **t≈8s**
(aggregate flood). Per-topic sims miss this contention; integrated runs capture it natively
— the reason for choosing the integrated approach.

Crucially, coupling intensity is a function of **per-node load = workload**, which we hold
at mainnet across the sweep. So an integrated run at `N=1k` already has *mainnet-strength*
contention at each peak — only the depth is smaller. That's exactly why integrated +
fixed-workload extrapolates: the hard part (contention) is right at every `N`; the cheap
part (depth) is what we extend.

---

## 5. Where it's weak: subnet geometry at small `N`

- **Global topics (block, aggregates, sync contributions):** graph = full `N`. Depth
  extrapolation is exact. **Strong.**
- **Subnet topics (attestation / sync / column subnets):** a subnet holds only
  `n_subnet ≈ N·(subs/64) + duty_subscribers` nodes. At small `N` the subnet degenerates:
  with `s_c=512` attesters forced onto few nodes, you get a handful of fat **injection
  points** instead of ~1–2 attesters/node → propagation looks artificially fast → `X`
  distorted (and step-2 stationarity bends).

**Mitigations (pick per topic):**
- **Floor `N_min`** so subnets stay realistic. Mainnet `n_subnet` is a few hundred; you
  need `N_min` large enough that (a) backbone `N·2/64` and (b) the `s_c` duty-subscribers
  land on *distinct* nodes — practically `N_min ≳ s_c ≈ 1–2k`.
- **Escape hatch:** run subnet topics as separate **full-size per-topic** sims (~few-hundred
  nodes = one whole subnet, no extrapolation) and only extrapolate the global topics. This
  is the one spot where the integrated method borrows the per-topic trick.

Decide this from the sweep: if the attestation `T` vs `H(N)` line is straight down to your
`N_min`, the integrated subnets are fine; if it bends at the low end, switch attestations to
the per-topic escape hatch.

---

## 6. Processing delay (no crypto) — the dominant tunable

With crypto stripped, the per-message validation delay is *synthetic* and sits **on the
critical path at every hop** — get it wrong and every CDF shifts. Model per message type as:

```
service_time = fixed_validate + queue
```

- `fixed_validate`: stand-in for sig-verify + checks; calibrate to real client numbers.
- `queue`: messages arriving in a burst wait behind each other (attestation flood, aggregate
  flood). Model as a single-server queue per node (≈ M/D/1) fed by the ingress rate — this
  is where high `s_c` actually bites, as *CPU* contention rather than bandwidth.

This is the main knob you'll fit during mainnet calibration (§7).

---

## 7. Calibration & validation loop (Fulu → mainnet)

Free primitives in `X`: latency geo-distribution, bandwidth profiles, `fixed_validate`.
Fit them so the **composed Fulu CDFs match your beaconprobe / mainnet arrival CDFs**
(block-arrival and attestation-arrival are the load-bearing ones). Lock the primitives once
matched — *then* the model is trustworthy for counterfactuals.

```
fit primitives ─▶ compose Fulu CDFs ─▶ compare to mainnet (beaconprobe) ─▶ adjust ─▶ lock
                                                                                      │
                                                  sweep counterfactuals ◀─────────────┘
```

This is your stated plan: **Fulu first, compare with mainnet**, then explore.

---

## 8. What to record each run (so composition is possible)

Per message, per node: **receive time** (relative to publish), **hop count** (carry a depth
tag / count, or reconstruct from the relay tree), **ingress/egress bytes**, **queue depth at
arrival**. Derived: per-hop delay `X` (differences along the dissemination tree); arrival
CDF; `p50 / p90 / p99 / p100`. Slot-timing decisions come off `p99–p100`, **not the mean**
— the last 1–5% of nodes set the safe deadline.

---

## 9. Study knobs (vary *after* calibration is locked)

- **Slot timing** — `SLOT_DURATION_MS` and the BPS deadlines (the headline question).
- **`B`** — blob/column count → column size/volume (the rising-blob roadmap).
- **`N`** — extrapolate further as the network grows.
- **Fork** — Fulu → Gloas (ePBS): split block into bid + payload envelope + PTC votes, and
  the earlier GLOAS deadlines (see `slot-messages.md §8`). New message shapes, same machinery.
