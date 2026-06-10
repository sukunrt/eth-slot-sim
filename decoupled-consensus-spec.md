# Decoupled consensus — availability chain + finality chain (simnet/shadow)

Implementation-ready spec. Models a **decoupled-consensus** slot structure: two chains running on
the same node fleet.

- **Availability chain (AC)** — the existing block path (proposer publishes the block + data
  columns at t=0) plus a **global flood** of `ac_vote_size` (512) VRF-selected per-slot validator
  votes on **one global topic** (no subnets, no committees, no aggregation — every node downloads
  all of them). Each vote is block→vote coupled and **gated on data availability**, exactly like
  today's column-gated attestations.
- **Finality chain (FC)** — every `k` AC slots (`AC_SLOTS_PER_FINALITY_SLOT`, default 10) **all**
  validators vote for the finalized tip. Voting is partitioned into `fs_subnets` (40) subnets by
  **validator** (`finality_subnet_of[v]`, one independent uniform draw per validator →
  ~`V/fs_subnets` ≈ 25 000 votes/subnet at mainnet regardless of where keys live; a node publishes
  on its validators' subnets, fanning out where it isn't a member); then `fs_aggregators` (16)
  validators per subnet — sampled from the **entire** set — have their host node publish one
  `FinalityAggregate` (size **scales** with the subnet's voting population) on **one global
  topic** at `FINALITY_SLOT_AGGREGATION_FRACTION` (50 %) of the finality slot.

The whole feature is **opt-in** (`decoupled_consensus.enabled`). When on, the existing
attestation/aggregate **and** sync phases are **disabled entirely** — the AC vote stands in for the
attestation, and the FC vote/aggregate stand in for finality. **Data columns stay on** and gate the
AC vote (the "availability" in availability chain). The goal is the **dissemination + aggregation
time** CDFs: block spread, AC-vote spread, FC per-subnet vote spread + coverage, and the global
FC-aggregate spread — with the AC traffic deliberately **contending** with the FC burst.

This is the most **reuse-heavy** feature yet. The AC vote is "column-gated attestations on one
global topic, no aggregation." The FC vote is "sync-committee membership, but every node is a member
and every validator votes." The FC aggregate is "the attestation aggregate twin, with a
population-scaled bitfield and a previous-slot aggregator pre-join."

> This is a spec, not code. All decisions are locked in §13; §15 lists open risks.

---

## 0. Non-goals (this cut)

Real VRF (the 512 AC voters are a seeded per-slot draw — a stand-in for the selection's *count*, no
key-correlation, as `is_aggregator()` is modeled today); real BLS / SSZ / KZG (validation is
sleep, as everywhere); the on-chain folding of votes into the next block (we stop at the aggregate
gossip); FC **fork-choice content** (the finality vote is **dissemination-only** this cut — fixed-time,
no per-block coupling, no measured "voted X" outcome; we measure *when* it spreads and is aggregated,
not *what* it decides — **content is a planned next step**, §4b); the realistic validator→node
**custody/stake skew** (uniform `V/N` here,
behind a `Dist` seam so skew drops in later — `scaling.md §5`, the `data-column-custody-distribution`
todo); PeerDAS req/resp **sampling** (the AC gate is custody-completeness only, inherited from
data-columns §0); light-client updates. AC-vote **aggregation** is explicitly out (the 512 vote
directly on the global topic).

---

## 1. Reuse vs. new — decoupled consensus is mostly reuse

| Layer | Reuse **as-is** | Needs a **new variant** |
|---|---|---|
| AC block + column burst | `driver/runner.go:publishBlock` + the t=0 column burst (`data-columns-spec §4`) | — (proposer is a full-custody supernode, as columns require) |
| AC vote coupling | `driver/coupling.go:emitDecision` + the **column gate** (`seen && columnsComplete`, `runner.go:tryEarlyEmit`) | AC votes ride it, emit on **one global topic**, **no aggregation** (§3) |
| AC vote filler | `validator/attestation.go:sizedFiller` + `PriorHead` | `MakeACVote` (§7) |
| FC subnet membership | the `sync_subscribers` node-set pattern (`schedule.py`, `schedule/schedule.go`) | every node is a member of **one** subnet; `fs_subscribers` (§4) |
| FC per-subnet topology | the per-subnet spanning-tree loop (`netsim.discv5Graph`, `simctl.generate_subnet_topology`) | a loop over `fs_subscribers` (§5) |
| FC aggregator draw | the per-slot `Aggregators`/`SyncAggregators` draw (`schedule.py`, `schedule.SlotPlan`) | `fc_aggregators[fslot]`, **+ previous-AC-slot pre-join dial** (§4, §6) |
| FC aggregate emit | `driver/runner.go:emitAggregate` / `emitSyncContribution` (fixed-deadline global flood) | `emitFinalityAggregate`, **population-scaled size** (§6, §7) |
| Verify | the per-node **batched verifier** (`node/verifier.go`); `node/node.go:batchedTopic` | add 3 topic prefixes to `batchedTopic` (§8) |
| Custody / DA gate | the whole data-columns phase (`data-columns-spec`), unchanged | — (reused verbatim; gates the AC vote) |
| Metrics identity | `metrics.MsgID` + `AttestID`/`AggregateID` | `ACVoteID`/`FinalityVoteID`/`FinalityAggregateID` (§9) |
| Analysis | `analyze_attestations`/`analyze_aggregates` (`analysis/check_arrivals.py`) | `analyze_ac_votes`/`analyze_finality_votes`/`analyze_finality_aggregates` (§9) |
| Config | `AttestationConfig`/`SyncConfig`/`DataColumnsConfig` | `DecoupledConsensusConfig` (§10) |

**New code is small:** a `pb.ACVote` / `pb.FinalityVote` / `pb.FinalityAggregate` wire triple (§7),
the AC-voter + FC-subnet + FC-aggregator schedule fields (§4), a **finality-slot-spanning** runner
state (the one structurally new thing — FC events span `k` AC slots, §6), and the three emit paths.
Everything else is a loop or a field next to its attestation/sync/column analogue.

---

## 2. The two chains — clocks and knobs

**One clock: the AC slot.** The base loop is the existing per-AC-slot loop
(`driver/runner.go:Run`). A **finality slot** is `k` AC slots; finality slot `n` spans AC slots
`[n·k, (n+1)·k − 1]` and its events fire at AC-slot offsets derived from `n·k`. We **run `x` AC
slots with `x > k`** and measure a settled finality slot (§11). The AC keeps producing a block +
`ac_vote_size` votes **every** AC slot, including the ones inside a finality slot — so the FC burst
**contends** with AC traffic on each node's single verifier and pipe. *That contention is a primary
thing we measure* (the AC and FC loads share one CPU per node).

### Knobs (all configurable; mainnet defaults / test values)

| Knob | Meaning | Mainnet | Tests |
|---|---|---|---|
| `ac_vote_size` | VRF-selected validators voting on the AC each slot (global topic) | 512 | 8 |
| `ac_slots_per_finality_slot` = `k` | AC slots per finality slot | 10 | 2 |
| `fs_subnets` | finality subnets (validator-partitioned; stable node receiver core) | 40 | 2 |
| `fs_aggregators` | aggregator validators per FC subnet (whole-set sample) → aggregates | 16 | 2 |
| `finality_slot_aggregation_fraction` | % of the finality slot when aggregators publish | 50 | 50 |
| `fc_vote_offset_ms` | offset into the finality slot when validators emit the FC vote | 1000 | 1000 |

Reused from the existing phases (no new knob): `V` (`attestation.validators`), the AC-vote deadline
(`attestation.attestation_due_ms`, ≈4 s) and its `prep_ms`, the whole `data_columns` block, and the
topology / `num_slots` / `slot_duration` / `block_size` globals.

**Constraints (asserted at generation):** `ac_vote_size ≤ V`; `fs_subnets ≤ N`; `fs_aggregators`
clamped to `V`; `V ≥ N` (inherited from data-columns — every node validates
⇒ uniform custody 8 and a non-empty FC vote set per node); `topology.super_node_fraction > 0`
(columns need full-custody supernodes). `0 < finality_slot_aggregation_fraction < 100`;
`fc_vote_offset_ms < finality_slot_aggregation_fraction% · k · slot_duration` (votes precede the
aggregate).

---

## 3. Availability chain — global vote flood, DA-gated

The AC is **the existing block+column machinery, plus a global vote flood, minus the attestation
subnet/committee/aggregation structure.**

### 3a. Block + columns (reused verbatim)

The per-slot proposer is a **full-custody supernode** (drawn from `full_custody`, exactly as
data-columns §3). At `slotStart + offset + rand(jitter)` it publishes the block on the global block
topic **and** one `DataColumnSidecar` on each of `num_columns` column subnets — the t=0 burst
(`data-columns-spec §4`). Nodes custody a seeded-random `custody_floor` (8) columns (full-custody
nodes all). **Nothing here changes.**

### 3b. The 512 votes — one global topic, no aggregation

Each AC slot, a **seeded per-slot draw** of `ac_vote_size` distinct validators (a stand-in for VRF
selection; `_rng(seed, 8, slot)`) each emit **one** `ACVote` on the single **global**
`AvailabilityVoteTopic`. A node holding `m` of the slot's selected validators emits `m` votes
(k-multiplicity, like attestations). **Every node downloads all `ac_vote_size` votes** — coverage is
`N − 1` per vote (loopback skip), exactly like the block and aggregate global topics. There is **no
AC-level aggregation**.

### 3c. The vote is block→vote coupled **and DA-gated** (reused)

An `ACVote` votes the block iff, by the AC attestation deadline, the node has **(a)** processed the
block **and (b)** received every column on the subnets it custodies — else it votes prior head. This
is **literally the existing column-gated attestation rule** (`driver/runner.go:tryEarlyEmit`,
`emitDecision` with `ready = seen && columnsComplete`, `processed = laterOf(seenAt,
columnsCompleteAt)`), reused unchanged. The only differences from today's attestation emit:

- duties come from the **AC-voter draw** (`ACVoteDuties(slot)`), not committee membership;
- emit is on the **global** `AvailabilityVoteTopic`, not a per-subnet topic;
- there is **no aggregator phase**.

The measurable headline is `voted_origin` per vote → **fraction voting block vs. prior head**,
bucketed by block/column arrival — the same metric the column gate exists to produce, now over the
512 global voters.

**Deadline:** reuse `attestation.attestation_due_ms` (≈4 s) and `prep_ms`. No new knob.

---

## 4. Finality chain — validator-partitioned subnets, fan-out votes, scaled aggregates

### 4a. Subnet assignment — per-validator draw + a stable node receiver core

Two independent draws, two roles:

- **`finality_subnet_of[v]`** = the subnet validator `v` votes on — an **independent seeded
  uniform draw per validator** (`_rng(seed, 12)`, slot-independent), so subnets partition the
  **validator set** like attestation committees: ≈`V/fs_subnets` votes/subnet (± binomial noise)
  **regardless of where keys live** — a 1000-key supernode's votes spread over ~all subnets, not
  one. Rides `schedule.json` as a plain array of `V` small ints (~1–3 MB at `V`=400k).
- **`fs_subscribers[i]`** = the **member nodes** of subnet `i` — a stable node partition
  (`_rng(seed, 9)`): the subnet's mesh/receiver core **and** the base of the **expected receiver
  set** for coverage (§9). Every node **persistently subscribes** its one subnet at bring-up
  (`Prepare`) and never leaves — the relay fabric is stable for the run.
- **`validators_per_subnet[i]`** = the draw's per-subnet counts (`Σ [finality_subnet_of[v] == i]`)
  — carried for the aggregate size model (§7) and the coverage count (§9); decoupled from
  member-node hosting (skew no longer lumps it — see skewed-validators-spec §6).

### 4b. The votes — per-validator, fixed-time, fan-out where not a member

At `finalitySlotStart(n) + fc_vote_offset_ms` (a **fixed** instant, ≈1 s into the finality slot —
**no block coupling**, no DA gate), every node emits **one** `FinalityVote` **per validator it
hosts**, each on **its validator's** drawn subnet. Per subnet that is ≈`V/fs_subnets` messages —
the large flood the feature exists to measure. Where the node is **not** a member, it publishes by
**gossipsub fan-out** exactly like non-decoupled attestations (att-subnet.md): at the previous AC
slot (`n·k−1`) it Joins the topic and dials 2 of the subnet's stable members, so the burst pushes
straight through; the dials drop at the aggregation deadline. Each vote is keyed by
`(finality_slot, subnet, val, origin)`; coverage per vote is `fs_subscribers[i]` ∪ the slot's
aggregator hosts for `i`, minus the publisher when it is in that set. The vote carries no measured
fork-choice outcome (§0) — it is sized filler (§7), and we record only **when** it arrives and
**whether the subnet is covered**.

**Content is a planned extension (not this cut).** A later cut gives the finality vote *content* — a
measured fork-choice **target** (which finalized tip each validator votes), unlocking
agreement / coverage-**by-target** metrics and, eventually, a **coupling** (the target derived from
the AC chain state each node has seen by `fc_vote_offset`). `pb.FinalityVote` reserves room for it
(add a `target` / `voted_origin` field; 226 B is unchanged or grows by a root). This cut stays
dissemination-only; **nothing below changes when content lands** except the recorded per-vote
outcome — a clean seam, like the custody **skew** (§13).

### 4c. The aggregators — 16 validators/subnet from the whole set, pre-join Subscribe

Per finality slot `n`, a seeded draw (`_rng(seed, 10, n)`) samples `fs_aggregators` (16)
**validators from the ENTIRE validator set** per subnet — unrelated to which subnet they vote on,
unrelated to subnet membership, unrelated to where their node sits (stored as `(val, node)` refs;
the **host node carries the duty**, and two selected validators on one host dedupe to one
aggregate). Each host collects the subnet's votes and publishes **one** `FinalityAggregate` on the
**global** `FinalityAggregateTopic` at the aggregation deadline. With `fs_subnets ·
fs_aggregators` = 640 aggregates at mainnet, **every node downloads all of them** (coverage
`N − 1` each). Each aggregate's **size scales** with `validators_per_subnet[i]` (the aggregation
bitfield, §7) — ≈3.4 KB at 25 000/subnet, ≈2.2 MB of global aggregate traffic per finality slot.

**Pre-join (the load-bearing timing detail).** An aggregator host for finality slot `n` generally
has **no connection to subnet `i` at all** — so at the start of the previous AC slot (`n·k − 1`;
the boundary itself for `n=0`) it **dials 2 of the subnet's stable members and Subscribes** (a
real mesh join, not fan-out), giving it a full AC slot (~12 s) of mesh formation before the
`fc_vote_offset` burst. It collects the subnet's votes through the slot, publishes its aggregate
at the deadline, then **Unsubscribes and drops the dials** (it stays only if it is independently a
stable member; a teardown spares whatever the next finality slot's pre-join — which can land on
the very same instant — still needs).

### FC slot timeline (`k`=10, slot=12 s ⇒ finality slot = 120 s)

```
 AC slot:  n·k−1        n·k                 …                 n·k + k/2            (n+1)·k
            |            |==================finality slot n=====================|
 pre-join   ▲                                                                      next finality slot
 (voters    │  ▲ fc_vote_offset (~1 s):                       ▲ aggregation_fraction (50% ⇒ +60 s):
  Join+dial │  │ every validator's vote rides ITS             │ each aggregator host publishes one
  foreign   │  │ drawn subnet (fan-out where the              │ FinalityAggregate (scaled size) on
  subnets;  │  │ host isn't a member;                         │ the global topic, then Unsubscribes
  agg hosts │  │ ≈V/fs_subnets per subnet)                    │ + drops the pre-join dials
  Subscribe)│  └─── votes disseminate within each subnet ─────┘   → aggregates flood globally
```

Meanwhile the AC fires a block + `ac_vote_size` votes **every** AC slot across the whole window.

---

## 5. Topology — `fs_subnets` persistent meshes + the reused column/global trees

When `decoupled_consensus` is on, both graph builders construct: the **global** spanning tree (block
+ AC-vote + FC-aggregate global topics), the **per-column** trees (`column_subscribers`, reused from
data-columns), and the **per-FC-subnet** trees over `fs_subscribers` — **instead of** the attestation
and sync subnet trees (those phases are off). Each FC subnet is **fat** (≈`N/fs_subnets` member
nodes; ~100 at mainnet, 2.5 % of N) so it connects easily, but the guarantee comes from construction
(the project's "connectivity by construction, not chance" rule, `attestation-spec §3`):

- `netsim.discv5Graph` (`netsim/subnet.go:35`): global tree → column trees → **a loop over
  `a.FinalitySubscribers`** (random spanning tree per subnet) → `fill`-to-K.
- `simctl.generate_subnet_topology` (`simctl/topology.py:272`): the same, mirroring the existing
  column loop.

Reuses `random_tree`/`randomTree` + `fill` unchanged. No `per_subnet_floor` top-up needed (the
membership *is* the subscriber set). The full-custody column backbone (data-columns §3) is unchanged.

---

## 6. Emit — three paths, all riding existing machinery

`NodeRunner` gains a `decoupled bool` (from `decoupled_consensus.enabled`) that gates all of the
below and, when set, **suppresses** the attestation and sync emit/subscribe paths. `Prepare`
(`driver/runner.go:Prepare`) under `r.decoupled` subscribes: the global `AvailabilityVoteTopic`, the
global `FinalityAggregateTopic`, the node's **one** `FinalityVoteTopic(FinalitySubnet())`
(persistent), and its custody column subnets (reused). It does **not** subscribe attestation/sync
topics.

### 6a. AC vote — column-gated, rides the existing block/column trigger

The AC vote **is** the column-gated attestation emit (§3c), retargeted. In `beginSlot`, set
`acDuties = view.ACVoteDuties(slot)`. The existing block-receipt and columns-complete paths
(`onBlockProcessed`, `onColumnProcessed`) and the deadline path (`onDeadline`) already compute
`emitDecision(seen && columnsComplete, laterOf(seenAt, columnsCompleteAt), deadline, prep)`; under
`r.decoupled`, the emit publishes one `MakeACVote(slot, val, self, votedOrigin)` per `acDuties` on
`AvailabilityVoteTopic` and records `ACVoteID(slot, val, self)` with the voted-block bool — instead
of the per-subnet attestation. `emitOnce` still guarantees one emission per slot. The proposer's
self-block/self-columns make `columnsComplete` immediate (reused).

### 6b. FC vote — fixed-time, per-validator, un-gated

Structurally new: FC events **span `k` AC slots**, so the runner gains a finality-slot-keyed state
`finals map[int]*finalityState` (distinct from the per-AC-slot `slots map[int]*slotState`), armed at
a finality boundary and pruned after the aggregate drains.

- At `beginSlot(s)`, if `s % k == 0` (a finality boundary, `n = s/k`): build `finals[n]`, arm a
  one-shot timer at `finalitySlotStart(n) + fc_vote_offset_ms` → `emitFinalityVotes(n)`, and arm the
  aggregate timer (6c).
- `emitFinalityVotes(n)`: for **every** duty `(v, subnet)` the node hosts
  (`view.FinalityVoteDuties()` pairs its `nodeVals` with `finality_subnet_of`), publish
  `MakeFinalityVote(n, subnet, v, self)` on `FinalityVoteTopic(subnet)`, record
  `FinalityVoteID(n, subnet, v, self)`. No coupling, no gate — a fixed-time burst. Non-member duty
  subnets were Joined + dialed at the pre-join slot, so the publish lands via fan-out.

### 6c. FC aggregate — fixed-deadline global flood, an aggregate twin

- **Pre-join:** at `beginSlot(s)` where `(s+1) % k == 0` (the AC slot before finality slot
  `n=(s+1)/k`; the boundary itself for `n=0`), for each subnet in `view.FinalityAggregations(n)`,
  **dial 2 members and Subscribe** it (the host is generally not a member); also Join + dial each
  non-member vote-duty subnet (mirrors the attestation per-slot dial, `runner.go:beginSlot`). All
  recorded for teardown at the aggregation deadline.
- At the boundary, arm a one-shot timer at `finalitySlotStart(n) + aggregation_fraction% · k ·
  slotDur` → `emitFinalityAggregate(n)`.
- The aggregator **collects** votes into `finals[n]` from `onReceive`'s `KindFinalityVote` case
  across the AC slots of the finality slot, until the aggregate deadline.
- `emitFinalityAggregate(n)`: publish **one** `MakeFinalityAggregate(n, subnet, self,
  validators_per_subnet[subnet])` per aggregated subnet on the global `FinalityAggregateTopic`,
  record `FinalityAggregateID(n, subnet, self)`. Mirrors `emitAggregate`; `aggOnce` guards it.
  Then Unsubscribe the foreign subnets and drop the pre-join dials (sparing what the next finality
  slot's pre-join, possibly this same instant, still lists).

---

## 7. Wire format & topics

```proto
// pb/decoupled.proto  (proto3, package block, same go_package)
message ACVote {                 // → 240 B (block_hash 32 + BLS sig 96 + selection proof 96 + ids)
  uint32 slot         = 1;
  uint32 val          = 2;       // global validator index — the stable identity
  uint32 origin       = 3;       // publishing node (gossip origin; loopback skip)
  uint32 voted_origin = 4;       // block origin, or PriorHead sentinel (reuse validator.PriorHead)
  bytes  payload      = 5;       // random filler to 240 B (sizedFiller)
}                                // NOTE: no subnet field — one global topic
message FinalityVote {           // → 226 B (att data 128 + sig 96 + validator id 2)
  uint32 finality_slot = 1;
  uint32 subnet        = 2;      // 0..fs_subnets-1 (the validator's drawn subnet)
  uint32 val           = 3;      // global validator index — the identity (one vote/validator)
  uint32 origin        = 4;      // publishing member node (gossip origin)
  bytes  payload       = 5;      // random filler to 226 B; dissemination-only (no voted_origin)
}
message FinalityAggregate {      // → 328 + ceil(validators_per_subnet/8) B (scaled bitfield)
  uint32 finality_slot = 1;
  uint32 subnet        = 2;      // subcommittee (distinctness; topic is global)
  uint32 origin        = 3;      // aggregator node (distinct ⇒ no gossip dedup)
  bytes  payload       = 4;      // filler sized to base + bitfield (§ below)
}
```

- Append `decoupled.proto` to `pb/doc.go` go:generate; regenerate.
- **`validator/decoupled.go`** (new), mirroring `attestation.go` + `aggregate.go` + `sync.go`:
  - `AvailabilityVoteTopic = "/eth2/00000000/availability_vote/ssz_snappy"` (global);
    `ACVoteSize = 240`; `MakeACVote(slot, val, origin, votedOrigin)`.
  - `FinalityVoteTopicPrefix = "/eth2/00000000/finality_vote_"`; `FinalityVoteTopic(i)`;
    `FinalityVoteSize = 226`; `MakeFinalityVote(finalitySlot, subnet, val, origin)`.
  - `FinalityAggregateTopic = "/eth2/00000000/finality_aggregate_and_proof/ssz_snappy"` (global);
    `FinalityAggregateBase = 328`; `FinalityAggregateSize(vps) = FinalityAggregateBase + (vps+7)/8`;
    `MakeFinalityAggregate(finalitySlot, subnet, origin, vps)`. Reuse `sizedFiller` for the AC/FC
    votes; for the aggregate, size the payload to `FinalityAggregateSize(vps)` — at mainnet vps=25 000
    that is 3 453 B (< 16 384, `sizedFiller` ok); for vps ≳ 130 000 size the payload directly like
    `makeBlock` (the §15 varint caveat, as data-columns §14).
- **`node/node.go`:** `KindACVote = 7`, `KindFinalityVote = 8`, `KindFinalityAggregate = 9` (near
  `:34`); `decode` (`:364`) three cases by topic prefix → the new kinds.

---

## 8. Verify — reuse the per-node batched verifier (no new component)

All three decoupled floods — the 512 AC votes (t≈4 s, global), the ~25 000/subnet FC votes (a fixed
burst), and the 640 FC aggregates (the aggregation deadline, global) — are M/D/1 floods on the
node's **single** CPU, the queue the batched verifier already models (`node/verifier.go`,
`scaling.md §7`). The ~25 000/subnet FC burst through one core, **contending with the AC traffic**,
**is** the bottleneck the feature measures — so all three must **join the one batched queue**, not
get their own.

- Extend `batchedTopic` (`node/node.go:221`) to also return true for
  `topic == AvailabilityVoteTopic`, `HasPrefix(topic, FinalityVoteTopicPrefix)`, and
  `topic == FinalityAggregateTopic`. That alone routes all three through `verifier.submitAndWait` in
  `registerVerifyHook` — **no new case, no new semaphore, no new flags.** (Data columns keep their
  width-P semaphore, unchanged; the AC block keeps its fixed per-hop hook.) Per-item verify cost is
  the existing `attest_per_item_ms` knob (calibratable).

---

## 9. Metrics & output

- **`metrics.ACVoteID(slot, val, origin)`** mirrors `AttestID` minus subnet: `Kind=KindACVote,
  Subnet=-1, Attester=val, Origin=origin`; carries the voted-block bool via `OnPublish`.
- **`metrics.FinalityVoteID(fslot, subnet, val, origin)`** mirrors `AttestID`:
  `Kind=KindFinalityVote, Subnet=subnet, Attester=val, Origin=origin` (`fslot` rides `Slot`).
- **`metrics.FinalityAggregateID(fslot, subnet, origin)`** mirrors `AggregateID`:
  `Kind=KindFinalityAggregate, Subnet=subnet, Attester=origin, Origin=-1`.
- No `MsgID`/CSV schema change (`kind`/`subnet`/`attester` already carry it).
- **Headline metrics (the goal — "time to disseminate the block and aggregate the votes"):**
  - **AC:** block-arrival CDF (reused) + the global **AC-vote** arrival CDF + **`fraction_voted_block`**
    over `KindACVote` (reuse `FractionVotedBlock`) + the column custody-complete rate (reused) — the
    block→vote→DA story, now over the 512 global voters.
  - **FC dissemination:** per-subnet **FinalityVote** arrival CDF + coverage, and the **vote-coverage
    at the aggregation deadline** — the fraction of each subnet's votes that reached its aggregators
    by `aggregation_fraction%` (a new `FinalityCoverageAtDeadline(fslot, subnet, due)` alongside
    `CustodyCompleteRate`). This is *how much of the vote each aggregate captures*.
  - **FC aggregation:** the global **FinalityAggregate** arrival CDF (relative to the
    aggregation-deadline publish instant) — *when the finalized aggregate is everywhere*.
- **`analysis/check_arrivals.py`:** `analyze_ac_votes` (global `N−1` coverage + dedup + CDF +
  fraction-voted-block, mirroring `analyze_aggregates` + the voted-block column);
  `analyze_finality_votes` (per-vote coverage of `fs_subscribers[i]` ∪ the slot's aggregator
  hosts, minus the publisher when in the set — aggregator hosts are REQUIRED receivers; no-leak,
  missing, dup, CDF, coverage-at-deadline, mirroring `analyze_attestations`);
  `analyze_finality_aggregates` (global `N−1` coverage + dedup + CDF, mirroring `analyze_aggregates`).
  A `load_finality_subscribers` loader reads `finality_subscribers` from `schedule.json`. Kinds
  `7`/`8`/`9` added to the constants + `main`/`compare` wiring.

---

## 10. Config surface

A `decoupled_consensus:` block on `SimConfig` (mirrors the other phase configs; present + enabled ⇒
run decoupled, **disable** attestation/sync emit):

```yaml
decoupled_consensus:
  enabled: true
  ac_vote_size: 512                      # VRF-selected validators voting on the AC each slot (tests: 8); ≤ V
  ac_slots_per_finality_slot: 10         # k (tests: 2)
  fs_subnets: 40                         # finality subnets, validator-partitioned (tests: 2); ≤ N
  fs_aggregators: 16                     # aggregator validators per FC subnet, whole-set sample (tests: 2)
  finality_slot_aggregation_fraction: 50 # % of the finality slot when aggregates publish
  fc_vote_offset_ms: 1000                # offset into the finality slot for the FC vote burst

# REQUIRED alongside (reused): the AC vote needs V + the deadline; columns gate the AC vote.
attestation:
  enabled: false        # the OLD attestation messages are OFF; this block only supplies V + deadlines
  validators: 2000      # V (≥ N)
  attestation_due_ms: 4000   # the AC-vote deadline (reused; no new knob)
  prep_ms: 0
data_columns:
  enabled: true         # columns gate the AC vote (the "availability" chain); proposer = full-custody
  num_columns: 128
  blobs: 6
  custody_floor: 8
  full_custody_fraction: 0.5
  # … (the full data_columns block, unchanged)
```

- Validation (`_check_decoupled_consensus`): requires the `attestation` block present for `V` +
  `attestation_due_ms` (but its `enabled` is ignored for messages — decoupled drives them); requires
  the `data_columns` block enabled (the AC gate) and `super_node_fraction > 0` and `V ≥ N`; asserts
  `ac_vote_size ≤ V`, `fs_subnets ≤ N`, the aggregation-fraction/vote-offset ordering. With
  `decoupled_consensus.enabled`, the generator **skips** attestation-committee and sync membership
  generation and emits the AC/FC fields instead (§4). `sync` and the old attestation/aggregate emit
  are mutually exclusive with decoupled (asserted).

---

## 11. Build / test milestones (TDD — each a failing synctest test first, green before the next)

Mirrors the data-columns / sync milestone style: **exact counts from a seeded assignment.** Reuses
the `synctest.Test` + `buildScenario`/`run`/`assertCoverageNoLeakage` skeleton
(`driver/shared_test.go`), the forced-flip coupling test (`driver/coupling_network_test.go`), the
verifier inequality (`node/verifier_test.go`), and the `raceEnabled` skip for sized runs.

1. **Wire + topics + decode + size.** `pb.ACVote` / `pb.FinalityVote` / `pb.FinalityAggregate`
   publish→decode→arrival on their topics; size unit tests (240 B / 226 B / `328+⌈vps/8⌉`, non-zero
   filler) and topic formats. *(mirror `validator/attestation_test.go`, `driver/columnonly_test.go`.)*
2. **Schedule membership.** `simctl/schedule.py` emits per-slot `ac_voters`, stable
   `finality_subscribers` (the node receiver core), the per-validator `finality_subnet_of`,
   per-finality-slot `fc_aggregators` (whole-set validator refs), and `validators_per_subnet`;
   `schedule.View` exposes `ACVoteDuties`/`FinalitySubnet`/`FinalityVoteDuties` ((val, subnet)
   pairs)/`FinalitySubscribersOf`/`FinalityAggregations`. Assert: `|ac_voters[slot]| =
   ac_vote_size`, voters ⊆ V, fresh per slot; every node in exactly one FC subnet; every validator
   one subnet, `validators_per_subnet` = the draw's counts, `Σ = V`; aggregator refs from the
   whole set, count clamped to V; the `ac_vote_size ≤ V` / `fs_subnets ≤ N` guards fire;
   **same-seed-identical / seed+1-differs.** Go in `schedule/schedule_test.go`, Python in
   `tests/test_schedule.py`. *(mirror the sync/column membership tests.)*
3. **AC vote — global coverage + the DA-gated flip.** Each selected validator publishes one AC vote
   on the global topic; **every other node receives it once** (`N−1`, no-leak). Then the
   **column-gated forced flip**: suppress the block (or drop one custody column) to one voter past
   the deadline → its AC vote votes **prior**, the others **block**; `fraction_voted_block ==
   (voters−1)/voters`. *(mirror `driver/coupling_network_test.go` + `column_gate_test.go`; reuses the
   existing gate verbatim.)*
4. **FC vote — per-subnet coverage, multiplicity, fan-out.** At `fc_vote_offset`, every node emits
   one vote **per hosted validator**, each on **its validator's** drawn subnet (a node with keys on
   2 subnets votes on both; a non-member vote rides the fan-out); each vote reaches exactly the
   subnet's members ∪ the slot's aggregator hosts, minus the publisher when in the set.
   `got == want` set-equality both directions. *(mirror `assertCoverageNoLeakage` / `sync_coverage_test.go`.)*
5. **FC aggregate — scaled size, global coverage, pre-join, deadline.** The aggregator hosts emit
   distinct aggregates on the global topic at `slotStart(n) + aggregation_fraction%·k·slotDur`; each
   reaches `N−1`; size `== 328+⌈vps/8⌉`; assert a **non-member** aggregator host **dialed + Subscribed
   the subnet at AC slot `n·k−1`**, held its votes, and dropped it all after publishing.
   *(mirror `aggregate_test.go` + the per-slot dial.)*
6. **Full run + metrics.** A sized run (≥ `k+ k/2 +` settle AC slots) writes the AC block + AC-vote
   CDFs + `fraction_voted_block`, the per-subnet FC-vote CDF + coverage-at-deadline, and the global
   FC-aggregate CDF; Python `analyze_*` confirm coverage/no-leak. **Race-skip** if sized. Run twice
   in one process (two bubbles, same seed) → byte-identical sorted arrivals. *(mirror
   `column_fullrun_test.go` / `sync_fullrun_test.go`.)*

**Sizing for M6:** `N=16`, `V=32`, `ac_vote_size=8`, `k=2`, `fs_subnets=2`, `fs_aggregators=2`,
`num_columns=8` — 8 global AC voters/slot; 2 FC subnets of ~8 nodes (~16 validators each → ~16
votes/subnet); 2 aggregators/subnet (4 aggregates). Measure finality slot **1** (vote at AC slot 2,
aggregate at AC slot 3); run ≥5 AC slots. Keeps per-event deliveries in the low hundreds (the
proven-safe band).

---

## 12. Defaults

| Knob | Default | Tests | Source / note |
|---|---|---|---|
| `ac_vote_size` | 512 | 8 | `SYNC_COMMITTEE_SIZE`-scale VRF count; ≤ V |
| `ac_slots_per_finality_slot` (`k`) | 10 | 2 | the brief |
| `fs_subnets` | 40 | 2 | the brief (40 for 4k nodes) |
| `fs_aggregators` | 16 | 2 | `TARGET_AGGREGATORS_PER_COMMITTEE`; clamp to members |
| `finality_slot_aggregation_fraction` | 50 | 50 | the brief |
| `fc_vote_offset_ms` | 1000 | 1000 | "pretty early in the slot, ~1 s" |
| AC vote size | 240 B | — | block_hash 32 + sig 96 + selection proof 96 + ids |
| FC vote size | 226 B | — | att data 128 + sig 96 + validator id 2 |
| FC aggregate size | `328 + ⌈vps/8⌉` B | — | base + scaled aggregation bitfield (≈3.4 KB @ 25 k) |
| AC vote deadline | `attestation_due_ms` (4 s) | — | shared instant; no new knob |
| AC vote DA gate | columns (data-columns gate) | — | reused verbatim |
| verify | shared batched verifier | — | no new params |
| V→N distribution | uniform (`V ≥ N`) | — | `Dist` seam; skew later (§13) |

---

## 13. Decisions (all answered)

- **Opt-in & exclusivity:** `decoupled_consensus.enabled`. When on, the **attestation + aggregate +
  sync** phases are **off** (the AC vote replaces attestations; FC replaces finality). **Data columns
  stay on** and **gate the AC vote**.
- **Availability chain:** the existing block + column path, plus a **global** flood of `ac_vote_size`
  (512) **per-slot VRF-selected validator** votes on one topic, **no aggregation**, every node
  downloads all (`N−1` coverage). The vote is **block→vote coupled + DA-gated** (reuse
  `emitDecision` + the column gate); deadline = `attestation_due_ms`; size 240 B; headline =
  `fraction_voted_block`.
- **Clock:** base **AC slot**; a finality slot = `k` AC slots; run `x > k` AC slots and measure a
  settled finality slot. The AC keeps running inside the finality slot — **AC↔FC contention on one
  CPU/node is measured on purpose.**
- **Finality chain — assignment:** subnets partition the **validator set** (`finality_subnet_of`,
  one uniform draw per validator → ≈`V/fs_subnets` votes/subnet regardless of hosting); every node
  is additionally a stable member of **one** subnet (`finality_subscribers`, the persistent
  mesh/receiver core it subscribes at bring-up).
- **Finality vote:** **per-validator** (k per node), each on **its validator's** drawn subnet —
  fan-out (Join + 2 dials at the pre-join slot) where the host isn't a member; **fixed-time** at
  `fc_vote_offset_ms` (~1 s), **un-gated, dissemination-only** (no fork-choice outcome measured)
  **this cut — content is a planned next step** (a measured target + eventual coupling, §4b);
  size 226 B.
- **Finality aggregate:** `fs_aggregators` (16) **validators per subnet from the whole set**, drawn
  per finality slot; each host publishes **one aggregate** (deduped per host) on a **global** topic
  at `finality_slot_aggregation_fraction` (50 %); **size scales** with the subnet's voting
  population (`328 + ⌈vps/8⌉`). The host dials + **Subscribes** the (generally foreign) subnet at
  the **previous AC slot** (`n·k−1`), and Unsubscribes + drops the dials after publishing — so it
  is meshed long enough to collect slot-`n` votes.
- **Verify:** reuse the per-node **batched verifier** — all three floods join the one queue (one CPU);
  no new component.
- **Distribution:** **uniform** `V/N` (`V ≥ N`) now, behind a `Dist` seam so operator skew drops in
  later without changing anything below.

---

## 14. Implementation map (files, smallest-first — matches §11)

- **`pb/decoupled.proto`** (new) + `pb/doc.go` go:generate; regenerate `pb/decoupled.pb.go`.
- **`validator/decoupled.go`** (new): the three topics/sizes + `MakeACVote` / `MakeFinalityVote` /
  `MakeFinalityAggregate` (reuse `sizedFiller`, `PriorHead`; aggregate sized by `vps`).
- **`node/node.go`:** `KindACVote=7` / `KindFinalityVote=8` / `KindFinalityAggregate=9`; `decode`
  cases (`:364`); `batchedTopic` += the three topics (`:221`). **No verifier change.**
- **`schedule/schedule.go`:** `Assignment.FinalitySubscribers [][]int` + `FinalitySubnetOf []int`
  + `ValidatorsPerSubnet []int` + per-slot `SlotPlan.ACVoters []AttesterRef` + per-finality-slot
  `FinalityAggregators [][]AttesterRef` (json tags); `View.ACVoteDuties` / `FinalitySubnet` /
  `FinalityVoteDuties` / `FinalitySubscribersOf` / `FinalityAggregations`.
- **`simctl/schedule.py`:** generate `ac_voters` (`_rng(seed,8,slot)`), `finality_subscribers`
  (the node receiver core, `_rng(seed,9)`), `finality_subnet_of` (per-validator, `_rng(seed,12)`),
  `fc_aggregators` (whole-set validator refs, `_rng(seed,10,fslot)`), `validators_per_subnet` (the
  draw's counts); skip committee/sync gen when decoupled; `to_dict` keys; `Params` fields;
  asserts (`ac_vote_size≤V`, `fs_subnets≤N`).
- **`simctl/topology.py`:** per-subnet tree loop over `finality_subscribers`
  (`generate_subnet_topology`, `:272`).
- **`netsim/subnet.go`:** `discv5Graph` (`:35`) loop over `a.FinalitySubscribers` (in place of the
  attestation/sync loops when decoupled).
- **`simctl/config.py`:** `DecoupledConsensusConfig` block on `SimConfig`;
  `_check_decoupled_consensus` (needs attestation block for V+deadline, data_columns enabled,
  `super_node_fraction>0`, `V≥N`; mutually exclusive with sync/old-attestation emit).
- **`driver/runner.go`:** a `decoupled bool` field gating all of the below (and suppressing
  attestation/sync emit); `slotState` AC-vote duties (reusing the column gate); a new
  **`finals map[int]*finalityState`** (FC events span `k` AC slots) with FC vote duties, the
  aggregator's collected votes, and the aggregate timer; `Prepare` subscribe `AvailabilityVoteTopic`
  + `FinalityAggregateTopic` + the node's `FinalityVoteTopic` + custody columns; `beginSlot` arm the
  FC vote + aggregate timers at finality boundaries and the **previous-AC-slot aggregator dial**;
  `emitACVote` (global, no-agg twin of `emit`), `emitFinalityVotes` (per-validator burst),
  `emitFinalityAggregate` (scaled-size twin of `emitAggregate`); `onReceive` `KindACVote` /
  `KindFinalityVote` (aggregator collects) / `KindFinalityAggregate` trace cases.
  **`driver/driver.go`:** thread `decoupled` + the knobs (`ac_vote_size`, `k`, `fs_subnets`,
  `fs_aggregators`, `aggregation_fraction`, `fc_vote_offset`) into `Config`/`NewRunner`.
- **`metrics/tracer.go`:** `ACVoteID` / `FinalityVoteID` / `FinalityAggregateID`;
  `FinalityCoverageAtDeadline` alongside `CustodyCompleteRate`; `FractionVotedBlock` reused for AC.
- **`analysis/check_arrivals.py`:** `analyze_ac_votes` / `analyze_finality_votes` /
  `analyze_finality_aggregates` + `load_finality_subscribers` + kinds 7/8/9; `main`/`compare` wiring.
- **`cmd/slot-sim-node/main.go`:** a `-decoupled` enable flag + the knobs (AC deadline reuses
  `-att-due`; columns reuse their flags); read AC voters / FC subnet / FC aggregators from
  `schedule.json`. **`simnetrun/run_test.go`:** a `decoupled` enable key. **`simctl/runner.py`:**
  thread the decoupled config into Params / host args / simnet params.

---

## 15. Open implementation risks

- **Finality-slot-spanning state (the one structural novelty).** Unlike every prior phase, FC events
  live across `k` AC slots — the aggregator collects from `n·k−1` (pre-join) through the aggregation
  deadline (`n·k + k/2`). The new `finals` map must arm at the boundary, survive `beginSlot`/`endSlot`
  of the intervening AC slots (which only prune `slots`), and prune after the aggregate drains. M5/M6
  verify the aggregator collects votes that arrive several AC slots after the burst.
- **AC↔FC contention is the measurement, not a bug.** With both floods on one batched verifier, the
  FC burst's tail can run long under AC load; M6 should sanity-check that a finality slot's aggregate
  CDF closes within the finality slot, and surface it as a *metric* (this is the headline question:
  does the finalized aggregate beat the next finality slot?).
- **Scaled aggregate + `sizedFiller` varint.** `validator/attestation.go:sizedFiller` assumes payload
  < 16 384 B (2-byte varint). At mainnet `vps`=25 000 the aggregate is 3 453 B (fine); for `vps ≳
  130 000` (extreme V or few subnets) size the payload directly like `makeBlock` (data-columns §14).
- **Persistent membership vs. the brief's "leave after aggregating."** We keep all members subscribed
  for the whole run (warm mesh) and model "join previous slot / leave after" as the aggregator's
  *extra-peer* dial/drop. If membership is later made non-persistent (a sampled relay set), the
  previous-AC-slot pre-join becomes load-bearing for collection — the dial hook is already in place.
- **Thin FC subnets at extreme N.** At `N ≫ fs_subnets` each subnet is fat (≈`N/fs_subnets` nodes);
  the per-subnet tree keeps it connected and coverage is computed from the actual `fs_subscribers`
  set, so counts stay exact even when a subnet's *validator* population (the load) is huge.

---

## 16. Summary

Decoupled consensus runs two chains on one fleet. The **availability chain** is the existing block +
data-column burst plus a **global** flood of `ac_vote_size` (512) seeded-per-slot validator votes on
one topic — no committees, no subnets, no aggregation, every node downloads all — each **block→vote
coupled and DA-gated** (the column gate, reused verbatim; headline `fraction_voted_block`). The
**finality chain** fires every `k` AC slots: every node emits **one fixed-time `FinalityVote` per
validator it hosts**, each on **its validator's** drawn subnet (`finality_subnet_of` partitions the
validator set over `fs_subnets` (40) subnets, ≈`V/fs_subnets` ≈ 25 000 votes/subnet; fan-out where
the host isn't a stable member) at ~1 s; then `fs_aggregators` (16) validators/subnet from the
whole set — whose hosts **dial + Subscribe the subnet at the previous AC slot** — each publish one
**population-scaled** `FinalityAggregate` on a **global** topic at 50 % of the finality slot, every
node downloading all 640. All three floods share the existing per-node **batched verifier** (one CPU) and **contend with
the AC traffic on purpose**. Membership and the AC-voter draw are proven before any network; the AC
vote's flip is proven by the reused column/block-suppression test; and the headline output is the
block + AC-vote dissemination CDFs (with `fraction_voted_block`), the per-subnet FC-vote CDF +
coverage-at-the-aggregation-deadline, and the global FC-aggregate CDF — **the time to disseminate the
block and to aggregate the votes.**
