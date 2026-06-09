# Sync committees — message + contribution dissemination (simnet/shadow)

Implementation-ready spec. Adds the **sync-committee** message pair — the last unbuilt *live*
(Fulu) message type (`slot-messages.md §4.2/4.3`) — to the simulator. A seeded subset of
`sync_size` **nodes** are sync-committee members, each on one of `sync_subnets` subnets (stable for
the whole run). Each slot a member emits one `SyncCommitteeMessage` (144 B) on its subnet, voting
the head block at `min(block_seen, deadline)`; then `target_aggregators` per subnet emit one
`SignedContributionAndProof` (360 B) on **one global topic** (every node downloads all of them, like
the attestation aggregate) at the contribution deadline. We record per-subnet arrival, coverage, and
the head-vote outcome.

This rides the machinery already built for **attestations** (the block→vote coupling in
`driver/`, the per-node batched verifier) and **aggregates** (a fixed-deadline global flood). Sync
is "attestations with fixed membership + persistent subscribe + 4 subnets," plus an
aggregate-twin. It is mostly **reuse**.

> This is a spec, not code. Decisions are locked in §11; §13 lists open risks.

---

## 0. Non-goals (this cut)

Light-client updates (`light_client_*`, `slot-messages.md §9` — feature-gated, separate);
sync-committee **period rotation** (the 512 rotate every 256 epochs ≈ 27 h; a run is ~5 slots, so
membership is a single stable set — rotation is irrelevant and omitted); real BLS / SSZ;
stake-weighted sync-committee selection (uniform seeded draw here); the on-chain `SyncAggregate`
the proposer folds into the next block (we stop at the contribution gossip). Crypto is
validation-as-sleep, as everywhere.

---

## 1. Reuse vs. new — sync is mostly reuse

| Layer | Reuse **as-is** | Needs a **sync variant** |
|---|---|---|
| Coupling | `driver/coupling.go:emitDecision` (the pure `min(block,deadline)` rule) | — |
| Filler | `validator/attestation.go:sizedFiller` | — |
| Verify | the per-node **batched verifier** (`node/verifier.go`); `node/node.go:batchedTopic` | add 2 topic prefixes to `batchedTopic` (§6) |
| Block-seen state | `slotState.seen/seenAt/seenOrigin` + `onBlockProcessed` (`driver/runner.go`) | — |
| Subscribe-at-bring-up | `NodeRunner.Prepare` (`driver/runner.go:86`) | add a sync block (§4) |
| Per-subnet topology | `netsim.discv5Graph` (`netsim/subnet.go:35`), `simctl.generate_subnet_topology` (`simctl/topology.py:272`) | add a loop over `sync_subscribers` (§3) |
| Stable subscriber map | `column_subscribers` pattern (`simctl/schedule.py`, `schedule/schedule.go`) | add `sync_subscribers` (§2) |
| Per-slot aggregator draw | attestation `Aggregators` (`simctl/schedule.py`, `schedule.SlotPlan`) | add `sync_aggregators` (§2) |
| Message emit | `NodeRunner.emit` (`driver/runner.go:383`) | sync messages ride it, **un-gated** vote (§4) |
| Contribution emit | `NodeRunner.emitAggregate` (`driver/runner.go:406`) | `emitSyncContribution` twin (§4) |
| Metrics identity | `metrics.MsgID` + `AttestID`/`AggregateID` (`metrics/tracer.go`) | `SyncMessageID`/`SyncContributionID` (§7) |
| Analysis | `analyze_attestations`/`analyze_aggregates` (`analysis/check_arrivals.py`) | `analyze_sync`/`analyze_sync_contributions` (§7) |
| Config | `AttestationConfig` (`simctl/config.py`) | `SyncConfig` (§8) |

**New code is small:** a `pb.SyncMessage`/`pb.SyncContribution` wire pair (§5), the node-based
membership map (§2), and the two emit twins (§4). Everything else is a loop or a field added next
to its column/attestation analogue.

---

## 2. The sync committee — node-based membership

`plan.md`'s **topology seam**, generated centrally and seeded, identical on both backends, carried
in `schedule.json` (next to `subnet_subscribers` / `column_subscribers`). **Simplest faithful
model:** membership is a per-node attribute, not validator-derived.

### Three independent knobs

| Knob | Meaning | Mainnet | Tests |
|---|---|---|---|
| `sync_size` | sync-committee **member nodes** (a seeded subset of the `N`) | 512 | 8 |
| `sync_subnets` | subcommittees = subnets | 4 | 2 |
| `target_aggregators` | aggregators per subnet (→ contributions) | 16 | 2 |

- **Constraints (asserted at generation):** `sync_size ≤ N`; `sync_subnets ≤ sync_size`.
- `target_aggregators` is clamped to a subnet's member count (as attestation aggregators are clamped
  to subscribers, `simctl/schedule.py`).
- `sync_size` is **member nodes**, not validators — sync never touches the V→N map. (On mainnet the
  512 are stake-weighted validators; here each member node stands in for "a node hosting a committee
  seat," which is all the dissemination model needs. The fidelity dropped — stake weight, and
  k-seats-per-node — is immaterial for a ~512-message tail phase; see §11.)

### The per-node attribute

Each node carries `sync_committee_member: bool` + `sync_committee_subnet: 0..sync_subnets-1` (the
subnet is meaningful only for members). Generation (`_rng(seed, 6)`, slot-independent):

- **Members (stable):** draw `sync_size` node ids from `0..N-1`; assign each a subnet round-robin
  (member `j` → subnet `j % sync_subnets`), so subnets are even (≈`sync_size/sync_subnets` each, 128
  at mainnet). Stable for the whole run.
- **`sync_subscribers[i]`** = the member nodes on subnet `i` — the subnet's mesh **and** the
  **expected receiver set** for coverage (§7). Pre-stored as a node-set map, exactly like
  `column_subscribers`, so topology.py + check_arrivals.py just read it. (It is the transpose of the
  per-node attribute — one structure, no separate committee refs.)
- **`sync_aggregators[slot][i]`** (per-slot, `_rng(seed, 7, slot)`) = `target_aggregators` node ids
  drawn from `sync_subscribers[i]` — exactly as attestation `Aggregators` are drawn from a subnet's
  subscribers. Each emits one contribution. Count: `target_aggregators · sync_subnets` = 64/slot at mainnet.

### Derived per-node facts (Go `schedule.View`)

- **`SyncSubnet() (subnet int, member bool)`** — this node's membership + subnet (a lookup in
  `sync_subscribers`). A member emits one message on `subnet`; a non-member emits none.
- **`SyncSubscribersOf(subnet) []int`** — mirrors `ColumnSubscribersOf` (`schedule/schedule.go:96`).
- **`SyncAggregateSubnets(slot) []int`** — subnets this node aggregates this slot (it appears in
  `sync_aggregators[slot][i]`). Mirrors `AggregateSubnets(slot)` (`:185`).

**members = subscribers, one subnet each, one message each** (unlike attestations, where plain
attesters fan-out without joining and a node emits k-per-validator): a sync member subscribes its
one subnet, so there is **no separate backbone, no per-slot dialing, and no k-multiplicity**. This
makes sync markedly *simpler* than attestations on every axis.

---

## 3. Topology — `sync_subnets` persistent meshes

Each sync subnet's subscribers must form a connected subgraph (so its mesh forms and a message
disseminates), the project's standing "connectivity by construction, not chance" rule
(`attestation-spec.md §3`). The 4 subnets are **fat** (≈`sync_size/sync_subnets` members; 128 at
mainnet, ~12.8% of nodes at N=1000) so they connect easily — but at large N or tiny test N the
guarantee still comes from construction.

Both builders add a **third** per-subnet spanning-tree loop (after the attestation-subnet and
column-subnet loops), over `sync_subscribers`:
- `netsim.discv5Graph` (`netsim/subnet.go:35`): add the loop after the `ColumnSubscribers` loop.
- `simctl.generate_subnet_topology` (`simctl/topology.py:272`): add the loop after the
  `column_subscribers` `random_tree` loop, before `fill`-to-K.

Reuses `random_tree` / `randomTree` and `fill` unchanged. No `per_subnet_floor` top-up needed (the
membership *is* the subscriber set; a subnet is never emptier than its members).

---

## 4. Emit — two paths, both reuse

### 4a. Sync message — block-seen coupling (un-gated), rides the attestation trigger

Sync messages vote the head block, emitted at `min(block_seen, deadline)` — **Prysm's exact rule**
(`waitUntilAttestationDueOrValidBlock`, the same fn attestations use). Reuses the existing
block-seen state and `emitDecision`, but with **un-gated** inputs (block-seen only — *no* column
gate, since Prysm does not gate the sync vote on DA):

- `slotState` (`driver/runner.go:41`) gains `syncSubnet int` + `syncMember bool` (set at `beginSlot`
  from `view.SyncSubnet()`) and a `syncEmitOnce`.
- On block receipt (`onBlockProcessed`, `:283`) and at the deadline (`onDeadline`, `:359`), attempt
  the sync emit alongside the attestation emit, feeding `emitDecision(seen, seenAt, deadline, prep)`
  — note `seen`/`seenAt`, **not** the column-gated `ready`/`laterOf(...)` the attestation uses.
- `emitSyncMessage(slot, ss, votedOrigin)` mirrors `emit` (`:383`) but emits **one** message: under
  `syncEmitOnce`, if `syncMember`, publish `MakeSyncMessage(slot, syncSubnet, self, votedOrigin)` on
  `SyncMessageTopic(syncSubnet)` and record `SyncMessageID(slot, syncSubnet, self)` with the head-vote bool.
- **Deadline = `attestation_due_ms`** (reused; same instant on mainnet, t≈4 s). No new knob.

**The contrast metric falls out free.** When the column gate is on, a node with the block but a
missing custody column votes **prior** on its attestation yet **head** on sync. `fraction_voted_head
(sync) − fraction_voted_block (attestation)` = the head-votes lost purely to missing columns — the
DA gate's effect, isolated. When columns are off the two coincide.

### 4b. Sync contribution — fixed deadline, an aggregate twin

`SubmitSignedContributionAndProof` waits a fixed `ContributionDueBPS` (Prysm; not block-coupled) —
identical in shape to the current aggregate phase. Mirror `emitAggregate` (`:406`):
- `slotState` gains `syncAggSubnets []int`, `syncAggTimer *time.Timer`, `syncAggEmitOnce`.
- `beginSlot` (`:126`) arms `syncAggTimer` at `slotStart + sync_contribution_due` and sets
  `syncAggSubnets = view.SyncAggregateSubnets(slot)`.
- `emitSyncContribution(slot, ss)` mirrors `emitAggregate`: one `MakeSyncContribution(slot, subnet,
  self)` per `syncAggSubnets` on the global `SyncContributionTopic`, recorded `SyncContributionID(...)`.
- **Deadline = `aggregate_due_ms`** (reused; t≈8 s). No new knob.

### Bring-up (`Prepare`, `:86`) + phase gating

Sync is **flag-gated exactly like attestations.** `NewRunner` (`driver/runner.go:68`) gains a
`sync bool` next to `attest`, set from `SyncConfig.enabled`; `r.sync` gates the sync `Prepare` block
and the sync emit. Beyond the phase flag, **a node emits sync only if it is a member** — the emit is
guarded by `syncMember`, so a non-member is silent on sync even when the phase is on, exactly as a
non-attester emits no attestations. (Membership is the per-node gate; `r.sync` is the per-run gate.)

`Prepare` adds a sync block (under `r.sync`) mirroring the attestation block: subscribe the **global
`SyncContributionTopic`** (every node downloads all contributions, like aggregates) and — if the node
is a member — its one **`SyncSubnet()`**. The shared block-seen/deadline timer arms when
**`attest || sync`**, so a sync-only run (attestations off) still gets the block-driven emit. The
block-only fast path (`sched == nil`) is unchanged.

---

## 5. Wire format & topics

```proto
// pb/sync.proto  (proto3, package block, same go_package as pb/attestation.proto)
message SyncMessage {          // → 144 B (SyncCommitteeMessage, slot-messages.md §5)
  uint32 slot         = 1;
  uint32 subnet       = 2;     // subcommittee 0..sync_subnets-1
  uint32 origin       = 3;     // publishing member node (gossip origin; loopback skip; the identity)
  uint32 voted_origin = 4;     // head origin, or PriorHead sentinel (reuse validator.PriorHead)
  bytes  payload      = 5;     // random filler to 144 B (sizedFiller)
}
message SyncContribution {     // → 360 B (SignedContributionAndProof, slot-messages.md §5)
  uint32 slot   = 1;
  uint32 subnet = 2;           // subcommittee (distinctness; topic is global)
  uint32 origin = 3;           // aggregator node (distinct ⇒ no gossip dedup)
  bytes  payload = 4;
}
```

- Append `sync.proto` to `pb/doc.go` go:generate; regenerate.
- **`validator/sync.go`** (new), mirroring `attestation.go` + `aggregate.go`:
  `SyncMessageTopicPrefix = "/eth2/00000000/sync_committee_"`, `SyncMessageTopic(i)`,
  `SyncMessageSize = 144`, `MakeSyncMessage(slot, subnet, origin, votedOrigin)`;
  `SyncContributionTopic = "/eth2/00000000/sync_committee_contribution_and_proof/ssz_snappy"`,
  `SyncContributionSize = 360`, `MakeSyncContribution(slot, subnet, origin)`. Reuse `sizedFiller`
  and `PriorHead`.
- **`node/node.go`:** `KindSyncMessage Kind = 5`, `KindSyncContribution Kind = 6` (near `:34`);
  `decode` (`:364`) two cases by topic prefix → the new kinds.

---

## 6. Verify — reuse the per-node batched verifier (no new component)

Sync messages are a t≈4 s flood and contributions a t≈8 s flood — the same M/D/1 queue the
attestation/aggregate verifier already models. The node has **one** batched verifier (one CPU);
the sync floods must **join that queue**, not get their own — else we understate the t≈4 s burst
(now attestations **and** sync messages contend for the same core, which is the point).

- Extend `batchedTopic` (`node/node.go:221`) to also return true for
  `HasPrefix(topic, SyncMessageTopicPrefix)` and `topic == SyncContributionTopic`. That alone routes
  both through `verifier.submitAndWait` in `registerVerifyHook` (`:235`) — **no new case, no new
  semaphore, no new flags.** (Contrast: columns needed the width-P semaphore because of the 128-wide
  t=0 burst; sync has no such burst.)

---

## 7. Metrics & output

- **`metrics.SyncMessageID(slot, subnet, origin)`** mirrors `ColumnID` (`metrics/tracer.go`):
  `Kind=KindSyncMessage, Subnet=subnet, Attester=-1, Origin=origin` (one message per member, so
  `origin` alone is the key); carries the head-vote bool via `OnPublish(..., votedBlock, at)`.
- **`metrics.SyncContributionID(slot, subnet, origin)`** mirrors `AggregateID`:
  `Kind=KindSyncContribution, Subnet=subnet, Attester=origin, Origin=-1`.
- No `MsgID`/CSV schema change (the `kind`/`subnet`/`attester` columns already carry it).
- **Headline metrics:** per-subnet sync-message arrival CDF + coverage; the global contribution CDF;
  **`fraction_voted_head`** over `KindSyncMessage` (generalize `FractionVotedBlock`, or a sibling) —
  reported next to the attestation `fraction_voted_block`, the two together isolating the DA gate's
  effect (§4a).
- **`analysis/check_arrivals.py`:** `analyze_sync` (per-subnet coverage of `sync_subscribers[i] \
  {publisher}`, no-leak, missing, dup, CDF, fraction-voted-head) mirrors `analyze_attestations`;
  `analyze_sync_contributions` (global N−1 coverage, dedup) mirrors `analyze_aggregates`; a
  `load_sync_subscribers` loader reads `sync_subscribers` from `schedule.json`. Kinds `5`/`6` added to
  the constants + `main`/`compare` wiring.

---

## 8. Config surface

A `sync:` block on `SimConfig` (mirrors `AttestationConfig`/`DataColumnsConfig`,
`simctl/config.py`; present+enabled ⇒ run the sync phase):

```yaml
sync:
  enabled: true
  size: 512               # sync_size = member nodes (tests: 8); asserted ≤ N
  subnets: 4              # sync_subnets / subcommittees (tests: 2)
  target_aggregators: 16  # per subnet → contributions (tests: 2; clamped to members)
```

- Deadlines **reuse** `attestation.attestation_due_ms` (messages, t≈4 s) and
  `attestation.aggregate_due_ms` (contributions, t≈8 s) — no new deadline knobs; same instants as
  mainnet.
- Validation (`_check_sync`, mirroring `_check_data_columns`): requires the `attestation` block
  present **only** for those reused deadline knobs (sync needs no `V`); `size ≤ N`; `subnets ≤ size`.
  A sync run with `attestation.enabled=false` is allowed (disseminate+measure sync, no attestations) —
  the coupling arms for sync regardless (§4).

---

## 9. Build / test milestones (TDD — each a failing test first, green before the next)

Mirrors the data-columns/attestation milestone style: **exact counts from a seeded assignment**.

1. **`pb.SyncMessage` + topic + decode + size.** A member publishes one sync message on one subnet;
   a subscriber decodes it; arrival recorded. Size unit test (144 B, non-zero filler) and topic
   format. *(mirror `validator/attestation_test.go`, `driver/columnonly_test.go`.)*
2. **Membership.** `simctl/schedule.py` emits `sync_subscribers` (stable, node-based) + per-slot
   `sync_aggregators`; `schedule.View` exposes `SyncSubnet` / `SyncSubscribersOf` /
   `SyncAggregateSubnets`. Assert: `Σ |sync_subscribers[i]| = sync_size`; subnets even
   (`≈ sync_size/sync_subnets` each); each member in exactly one subnet; `size ≤ N` guard fires when
   violated; aggregators ⊆ subscribers, count clamped; same-seed-identical / seed+1-differs. Go in
   `schedule/schedule_test.go`, Python in `tests/test_schedule.py`. *(mirror the column membership tests.)*
3. **Coverage / no-leak.** Each member publishes one message on its subnet at the deadline; every
   other member of subnet `i` receives it exactly once; **no non-member receives**. `got == want`
   set-equality both directions. *(mirror `assertCoverageNoLeakage` / `column_coverage_test.go`; new
   `driver/sync_coverage_test.go`.)*
4. **The coupling (forced flip).** Suppress the block to one member past the deadline → its sync
   message votes **prior**, the others **head**; `fraction_voted_head == (members−1)/members`.
   Deterministic in the bubble. *(mirror `driver/coupling_network_test.go`; new
   `driver/sync_coupling_test.go`.)* Plus the **un-gated contrast**: with columns on, drop one custody
   column to a member that *has* the block → its attestation votes prior but its sync votes head.
5. **Contributions.** Aggregators emit distinct contributions on the global topic at the contribution
   deadline; each reaches `N−1` (loopback skip); no dedup collisions; publish exactly at
   `slotStart + aggregate_due`. *(mirror `driver/aggregate_test.go`; new `driver/sync_contribution_test.go`.)*
6. **Full run + metrics.** A sized simnet run writes the per-subnet sync arrival CDF +
   `fraction_voted_head` + the contribution CDF; Python `analyze_sync` / `analyze_sync_contributions`
   confirm coverage/no-leak. **Race-skip** (extend `raceEnabled`) if sized. *(mirror
   `driver/column_fullrun_test.go`, `tests/test_check_arrivals.py`.)*

**Sizing for M6 (knobs direct, `size ≤ N`):** `N=16`, `sync_size=8, sync_subnets=2,
target_aggregators=2` (8 of 16 nodes are members, 4 per subnet) — keeps per-slot deliveries in the
low hundreds (the proven-safe band). Run twice in one process (two bubbles, same seed) →
byte-identical sorted arrivals (determinism guard).

---

## 10. Defaults

| Knob | Default | Tests | Source / note |
|---|---|---|---|
| `sync.size` | 512 | 8 | member nodes; `SYNC_COMMITTEE_SIZE`-scale; ≤ N |
| `sync.subnets` | 4 | 2 | `SYNC_COMMITTEE_SUBNET_COUNT` |
| `sync.target_aggregators` | 16 | 2 | `TARGET_AGGREGATORS_PER_SYNC_SUBCOMMITTEE`; clamp to subscribers |
| sync message size | 144 B | — | `SyncCommitteeMessage` |
| contribution size | 360 B | — | `SignedContributionAndProof` |
| message deadline | `attestation_due_ms` (4 s) | — | shared instant; no new knob |
| contribution deadline | `aggregate_due_ms` (8 s) | — | shared instant; no new knob |
| verify | shared batched verifier | — | no new params |

---

## 11. Decisions (all answered)

- **Scope:** messages **and** contributions.
- **Coupling:** sync messages = **block-seen** `min(block_seen, deadline)` (Prysm
  `waitUntilAttestationDueOrValidBlock`); contributions = **fixed deadline** (Prysm
  `ContributionDueBPS`). **No DA/column gate** on sync (un-gated vote — yields the contrast metric).
- **Knobs:** independent `size`/`subnets`/`target_aggregators`; mainnet 512/4/16; small in tests;
  `size` = member nodes, `size ≤ N`, `subnets ≤ size`.
- **Membership:** **node-based** — a seeded subset of `size` nodes are members, each a per-node
  `sync_committee_member` + one `sync_committee_subnet` (stable for the run; no period rotation, no
  validator layer, no k-per-node). Members **subscribe** their one subnet (no backbone, no per-slot dial).
- **Verify:** reuse the per-node **batched verifier** — sync floods join the attestation queue (one
  CPU). No new component.
- **Gating:** sync is **flag-gated like attestations** — a `sync bool` on the runner (from
  `sync.enabled`) is the per-run gate; per-node, a node emits only if it is a member (`syncMember`).
  The shared deadline timer arms on `attest || sync`.
- **Deadlines:** reuse `attestation_due_ms` / `aggregate_due_ms` (same instants on mainnet).
- **Size:** message 144 B, contribution 360 B.

---

## 12. Implementation map (files, smallest-first — matches §9)

- **`pb/sync.proto`** (new) + `pb/doc.go` go:generate; regenerate `pb/sync.pb.go`.
- **`validator/sync.go`** (new): topics, sizes, `MakeSyncMessage` / `MakeSyncContribution` (reuse
  `sizedFiller`, `PriorHead`).
- **`node/node.go`:** `KindSyncMessage=5` / `KindSyncContribution=6`; `decode` cases (`:364`);
  `batchedTopic` += the two sync topics (`:221`). **No verifier change.**
- **`schedule/schedule.go`:** `Assignment.SyncSubscribers [][]int` + per-slot
  `SlotPlan.SyncAggregators [][]int` (json tags); `View.SyncSubnet` / `SyncSubscribersOf` /
  `SyncAggregateSubnets`.
- **`simctl/schedule.py`:** generate `sync_subscribers` (node-based, `_rng(seed,6)`, stable) +
  per-slot `sync_aggregators` (`_rng(seed,7,slot)`); `to_dict` keys; `Params` fields; assert `size≤N`.
- **`simctl/topology.py`:** third per-subnet tree loop over `sync_subscribers` (`generate_subnet_topology`, `:272`).
- **`netsim/subnet.go`:** `discv5Graph` (`:35`) loop over `a.SyncSubscribers`.
- **`simctl/config.py`:** `SyncConfig` block on `SimConfig`; `_check_sync` (requires attestation for the deadlines; `size≤N`).
- **`driver/runner.go`:** a `sync bool` field (mirrors `attest`) gating all of the below; `slotState`
  `syncSubnet int` + `syncMember bool` + `syncAggSubnets/syncAggTimer/syncAggEmitOnce`; `Prepare`
  (`:86`) subscribe `SyncContributionTopic` + (if member) the node's sync subnet; `beginSlot` (`:126`)
  arm the contribution timer + set `syncSubnet`/`syncMember`; un-gated single-message sync emit from
  the block-receipt + deadline paths; `emitSyncMessage` (one-message twin of `emit`, `:383`) +
  `emitSyncContribution` (twin of `emitAggregate`, `:406`); `onReceive` (`:248`)
  `KindSyncMessage`/`KindSyncContribution` trace cases. **`driver/driver.go`:** thread a `sync bool` +
  the sync knobs into `Config`/`NewRunner` (mirroring `attest`); arm the shared deadline timer on
  `attest || sync`.
- **`metrics/tracer.go`:** `SyncMessageID` / `SyncContributionID`; generalize `FractionVotedBlock`
  (or a `fraction_voted_head` sibling).
- **`analysis/check_arrivals.py`:** `analyze_sync` / `analyze_sync_contributions` + `load_sync_subscribers`
  + kinds 5/6; `main`/`compare` wiring.
- **`cmd/slot-sim-node/main.go`:** a `-sync` enable flag (deadlines reuse the `-att-due`/`-agg-due`
  values; **no** `-sync-verify-*` flags — sync shares the batched verifier); read sync membership from
  `schedule.json`. **`simnetrun/run_test.go`:** a `sync` enable key (size/subnets/aggregators live in
  `schedule.json`). **`simctl/runner.py`:** thread the sync config into Params / host args / simnet params.

---

## 13. Open implementation risks

- **Shared deadline-timer arm (resolved, not a risk).** Generalizing the arm condition from `attest`
  to `attest || sync` touches the one shared control path, but it is the intended design: sync is its
  own flag, and each phase emits only for the duties/membership a node holds. M4/M6 verify a sync-only
  run and an attest-only run each arm + emit independently.
- **Thin sync subnets at extreme N.** At `N ≫ sync_size` a subnet has `≈ sync_size/sync_subnets`
  member nodes; the per-subnet tree keeps it connected (like columns), and coverage is computed from
  the actual `sync_subscribers` set, so counts stay exact. (Node-based membership means a subnet is
  exactly its member set — no collision subtlety; each member emits one message, keyed by `origin`.)
- **One batched verifier, two more floods.** Sync messages + contributions now share the node's single
  verify queue with attestations/aggregates — intended (one CPU), but it raises per-node t≈4 s / t≈8 s
  queueing; M6 should sanity-check the arrival tail doesn't blow past the slot.

---

## 14. Summary

Each slot, a seeded subset of `sync_size` **nodes** (the sync committee, stable for the run, each on
one of `sync_subnets` subnets) emit one 144 B `SyncMessage` on their subnet at `min(block_seen,
deadline)` — Prysm's exact attestation rule, reusing `emitDecision` with an **un-gated** block-seen
vote — and `target_aggregators` per subnet emit one 360 B `SyncContribution` on a single **global**
topic at the fixed contribution deadline, an aggregate twin. Membership is **node-based** (a per-node
flag + one subnet, no validator layer, one message each) and members **subscribe** their subnet, so
there is no backbone and no per-slot dialing; a third per-subnet tree in both topology builders keeps
the meshes connected. The sync floods join the existing per-node **batched verifier** (one CPU) — no
new verify component. The headline output is the per-subnet sync arrival CDF, the global contribution
CDF, and `fraction_voted_head`, which next to the column-gated `fraction_voted_block` **isolates the
DA gate's effect**. It is built as exact counts from a seeded assignment, the membership proven before
any network and the coupling proven by forcing one member's block past the deadline.
