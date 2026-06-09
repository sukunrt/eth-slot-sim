# Data columns — dissemination + custody gate (simnet/shadow)

Implementation-ready spec. Implements `plan.md` §6 **Phase 2 (Data columns)**, narrowed to
**gossip dissemination + custody + the attestation gate**. Each slot the proposer publishes the
block **and** one `DataColumnSidecar` on each of `num_columns` column subnets at t=0; nodes receive
the columns on the subnets they **custody**; a node votes for the block only once it has the block
**and all its custody columns** by the attestation deadline. We record per-node column-arrival time
and the gate outcome.

This rides the machinery already built for the block (global proposer publish at t=0) and for
attestations (`att-subnet.md` / `attestation-spec.md`: stable centrally-generated subscribe sets in
`schedule.json`, per-node verify queues, the block→attestation vote coupling in `driver/`). Columns
are "block-like origination on attestation-like subnets," with one new idea — **custody** — and one
new coupling — columns **gate** the attestation.

> All decisions are locked in §12. This spec assumes the attestation/aggregate phase is already
> implemented (it is): `schedule/`, `driver/coupling.go`+`runner.go`, `validator/attestation.go`,
> the per-node batched verifier (`node/verifier.go`), the subnet-aware graph (`netsim/subnet.go`,
> `simctl/topology.py`), and the receipt/CDF analysis (`analysis/check_arrivals.py`).

---

## 0. Non-goals (this cut)

PeerDAS **req/resp sampling** (`SAMPLES_PER_SLOT=8` over unicast) — deferred; the gate is
custody-completeness only (§7). **Reconstruction / cross-seeding** (a node with ≥50% columns
rebuilding the rest) — deferred. The **realistic validator→node custody skew** (custody varying per
node by stake) — deferred to the `data-column-custody-distribution` todo; this cut is **uniform**
custody. We **require `V ≥ N`** so every node runs ≥1 validator, which makes custody uniformly 8 for
every ordinary node and lets us drop the mainnet "non-validating node custodies 4" branch entirely
(it returns with the skew todo). Crypto / real KZG — verification is validation-as-sleep, as
everywhere else.

---

## 1. Reuse vs. new

**Reuse — as-is:**
- The **proposer publish** path. The per-slot proposer is a supernode read from `schedule.json`
  (`schedule.Assignment.ProposerSchedule`, `schedule/schedule.go:88`; the `Validator` obeys it,
  `validator/validator.go:58`). It publishes the block at `offset + rand(jitter)`
  (`driver/runner.go:160`, `publishBlock` at `:197`). Columns fire alongside, in the same burst.
- The **subnet subscribe set + stable membership** pattern: a seeded, centrally-generated
  `subnet → subscribers` map in `schedule.json` (`simctl/schedule.py:_subnet_subscribers`,
  `:123`), read by both backends; `netsim.discv5Graph` (`netsim/subnet.go:35`) and
  `simctl.generate_subnet_topology` (`simctl/topology.py:272`) build a per-subnet-connected graph
  from it. Columns add a **second** such map, `column_subscribers`.
- The **subscribe-at-bring-up** wiring: `NodeRunner.Prepare` (`driver/runner.go:77`) subscribes a
  node's stable meshes before slot 0; columns add the node's custody columns there.
- **Metrics** `Tracer` (`OnPublish`/`OnReceive` + a `MsgID`, `metrics/tracer.go:25`) and
  `analysis/check_arrivals.py`.

**New:**
- A **custody model** (§2) + a `full_custody` set (the data-column supernodes).
- A `column_subscribers` map and the **full-custody backbone** that makes `num_columns` thin subnets
  relayable at small N (§3).
- A `pb.Column` wire type + `KindColumn` decode + the `data_column_sidecar_{i}` topics (§5).
- A **bounded-concurrency column verifier** (§6). This is a **genuinely new** per-node component — a
  width-`P` semaphore — **not** a generalization of `node/verifier.go` (which is a batch-window
  accumulator for the attestation/aggregate flood and stays exactly as-is). The block keeps its
  fixed per-hop hook. So the node ends up with **three** verify classes.
- The **column→attestation gate** in the `NodeRunner` coupling (§7) — the one real behavior change.

---

## 2. Custody model

**Custody** = how many of the `num_columns` column subnets a node subscribes to (joins the mesh of)
and therefore receives + relays. In PeerDAS it scales with the stake a node runs; this cut holds it
**uniform**:

| Node class | Custody | Set by |
|---|---|---|
| ordinary node (every node, since `V ≥ N` ⇒ each runs ≥1 validator) | **8** (`custody_floor`) | floor |
| **full-custody node** (a data-column supernode) | **all `num_columns`** | the full-custody set |

- **Which 8** an ordinary node holds: a **seeded-random `custody_floor`-subset** of `num_columns`,
  per node — exactly mirroring `_subnet_subscribers` (`simctl/schedule.py:123`). Mainnet derives
  them from node-id; for dissemination timing only the subscribe-set *shape* matters, so
  seeded-random is equivalent, simpler, and reproducible across both backends from one seed.
- **The full-custody set** is a **subset of the 1 Gbit bandwidth-supernodes** (full custody needs the
  pipe). Sized by `full_custody_fraction` (§3). So full-custody ⊆ bandwidth-super ⊆ all nodes, and a
  column run requires `topology.super_node_fraction > 0`.
- **`V ≥ N` is asserted at startup.** It guarantees every node is a validating node (custody 8), so
  there is no "custody 4" non-validating branch in this cut.
- The skew distribution (home stakers→8, big operators→128 by stake) replaces "uniform 8" later — the
  `data-column-custody-distribution` todo. Nothing below changes when it lands; only the per-node
  custody **count** stops being constant.

---

## 3. Topology — column subnet membership & the backbone

`num_columns` (default 128) column subnets, **all active every slot** (unlike committees, the block
always erasure-codes into all of them — there is no "C active subset" for columns).

**`column_subscribers[i]` = the nodes custodying column `i`** = every full-custody node (all of them,
since they custody all columns) ∪ the ordinary nodes that drew `i`. Generated once in
`simctl/schedule.py`, seeded, identical on both backends, carried in `schedule.json` next to
`subnet_subscribers`. The **full-custody set itself is also carried explicitly** in `schedule.json`
(see the cross-backend note below).

**The thin-mesh problem and its fix.** Uniform custody-8 over `N` nodes gives only
`8N/num_columns` subscribers per subnet on average (~1.6 at N=25, num_columns=128) — too thin to
relay. The **full-custody nodes are the backbone**: each sits in *every* column subnet, so `F`
full-custody nodes give every subnet an `F`-node connected core that the ordinary nodes attach to.
This is the realistic DA backbone, and it's why full-custody is a **subset knob** rather than
incidental.

The three backbone knobs, in plain English:
- **`full_custody_fraction` (default 0.5)** — of the 1 Gbit supernodes, what share also hold *all*
  `num_columns` columns and act as the relay backbone. `F = round(full_custody_fraction · n_super)`.
- **`column_backbone_floor` (default 3)** — a safety check: there must be at least this many
  full-custody nodes, so every column subnet has a few relayers. If `F < column_backbone_floor` the
  run **errors at generation** (rather than silently producing a disconnected column network).
- **`per_subnet_floor` (default 0 = off)** — an *optional* top-up that would sprinkle a few random
  custodiers into any thin subnet. Left off because the backbone already covers every subnet; a spare
  safety valve, mirroring the attestation `subscribe_floor` top-up (`simctl/schedule.py:133`).

**Graph construction.** Both builders add a second loop over `column_subscribers`, building a random
spanning tree over each column's custodiers (so each column's custodiers are one connected piece),
on top of the existing global tree + per-attestation-subnet trees + fill-to-K:
- `netsim.discv5Graph` (`netsim/subnet.go:35`): add the loop after the `SubnetSubscribers` loop
  (`:46`).
- `simctl.generate_subnet_topology` (`simctl/topology.py:272`): add the loop after the subnet
  `random_tree` loop (`:297`).
- Full-custody nodes land in all `num_columns` column trees → **high degree** (intended; they have
  the pipe). `fill` (`simctl/topology.py:158`, `netsim/graph.go`) caps *other* nodes at K; a
  full-custody node already above K simply gets no fill and sits above K.

**Cross-backend note (important).** Go's `pickSupernodes` (`netsim/netsim.go:258`, a PCG shuffle) and
Python's `supernode_ids` (`simctl/topology.py:121`, node-0-pinned Bernoulli draws) are **not
bit-identical**. The supernode set and the full-custody subset are therefore **sourced from
`schedule.json`** (Python-generated) and **not re-derived in Go** — otherwise a full-custody node
could be handed a 25/50 Mbit pipe. (Cross-backend runs already share one `topology.json`, whose
bandwidth classes encode the supernode set; `NewFromTopology` infers them from bandwidth,
`netsim/netsim.go:101`.)

**Proposer.** Must be a full-custody node (it originates all columns). `simctl/schedule.py`'s
`generate` draws proposers from the **full-custody subset** instead of from all supernodes
(today it draws from `supers`, `:118-119`). A proposer, being full-custody, is already subscribed to
all column meshes at bring-up (`Prepare`), so it publishes on its own meshes — **no per-slot dialing**
(unlike attestation fan-out).

---

## 4. Proposer publish — the t=0 column burst

At its block-publish instant (`slotStart + offset + rand(jitter)`), the proposer publishes the block
on the global topic **and** one `DataColumnSidecar` on each of the `num_columns` column topics,
back-to-back — a burst of `num_columns` publishes on meshes it already belongs to. **Columns share
the block's instant**; a separate column offset stays a future knob (to model the builder→proposer
relay delay separately if ever wanted). Wired in `beginSlot` (`driver/runner.go:108`) alongside the
existing block dispatch (`:160`).

The burst of `num_columns` columns is what hits the proposer's immediate full-custody peers all at
once — the case the §6 verifier parallelism is calibrated against.

---

## 5. Wire format & topic

```proto
// pb/column.proto  (mirrors pb/attestation.proto: proto3, package block, same go_package)
message Column {
  uint32 slot    = 1;
  uint32 column  = 2;   // column / subnet index, 0 .. num_columns-1
  uint32 origin  = 3;   // proposer node (gossip origin; for the loopback skip)
  bytes  payload = 4;   // random filler so the marshaled message ≈ B*2144 + 356 bytes
}
```

- Append `column.proto` to the `//go:generate protoc …` line (`pb/doc.go:4`) and regenerate.
- **Size** = `B·2144 + 356` B (`slot-messages.md §5`). Default **`B=6` → 6·2144+356 = 13,220 B
  ≈ 12.9 KiB**. `B` is a knob (the blob-count roadmap 9 → 72 grows columns to ~151 KiB). At `B=6`
  (< 16384 B) the existing `sizedFiller` (`validator/attestation.go:63`) works; for the large-B
  roadmap, size the payload directly like `makeBlock` (`validator/validator.go:77`) — see §14.
- **Topic** `/eth2/00000000/data_column_sidecar_{i}/ssz_snappy`, `i` = 0..num_columns-1. New
  `ColumnTopicPrefix` + `ColumnTopic(i)` in a `validator/column.go` (mirrors
  `validator.AttestationTopicPrefix`/`AttestationTopic`, `validator/attestation.go:15,27`). Decode by
  prefix → `KindColumn` in `node.decode` (`node/node.go:336`), exactly like `KindAttestation`;
  `KindColumn Kind = 4` added near `node/node.go:34`.

---

## 6. Verify hook — bounded-concurrency column verifier

Per `scaling.md §7` (validation-as-sleep on the critical path every hop). Research on real KZG cost
(`verify_cell_kzg_proof_batch` over the column's `B` cells; generation ~150–200 ms/blob, verification
far cheaper; batch amortizes the fixed pairing cost):

- **Per column ≈ 3 ms** at `B=6` (fixed ~2 pairings dominate). `verify_service_ms = 3` — calibrate
  later.
- **It's per core.** A node has `P` parallel verify slots; a burst of `c` columns clears in
  `ceil(c/P)·service`. The component is a **new per-node width-`P` semaphore** (one per node, shared
  across all `num_columns` column topics): each arriving column acquires a slot, sleeps `service`,
  releases. Keyed by `ColumnTopicPrefix` in `registerVerifyHook` (`node/node.go:213`), making it
  three-way: column prefix → the semaphore hook; attestation/aggregate → the existing batched verifier
  (`batchedTopic`, `:206`, **unchanged, columns are not added to it**); everything else (block) →
  the fixed per-hop sleep. Built in `JoinTopics` (`node/node.go:131`), sized from the node's
  full-custody flag.
- **Defaults:** `P=16` for a **full-custody** node, `P=4` for everyone else → a full-custody node's
  128-column t=0 burst clears in ~24 ms, an ordinary custody-8 node's 8 in ~6 ms. Both knobs.

A plain fixed-per-hop sleep is rejected: gossipsub's validator pool would verify all 128 columns of a
full-custody burst concurrently (~3 ms wall), which one CPU cannot do — the full-custody node is the
backbone and verifies the most, so its serialization sets the column-arrival tail. (See §14 for the
gossipsub-concurrency-vs-`P` check.)

---

## 7. The gate — columns gate the attestation

Today the coupling (`driver/coupling.go:emitDecision`, `:11`; driven from
`driver/runner.go:onBlockProcessed` `:238` / `onDeadline` `:265` / `emit` `:279`) votes the block
when it was processed by the attestation deadline, else prior head. Columns add a second condition:

> **Vote block iff, by the attestation deadline, the node has (a) processed the block AND (b)
> received every column on the subnets it custodies. Else vote prior head.**

`slotState` (`driver/runner.go:40`) gains:
- `custody []int` — this node's custody subnets this slot (its `column_subscribers` membership; for a
  full-custody node, all `num_columns`).
- `haveColumn map[int]bool` + a count — set as columns arrive.
- `columnsComplete bool` — `count == len(custody)`.

Wiring:
- `onReceive` (`:209`) `KindColumn` case: skip own loopback (`origin == self`),
  `tracer.OnReceive(ColumnID(...))`, mark `haveColumn`, and if it completes custody re-run the vote
  decision (it may unblock an early block vote). **A column counts the moment its verify-sleep
  finishes** — consistent with block-seen, because the topic validator gates local delivery, so the
  `onReceive` timestamp is already post-verify. This is what lets the §6 verifier push a late column
  past the deadline and flip a vote.
- The decision rule extends to require **block processed AND `columnsComplete`** for an early
  block-vote. Concretely, the early emit fires at `max(block_processed, columns_complete)` (plus
  `Δ_prep`), capped at the deadline; both the block-receipt path and the columns-complete path attempt
  it, and `emitOnce` still guarantees a single emission. At the deadline, `onDeadline` re-derives the
  vote from `seen && columnsComplete`. A node with the block but missing a custody column at the
  deadline votes **prior head** — the measurable effect of column dissemination on attestation.
- The **proposer** has all columns at publish (it made them) → `columnsComplete` immediately, as with
  self-block-seen.

**v1 simplification:** real DA needs custody **+ sampling success**; with sampling deferred, the gate
is **custody-completeness only**. A node that custodies 8 votes on those 8 arriving — it does not
also require the 8 sampled columns. A known gap (closes when sampling lands).

**Coupling scope:** the gate is active only when **both** the column and attestation phases run. A
columns-only run (no attestations) just disseminates + measures columns (§8), no gate — mirroring how
`attestation.enabled=false` keeps the schedule but emits no attestations.

---

## 8. Metrics & output

Per `scaling.md §9`, the column subset:
- **Column-arrival time** per node per `(slot, column)` over the column's subscriber set:
  `ColumnID(slot, column, origin)` reuses the existing `MsgID` (`metrics/tracer.go:25`) — column index
  in the `Subnet` field, `Attester = -1`, `Origin =` proposer; `Kind = KindColumn`. No struct change;
  the CSV already carries `kind`/`subnet`. Output the per-column arrival CDF (`p50/p90/p99/p100`).
- **Gate outcome** per `(node, slot)`: a per-slot **custody-complete rate** (fraction of custodiers
  that had all their custody columns by the deadline), reported **alongside** the existing
  `FractionVotedBlock` (`metrics/tracer.go:135`). Together they attribute a prior-head vote to a
  missing column vs. a missing block — the headline the gate exists to measure.
- **Per-node column ingress bytes** (`custody · (B·2144+356)`) — optional, `scaling.md §6`.
- `analysis/check_arrivals.py`: add `ColumnResult` + `analyze_columns` / `analyze_columns_csv`
  mirroring `analyze_attestations`: every custodier of column `i` received it (coverage); no
  non-custodier did (no-leak); per-column + aggregate CDFs. Load `column_subscribers` from
  `schedule.json` (mirroring `load_committee`).

---

## 9. Config surface

A `data_columns:` block on `SimConfig` (mirrors `AttestationConfig`, `simctl/config.py:36`; present ⇒
run the column phase):

```yaml
data_columns:
  enabled: true
  num_columns: 128            # all active each slot (tests: 32)
  blobs: 6                    # B → column size B*2144+356 ≈ 12.9 KiB
  custody_floor: 8            # columns an ordinary node holds (uniform; skew later)
  full_custody_fraction: 0.5  # share of the 1 Gbit supernodes that are full-custody; F=round(frac*n_super)
  column_backbone_floor: 3    # min F so every subnet has a backbone core (else generation errors)
  per_subnet_floor: 0         # optional thin-subnet backstop (0 = off)
  verify_service_ms: 3        # per-column validation-as-sleep
  verify_parallelism_super: 16    # P for a full-custody node
  verify_parallelism_regular: 4   # P for everyone else
```

- The gate reuses `attestation.attestation_due_ms` (4 s) — columns must arrive by the same deadline;
  **no new deadline**. A columns-only run has no deadline (arrival CDF only).
- Validation: column runs require `topology.super_node_fraction > 0` and `V ≥ N` (asserted).

---

## 10. Build / test milestones (TDD — each a failing synctest test first, green before the next)

Mirrors existing patterns: the `synctest.Test` skeleton + `buildScenario`/`run`/
`assertCoverageNoLeakage` (`driver/shared_test.go:32,76,176`), the forced-flip coupling test
(`driver/coupling_network_test.go:17`), the verifier serialization inequality
(`node/verifier_test.go:286`), and the `-race` skip via the `raceEnabled` const
(`driver/race_test.go` / `norace_test.go`) for sized runs.

1. **`pb.Column` + topic + decode**: a proposer publishes one column on one subnet; a custodier
   receives + decodes it; arrival recorded. Smallest end-to-end (mirror `driver/blockonly_test.go`).
2. **Custody map**: `simctl/schedule.py` emits `column_subscribers` + the full-custody set (uniform-8
   + full-custody backbone); the Go `schedule.View` exposes a node's custody set (`CustodyColumns`);
   assert per-node custody count, that every subnet has ≥ `column_backbone_floor` full-custody nodes,
   and that proposers ∈ full-custody. Go in `schedule/schedule_test.go`; Python in
   `tests/test_schedule.py` (including the `F < floor` → raise case).
3. **Full burst**: proposer publishes all `num_columns` at t=0; every custodier receives its custody
   columns exactly once; **no non-custodier receives** (no-leak); arrival spread is multi-hop
   (`assertColumnCoverageNoLeakage`, mirroring `assertCoverageNoLeakage`).
4. **Column verifier**: a full-custody node receiving `num_columns` at once clears them in
   `ceil(num_columns/P)·service` (assert the serialization), an ordinary custody-8 node in
   `ceil(8/P)·service`. New P-semaphore test mirroring the verifier delay-grows-with-k inequality.
5. **The gate**: suppress one custody column to one node (a test-only `OnReceive` filter, as the
   forced-flip block test does, `coupling_network_test.go`) → that node votes **prior head**; with all
   custody columns by the deadline it votes **block**. Deterministic in the bubble.
6. **Driver run**: a full simnet run writes the per-column arrival CDF + the gate-outcome summary
   (mirror `driver/fullrun_test.go`; race-skip if sized). Plus `tests/test_check_arrivals.py` column
   coverage / missing / leak cases.

---

## 11. Defaults

| Knob | Default | Notes |
|------|---------|-------|
| `num_columns` | 128 (tests 32) | all active each slot |
| `blobs` B | 6 | column ≈ 12.9 KiB; knob (9→72 on the roadmap) |
| `custody_floor` | 8 | uniform; skew → todo |
| `full_custody_fraction` | 0.5 | share of 1 Gbit supernodes that are full-custody; F = round(frac·n_super), validated ≥ floor |
| `column_backbone_floor` | 3 | min F for a per-subnet core; generation errors below it |
| `per_subnet_floor` | 0 (off) | optional thin-subnet backstop |
| `verify_service_ms` | 3 | per-column; calibrate later |
| `verify_parallelism_super / regular` | 16 / 4 | P; full-custody burst ~24 ms, ordinary ~6 ms |
| column publish instant | with the block | same `offset + rand(jitter)` |
| gate deadline | `attestation_due_ms` (4 s) | no new deadline |
| `V` vs `N` | `V ≥ N` (asserted) | every node validating ⇒ uniform custody 8 |

---

## 12. Decisions (all answered)

- **Scope**: gossip dissemination + custody + gate; sampling, reconstruction, and the realistic
  custody skew deferred (§0).
- **Custody**: **uniform** — every ordinary node 8 (require `V ≥ N`, so no non-validating-4 branch),
  full-custody nodes all `num_columns`. The which-8 is **seeded-random per node**.
- **Full-custody / backbone**: a **subset of the 1 Gbit supernodes**, `F = round(full_custody_fraction
  · n_super)`, default `full_custody_fraction = 0.5`, validated `≥ column_backbone_floor = 3` (else
  generation errors). `per_subnet_floor = 0` (off). The full-custody set is carried explicitly in
  `schedule.json` (Go must not re-derive it).
- **Proposer**: a full-custody node (drawn from the full-custody subset), **publishes all columns on
  its own meshes**, no per-slot dialing.
- **Columns-only runs supported** (no gate, arrival CDF only) as well as gated runs; gate active only
  when both phases run.
- **Publish timing**: columns at the block's instant; separate column offset is a future knob.
- **Verify**: a **new bounded-concurrency width-`P` semaphore** verifier (one per node, shared across
  column topics), `service = 3 ms`, `P = 16` full-custody / `4` otherwise — distinct from the block's
  fixed hook and the attestation/aggregate batched verifier.
- **Gate**: vote block iff (block processed **AND** all custody columns received) by the deadline,
  else prior head; a custody column counts **after its verify-sleep finishes**; emit at
  `max(block, columns_complete)` capped at the deadline.
- **Size**: `B = 6`; `num_columns` default 128 (tests 32).

---

## 13. Implementation map (files, smallest-first — matches the §10 milestones)

- **`pb/column.proto`** (new) + append to `pb/doc.go:4` go:generate; regenerate `pb/column.pb.go`.
- **`validator/column.go`** (new): `ColumnTopicPrefix`, `ColumnTopic(i)`, `ColumnSize`/`Blobs`,
  `MakeColumn(slot, column, origin)`.
- **`node/node.go`**: `KindColumn Kind = 4` (`:34`); `decode` case (`:336`); three-way
  `registerVerifyHook` (`:213`) with a per-node column semaphore built in `JoinTopics` (`:131`); do
  **not** touch `batchedTopic` (`:206`).
- **`schedule/schedule.go`**: `Assignment.ColumnSubscribers [][]int`
  (`json:"column_subscribers"`) + `FullCustody []int` (`json:"full_custody"`) + column params
  (`:55`); `View.CustodyColumns()` and `View.ColumnSubscribers(col)` (after `:153`).
- **`simctl/schedule.py`**: generate `column_subscribers` + the full-custody subset (validate
  `F ≥ column_backbone_floor`); draw proposers from full-custody (change `generate`, `:108-120`);
  serialize (`to_dict`, `:59`).
- **`simctl/topology.py`**: `generate_subnet_topology` (`:272`) second loop over `column_subscribers`.
- **`netsim/subnet.go`**: `discv5Graph` (`:35`) second loop over `a.ColumnSubscribers`.
- **`driver/runner.go`**: `slotState` custody fields (`:40`); `onReceive` `KindColumn` (`:209`);
  proposer column burst in `beginSlot` (`:108`); custody subscribe in `Prepare` (`:77`); gate
  extension to `emitDecision` callers (`onBlockProcessed` `:238`, `onDeadline` `:265`). **`driver/driver.go`**:
  thread column knobs through `Config`/`New`, size each node's `P` from full-custody membership.
- **`cmd/slot-sim-node/main.go`**: new `-data-columns` / `-num-columns` / `-col-blobs` /
  `-col-verify-service` / `-col-custody-floor` / P flags (mirror the `-attestations …` block, `:64-89`);
  read custody + full-custody from `schedule.json`. **`simnetrun/run_test.go`**: column params keys.
- **`simctl/config.py`**: `data_columns:` block on `SimConfig` (`:60`); require
  `super_node_fraction > 0` and `V ≥ N`.
- **`metrics/tracer.go`**: `ColumnID(slot, column, origin)`; per-slot custody-complete rate alongside
  `FractionVotedBlock` (`:135`).
- **`analysis/check_arrivals.py`**: `ColumnResult` + `analyze_columns` / `analyze_columns_csv` +
  `column_subscribers` loader; wire into `compare`/`main`.

---

## 14. Open implementation risks

- **Gossipsub validator concurrency vs. `P`.** The `P`-semaphore only serializes if gossipsub invokes
  the column validator for `> P` messages concurrently. `node.Start` (`node/node.go:91`) sets
  `WithValidateQueueSize(600)` but no per-validator concurrency; the go-libp2p-pubsub default
  per-validator concurrency (1024) is ≥ any `P` we use, and the existing batched verifier already
  proves gossipsub runs validators concurrently — so register the column topic validators normally
  and **verify in milestone 4** that the burst actually serializes to `ceil(c/P)·service`.
- **`sizedFiller` varint assumption.** `validator/attestation.go:63` assumes payloads < 16384 B
  (2-byte length varint). `B=6` columns (13,220 B) are fine; for the large-B roadmap (≥16384 B), size
  the payload directly like `makeBlock` (`validator/validator.go:77`) instead of via `sizedFiller`.

---

## 15. Summary

The proposer — a full-custody node — publishes the block plus one `DataColumnSidecar` on each of
`num_columns` subnets in a t=0 burst. Membership is **custody**: ordinary nodes hold a seeded-random 8
(we require `V ≥ N` so every node is validating), full-custody nodes (a fraction of the 1 Gbit
supernodes) hold all and form the relay backbone that keeps the thin subnets connected. Each received
column is verified through a new per-node `P`-server semaphore (~3 ms/core), so a full-custody node's
128-column burst serializes realistically (~24 ms at `P=16`). A node votes for the block only once it
has the block **and all its custody columns** (counted after verify) by the attestation deadline — the
column phase's measurable effect on the slot. Output: the per-column arrival CDF and the gate-outcome
summary (custody-complete rate + fraction-voted-block) across `N` nodes. The realistic custody **skew**
is the next step (`data-column-custody-distribution` todo).
