# Attestation spec — the block→attestation response

Draft spec. Implements `plan.md` §6 step 4: after the block disseminates, nodes that received
it **attest**. Each attester emits one `SingleAttestation` (240 B) on its committee's subnet,
voting for the block **iff it processed the block by the attestation deadline**, else for the
prior head. This is the first **causal edge** in the simulator (one message's content depends on
another) and the first thing that needs the **V/N split** (V validators spread over N nodes).

Builds directly on the Phase-1 stack (`phase1-spec.md`, `current-state.md`): the same
`node`/`validator`/`driver` split, the same simnet (`synctest`) + Shadow backends behind one
network seam. **Aggregates** (the t≈8 s `SignedAggregateAndProof`) are the *next* spec; this one
stops at the attestation, but includes aggregator **selection** because it sets which nodes
subscribe each subnet.

> This is a spec, not code. Sections marked **(open)** are deferred decisions listed in §10.

---

## 0. Non-goals (this stage)

Aggregates (t≈8 s), sync-committee messages, data columns, req/resp sampling, crypto / real BLS,
real SSZ, faithful epoch-based committee rotation, discovery (discv5), hop-tree reconstruction,
mainnet calibration. The seams are built so these slot in later; only the attestation response is
built now. Aggregator **selection** is in scope (it determines subnet membership); what
aggregators *do* with what they collect is not.

---

## 1. What attestations break (vs. the block)

The block phase got away with four simplifications. Each one breaks here — naming them up front
because the rest of the spec is mostly about replacing them.

1. **The recording key stops being unique.** The recording/logging code (`metrics/tracer.go`)
   files every event under the pair `(slot, origin)` — the slot number and which node published
   it (`type pubKey struct{ slot, origin int }`, `tracer.go:49`). With one block per slot that
   pair is unique. With attestations, node 5 can publish ~100 messages in slot 3 — they all
   collide on `(3, 5)` and overwrite each other in the map. **Fix:** widen the key to
   `(slot, kind, subnet, attester)`. Nothing deeper than adding fields (§7).

2. **Publishing is not subscribing.** A plain attester *publishes* to its committee's subnet but
   does **not** join that subnet's mesh — it has no relay duty there. Only a small per-node
   **backbone** (2 subnets) plus the slot's **aggregators** actually subscribe and relay. So a
   subnet message reaches a *subset* of nodes, not all N (§4, §3).

3. **The vote depends on the block.** An attestation votes for the block only if the node
   processed the block in time; otherwise it votes for the prior head. This is "the response" —
   and it forces validators to become first-class (V validators over N nodes), because *which*
   validators attest *which* slot is the structure that drives the load (§2, §5).

4. **Emit time is event-driven, not a fixed offset.** The block publishes at a fixed slot offset.
   An attester emits at `min(block_processed, deadline)` — triggered by *receiving* the block, or
   by the deadline timer, whichever fires first (§5).

### Verified against Prysm

The model below mirrors how Prysm actually behaves (checked against `../prysm`, 2026-06):

- **One node running k validators emits k independent attestations — never combined.**
  `performRoles` (`validator/client/runner.go:239`) ranges over every validator key the node
  holds and spawns a goroutine per duty; for an attester that is `SubmitAttestation(slot, pubKey)`
  (`runner.go:246`). Each `SubmitAttestation` (`validator/client/attest.go:34`) independently
  fetches *that* validator's committee, signs with *that* key, and broadcasts **one**
  `SingleAttestation` carrying a single attesting index (`attest.go:98`). Combining only happens
  later, at aggregation.
- **Subnet is per-committee:** `(committees_since_epoch_start + committee_index) %
  ATTESTATION_SUBNET_COUNT` (`beacon-chain/core/helpers/attestation.go:104`). A node's k
  validators can land in different committees ⇒ different subnets; two in the same committee ⇒
  same subnet, still two messages.
- **The `min(block, deadline)` rule is literally Prysm's** `waitUntilAttestationDueOrValidBlock`
  (`attest.go:262`): a `for { select }` that returns on whichever fires first — a slot-feed event
  signalling the block for this slot was processed, or the deadline timer — with an early-out if
  the block was already seen (`slot <= v.highestSlot()`).

---

## 2. The assignment layer — a pure `committee` package

`plan.md`'s **topology seam** (validators→nodes→committees→subnets→custody), kept entirely
separate from `netsim` (which is the *network* topology: latency, bandwidth, peer graph). Pure,
seeded, no libp2p / no `time` / no `synctest` — unit-testable with plain `go test`, the way
`netsim/topology_test.go` already is. This is the highest-leverage layer: every later
expected-count derives from it, so it is tested exhaustively before any network exists (§8 M0).

### Three independent knobs: `V`, `C`, `s_c`

On mainnet these are tied by `attesters/slot = V/32 = C·s_c`. **This simulator does not enforce
that** — all three are set directly:

| Knob  | Meaning                                              |
|-------|-----------------------------------------------------|
| `V`   | total validators, mapped onto the `N` nodes         |
| `C`   | committees (= active attestation subnets) per slot  |
| `s_c` | committee size = attesters per committee per slot   |

- **Derived, not an input:** attesters/slot = `C·s_c` (the per-slot attestation-message count,
  network-wide). `s_c` per subnet is the **flood** the batched verifier models.
- **Only hard constraint:** `C·s_c ≤ V` (can't seat more committee positions than validators
  exist). Asserted at startup.
- The harness **warns** if `(V, C, s_c)` is far from `V/32 = C·s_c` (i.e. not a realistic
  once-per-epoch mainnet point) but **allows** it. That freedom is the point: a small `N` run can
  carry a realistic `s_c` with a modest `V`. (`scaling.md` §4's constraint — "full `s_c`=512 +
  correct fan-out + `N`<10k cannot coexist" — is sidestepped by decoupling the knobs.)
- **Optional convenience:** `ParamsFromV(V, N)` *fills* `C = min(64, V/4096)`, `s_c = (V/32)/C`
  from the mainnet formula, for runs that want a mainnet point. A helper — **not** the only path,
  **not** authoritative.

### Data shape

```go
package committee

// Stable for the whole run.
type Assignment struct {
    N, V     int
    valNode  []int    // validator i runs on node valNode[i]            (len V)
    nodeVals [][]int  // node j's validator indices                     (len N)
    backbone [][]int  // node j's SUBNETS_PER_NODE backbone subnets      (len N), from node-id
    params   Params
}

type Params struct {
    C, Sc            int   // committees/slot, committee size
    SubnetCount      int   // 64
    BackbonePerNode  int   // 2
    AggsPerCommittee int   // ~16
}

// AttesterRef: who emits one attestation, and where the recording key pins it.
type AttesterRef struct {
    Node     int   // which node publishes (the gossip origin)
    Val      int   // global validator index — the stable identity
    Subnet   int   // committee→subnet (one-to-one when C==64)
    Position int   // index within the committee (for aggregate bitfields later)
}

// SlotPlan is a pure function of (slot): everything about one slot's attestation phase.
type SlotPlan struct {
    Slot        int
    Committees  [][]AttesterRef  // [committeeIdx] → its s_c attesters     (len C)
    SubnetOf    []int            // committeeIdx → subnet id               (len C)
    Aggregators [][]AttesterRef  // [committeeIdx] → ~16 aggregator refs (subset of Committees)
    Subscribers [][]int          // [committeeIdx] → node ids that subscribe its subnet this slot
}
```

- `valNode[]` / `nodeVals[][]` — V→N mapping. **Uniform now** (validator `i` → node `i % N`),
  with a `Dist` seam for skew later (`scaling.md` §5). Validators are a **count + a pure
  function**, never V structs in memory — so memory scales with N, not V.
- `backbone[][]` — each node's `BackbonePerNode` long-lived subnets, a stable function of node-id
  (mainnet: from node-id, 256 epochs). Independent of validator count.
- `SlotPlan(slot)` — pure `f(slot)`: seed each slot's `C` committees as a draw of `s_c` validator
  indices; map committee→subnet (`= committeeIdx` when C=64, identity to subnets 0..63);
  seed-draw `AggsPerCommittee` aggregator positions per committee (models `is_aggregator()`'s
  *count* without BLS — a deliberate fidelity drop: no key-correlation, but the count and the
  resulting subscriber-set size are what dissemination depends on).
- **`Subscribers[c]`** = the nodes whose `backbone` contains `SubnetOf[c]`, **∪** the nodes
  hosting this slot's aggregators for `c`. This is the **expected receiver set** for any
  attestation on that subnet — the recording layer needs it to compute "missing" (§7).
- Because a node can hold several validators that land in the same slot's same committee, it can
  emit several messages on one subnet; the `Val` index keeps them distinct in the recording key.

### Both backends derive the same plan from one seed

simnet (one process) builds the `Assignment` as a Go value. For Shadow, **each host computes only
its own slice** of the plan from `(node-num, seed, V, C, s_c)` — a handful of scalar flags — so
there is no giant membership string to serialize. (The prior attestation sim passed
`-committee-memberships=0:7;1:42` per host, which does not scale to `s_c`=512×64; deriving from a
seed replaces it.)

---

## 3. Subnet-aware topology + fan-out reachability

**The consequence of modeling real subnets (the part to get right).** With no discovery layer,
the peer graph must *guarantee* what discv5 + the ENR `attnets` bitfield provide on mainnet: that
a node can find peers on the subnets it cares about. Random peers do **not** guarantee this.

Requirements the topology generator must satisfy:

- **Within each subnet S, its subscribers form a connected subgraph** — so gossipsub's D-mesh
  forms over S and an attestation actually disseminates within it.
- **Every attester has ≥ a few peers among S's subscribers**, for each subnet S it publishes to —
  so fan-out (`getFanoutPeersForPublishing`) has somewhere to send. Quantitatively: at `N`=1000 a
  subnet has ≈ `N/32` + aggregators ≈ 47 subscribers (~4.7% of nodes); with `P`=70 random peers a
  publisher expects ~3 subnet-peers **on average — but the variance means some publishers reach
  0**, which shows up as "0 arrivals, looks broken." So this must come from **construction, not
  chance**.

What changes, and what doesn't:

- We change topology **generation**, not the membership-sharing protocol. Once the graph is
  connected appropriately, gossipsub's own subscription gossip tells a publisher which of its
  peers are on S. Nodes still just receive a peer list, exactly as today — the list is now built
  subnet-aware. **The generator "plays discv5."**
- Touches the peer-graph construction in `netsim` (simnet) and `simctl/topology.py` (Shadow);
  both consume the `committee` assignment + the shared seed. `P` (peers/node) rises toward
  mainnet-ish ~70–100 to support the overlaid subnet meshes.
- The stable guarantee lives on the **backbone**; per-slot aggregators GRAFT into the existing
  backbone mesh through their peers.

**This is the single most likely silent failure of the whole stage**, so it gets its own
milestone test (§8 M2) — a publisher that is *not* a subscriber must still reach every subscriber.

Construction strategy (intra-subnet ring vs. k-regular per subnet, plus the inter-subnet glue and
the `P` needed at each `N`) is **(open)** — §10.

---

## 4. Node changes (smallest-first)

The Node stays message-agnostic — it matches on the gossipsub **topic name**, never on consensus
meaning. Reuses patterns from `../batched-attestation-sim/cmd/attestation/node/`.

- **A topic set instead of one topic.** Today `JoinTopics` hard-codes the single block topic.
  Generalize to: the global block topic (Join + Subscribe, as now) plus, per node, a set of
  `(subnet, subscribe?)`:
  - **backbone subnet** → Join **+ Subscribe** (mesh member: relays + receives);
  - **aggregator subnet** (this slot) → Join **+ Subscribe** for the slot;
  - **publish-only subnet** (an attester duty's subnet, not backbone/agg) → **Join only** (fan-out
    publish, no mesh).

  Reuse the prior art's `joinOne(name string, subscribe bool)`: `Join` always populates
  `n.topics[name]` (so `Publish` works — the existing `Publish` requiring `n.topics[topic]` is
  already compatible); `Subscribe` is the orthogonal mesh decision, appended to `n.subs` only when
  true. One receive goroutine per subscription, each tagging arrivals with its topic.

- **One batched verifier per node** (not per subnet) for the attestation topics. The node models a
  single CPU; the t≈4 s burst of attestations queues single-file (≈ M/D/1, `scaling.md` §7).
  Per-subnet verifiers would model N parallel CPUs and **understate** the flood — which is the
  whole point of this stage. The block path keeps its **own** fixed-delay verify hook (one block /
  slot, no batching; `plan.md` §5 wants the two paths independent). **Copy
  `batched-attestation-sim/.../verifier.go` verbatim** — the batch window + base + per-item sleep.

- **decode + route `KindAttestation`.** `node.Kind` already has `KindAttestation = 2`. `decode()`
  switches on topic; add: topic prefix `/eth2/.../beacon_attestation_` → unmarshal
  `pb.Attestation` → `Received{Kind: KindAttestation, Obj: att}`.

- **`SeenBlock(slot) (origin int, seen bool)`** — block-seen state, set in the block receive path
  (record the first-seen / processed time per slot). This is networking state living in the
  networking half; it is the *only* new read the coupling needs (§5).

---

## 5. The response coupling — `min(block_processed, deadline)`

The new causal edge. Keep `Node` (networking) and `Validator` (duties) pure; the **Driver is the
seam** — it already wires every node's `OnReceive` *and* drives the slot loop, so it is the one
component that sees both the block arriving and the duty firing. **No `Behavior` type yet** — that
abstraction (`plan.md` §3) earns its place once there are ≥2 policies (aggregates, gloas PTC);
introducing it now would be the "interface zoo" `phase1-spec.md` §3 rejected.

- **`Validator.AttestDuties(slot) []AttestDuty`**, where `AttestDuty{ Subnet string; Val, Position
  int }` — **content-free, pure, computed at slot start**: "this node owes an attestation to these
  subnets, for these validators." No vote in it.
- **Emit time = `min(block_processed, deadline)`**, event-driven per (node, slot):
  - `block_processed` = the block's receive timestamp (the verify-hook sleep is already on that
    path); plus an optional constant `Δ_prep` (default 0).
  - The Driver arms a deadline timer at `ATTESTATION_DUE`. **On block-receipt** for the slot (if
    not already emitted and `block_processed ≤ deadline`) → emit now, vote **block**. **At the
    deadline**, if not yet emitted → emit, vote **prior head** (a sentinel root). A tie exactly at
    the deadline votes **block** (`≤`).
  - At emit time the Driver reads `nd.SeenBlock(slot)`, builds `makeAttestation(slot, val, subnet,
    votedRoot)`, and publishes.
- **Honest note on "the node loop is unchanged":** the publish-at-an-offset *mechanism* is reused,
  but the duty model changes — emit timing becomes block-driven, and a node emits many
  attestations per slot rather than ≤1. So the flood is **not** a single spike at t=4 s; it is a
  **wave that tracks the block-arrival CDF (+Δ\_prep), with a spike at the deadline** from the
  nodes that missed the block. That shape *is* the measurement.

---

## 6. Wire format — `pb.Attestation`

A new proto mirroring `pb.Block`'s style, sized to **240 B** (`SingleAttestation`,
`slot-messages.md` §5):

```proto
message Attestation {
  uint32 slot         = 1;
  uint32 subnet       = 2;  // committee→subnet
  uint32 val          = 3;  // global validator index — stable identity
  uint32 origin       = 4;  // publishing node number (gossip origin; for the loopback skip)
  uint32 voted_origin = 5;  // which block it voted for, or a sentinel for prior-head
  bytes  payload      = 6;  // random filler so the marshaled message hits 240 B
}
```

`voted_origin` is the **measurable output of the causal edge** — it records, per attestation,
whether the attester voted the current block or the prior head. Aggregating it across the network
gives the headline metric (§7).

---

## 7. Metrics — generalize the identity, add per-subnet expectations

The recording layer (`metrics/tracer.go`) is where the one-message-per-slot assumption is hard-
coded. Generalize it once, cleanly, keeping the block path behaviorally identical.

- **`MsgID{ Kind; Slot, Subnet, Attester, Origin int }`** is the new key. A block is the special
  case `{KindBlock, slot, -1, -1, proposerNode}` — Phase-1 semantics preserved. Signatures become
  `OnPublish(MsgID, at)` / `OnReceive(node int, MsgID, at)`; `Recorder` keys its `pub` map on
  `MsgID`; `SlogTracer` emits the extra fields (absent / −1 for blocks).
- **Per-subnet expected/missing.** A subnet message is expected only at `Subscribers[subnet] \
  {publisher}` — **not `N−1`**. So the recording layer needs the subscriber sets. Both backends
  derive them from the same seed; emit the assignment (subscriber sets per slot/subnet) as a
  **JSON sidecar** in the run dir, and have Python consume it — rather than re-implementing the
  assignment in Python. Add a **`leaked`** category: an arrival at a node that is *not* a
  subscriber (a correctness failure, distinct from "missing").
- **Fix the silent skip.** `Recorder.OnReceive` currently `return`s silently if no publish was
  recorded ("shouldn't happen in-process"). Under drops or late publishes this *can* happen and
  would hide data — **count it** (e.g. an `orphans` tally), don't drop it.
- **Headline metric (why the stage exists):** the **fraction of attestations voting block vs.
  prior**, bucketed by the attester's block-arrival delay — the block→attestation coupling made
  visible. Plus the per-subnet arrival CDF, and a **drop rate** (a t≈4 s flood realistically
  overflows the validate / outbound queues; record drops via the prior art's `drop_tracer` and
  report them as a *metric*, not a test failure).

---

## 8. Testing strategy — the emphasis

The hardest part, and the reason to spec before coding. The north star is inherited from
`driver_test.go`: **exact counts derived from a known, seeded assignment** — never "at least N."
Every assertion below is arithmetic on a fixed `(N, V, C, s_c, seed)`.

### Milestones — each green before the next

| M | Layer | Bubble? | The exact invariant |
|---|-------|---------|---------------------|
| **M0** | `committee` assignment | no (pure) | V→N partitions (`Σ kₙₒ𝒹ₑ = V`, no overlap); each slot has exactly `C` committees of exactly `s_c` ⇒ attesters/slot `== C·s_c`; the `C·s_c ≤ V` guard fires when violated; committee→subnet bijection at C=64; backbone exactly 2/node and stable; aggregators = `AggsPerCommittee`, ⊆ committee; **same seed ⇒ byte-identical, seed+1 differs**; **direct knobs `V,C,s_c` honored as-is** (not re-derived); `ParamsFromV` tested *separately* as the optional filler at boundary V (4096, 64·4096, non-multiples). Exports `expectedSubscribers(slot, subnet)` reused by M2/M5/M6. |
| **M1** | attest duty | no (pure) | `AttestDuties(slot)` → correct subnets/positions for the node's validators; `pb.Attestation` ≈240 B, non-zero filler. The deadline rule is kept behind an **injectable block-seen predicate** so it is testable here with no network. |
| **M3** | batched verifier | yes (no net) | port `verifier_test.go` verbatim — see §8.4. |
| **M2** | subnet fan-out / subscribe | yes, N≈3 | publisher `P ∉ subscribers(S)` still reaches **both** subscribers; a non-subscriber receives **0**; `got == want` set-equality both directions. Clears the §3 reachability risk in miniature. |
| **M4** | attest on the wire | yes, N≈4 | one committee, all members subscribe, **no block** (isolate firing from coupling): arrivals `== s_c·(s_c−1)`; publish timestamps exactly at `slotStart + ATTESTATION_DUE` under the fake clock. |
| **M5** | response coupling | yes | the hardest — see §8.3. |
| **M6** | full run + metrics | yes, sized | `TestNNodesCyclicDissemination` analogue: per-subnet exact coverage + no-leakage + the fraction-voted-block metric; drop rate recorded. |

### 8.1 Determinism under `synctest`

The block phase already proved real libp2p parks cleanly in the bubble. Attestations add a flood
and a verifier goroutine; both are fine, with care:

- **The flood multiplies goroutines, not nondeterminism.** At the emit instant every attester
  wakes from its sleep, publishes, and re-parks on a channel/timer; randomness is seeded and the
  virtual clock is serialized, so arrival *delays* stay deterministic. Tie-breaking among messages
  at an identical virtual timestamp may reorder *arrivals* — so **assert on distributions and
  counts, never exact arrival sequences.**
- **The batched verifier durably blocks → the bubble can advance.** Its loop is `select { <-notify;
  <-timer.C }` with an idle timer at `math.MaxInt64`; both arms are receives, the canonical durable
  block. M3 proves this directly. Guard the one failure mode (a zero-length batch window busy-
  spinning) with an idle-advance test and by keeping `batchWindow > 0`.
- **`-race` epoch overflow gets worse.** The block N-node synctest test is already skipped under
  `-race` (`race_test.go`); the flood adds O(s_c²) deliveries/subnet plus verifier handoffs, so
  **race-skip M6** too (extend the `raceEnabled` guard's justification). Keep race coverage in the
  small layers: M3 (50-submit burst), M2/M4 (N≈3–4), and a generalized-recorder concurrency test.
- **Sizing (knobs set directly, `C·s_c ≤ V`):** M6 at `N=16, V=32` (2 validators/node), `C=1,
  s_c=8` — 8 of the 32 validators attest the single subnet; total arrivals = `Σ` over those 8
  publishers of `(|Subscribers(S)| − [publisher ∈ Subscribers(S)])`, one number computed from the
  assignment (low tens — *not* `(n−1)`-style, because only subscribers receive). A multi-subnet
  variant `C=2, s_c=4` adds a cross-subnet leakage check. **Exercise k>1 explicitly** (a node
  holding two of the slot's attesters on one subnet emits two messages). **Never hand-check
  `s_c=512`** — small `s_c` in tests, real `s_c` in Shadow (`scaling.md` §4). Keep total per-slot
  deliveries in the low hundreds (the block test's ~160 is the proven-safe reference).
- **Determinism guard:** run M6 twice in one process (two bubbles, same seed) and assert
  byte-identical sorted arrivals.

### 8.2 Subnet-dissemination invariants (M2, M6)

For one attestation on subnet S by node P at slot `sl`:

- **MUST receive (exactly once):** `expectedSubscribers(sl, S) \ {P}` = backbone subscribers of S
  ∪ this-slot aggregators of S's committee, minus P itself (the `origin == self` loopback skip,
  generalized to `receiver == publisher`).
- **MUST NOT receive (exactly zero):** every node ∉ `expectedSubscribers(sl, S)` — other subnets'
  subscribers and pure relays. This **no-leakage** property is the strongest new invariant.
- **The assertion:** from the assignment build `want = expectedSubscribers(sl, S) \ {P}`; from a
  `fakeTracer` keyed on `(node, slot, subnet, attester)` build `got`; assert `got == want`
  set-equality **both directions** (missing → fail, leaked → fail, duplicate → fail).
- **Fan-out specifically:** M2 picks a publisher `P ∉ subscribers(S)` and asserts the subscribers
  still receive while P itself receives 0 — proving publish-without-subscribe delivers.

### 8.3 The coupling test (M5) — deterministically flip the vote

The new and hardest behavior. Three layers, cheapest first:

- **(A) Pure decision-rule test — load-bearing, no network.** Drive the duty builder with
  synthetic `block_processed` times and assert *both* the emit time and the vote tag:
  `processed + Δ < deadline` → emit at `processed+Δ`, vote **block**; never processed → emit at
  **deadline**, vote **prior**; exactly at the deadline → assert the documented `≤` tie (vote
  block). This is where the rule's correctness is *proven*.
- **(B) Suppress the block to ONE node (primary network test).** A test-only `OnReceive` filter
  (or per-node verify override) holds the block for `lateNode` past the deadline. Assert
  `lateNode`'s attestation is tagged **prior**, the others **block**, and
  `fraction_voted_block == (s_c − 1)/s_c`. The crossing is forced by construction — it cannot
  flake.
- **(B2) Fixed-latency topology (full receive path).** Hand-build a topology (reuse
  `NewFromTopology` / `latencyFromEdges`, deterministic when min==max) where `lateNode`'s fastest
  block path + verify provably exceeds `deadline − Δ`; set the proposer's jitter to 0 so the
  publish time is exact. Same tag-flip assertion, exercised over the real receive path.
- **Metric assertion:** reuse the block-arrival recorder so the test knows each node's exact block
  arrival; assert `fraction_voted_block(slot)` **exactly** equals
  `|{members with block_arrival + Δ ≤ deadline}| / s_c` — exact, because the fake clock makes
  arrivals exact (a stronger statement than a real run could make). Plus a **monotonicity sweep**:
  move the deadline across the block-arrival CDF; the fraction is non-decreasing, 0 before the
  fastest arrival, 1 past the slowest — three points suffice.

### 8.4 Batched-verifier tests (M3) — network-independent

Port the prior `verifier_test.go` suite verbatim (it *is* the spec for this component), all inside
`synctest.Test`, no hosts: single dispatch; batch-window accumulation (first item solo, then a
windowed pair); total delay `= base + n·perItem` invoked once per *batch* not per item; **50
concurrent submits, none dropped** (the t≈4 s flood in a test tube — the single most important
verifier property); `submitAndWait` blocks for the batch (so the cost sits on the per-hop path);
stop-drains; stop-without-work stays idle (the busy-spin guard). Plus one **integration
inequality**: a single message's release delay `D₁`, then `k` at one instant releasing at `Dₖ`;
assert `Dₖ > D₁` and grows with `k` (≈ `base + k·perItem`) — proving the CPU queue is on the path.

### 8.5 Cross-backend + Python

Because both backends derive the assignment from the same seed, the **set** of
`(slot, subnet, attester, receiver)` arrivals is identical across backends (only timing differs).
So:

- `simnetrun.TestRun`'s `params` and `_simnet_params()` gain `V, C, s_c, AggsPerCommittee,
  ATTESTATION_DUE bp, Δ_prep`; the arrival CSV becomes
  `node, slot, kind, subnet, attester, delay_ms, voted_block` (block columns preserved, so the
  Phase-1 block cross-check is untouched).
- `SlogTracer` gains `attest_publish` / `attest_arrival` lines (slot, subnet, attester,
  voted_block, t_ns) so a Shadow run reassembles by `(slot, subnet, attester)`.
- `check_arrivals.analyze` generalizes: `expected = Σ (|Subscribers(S)| − [publisher∈Subscribers])`
  read from the sidecar; add the **`leaked`** category; `RESULT: OK` iff no missing / duplicate /
  leak and exact count. Per-subnet CDF via the existing `cdf()`; fraction-voted-block over the
  `voted_block` column. pytest table tests (stdlib only) cover full-receipt-OK / missing / leak /
  fraction.
- **Parity assertion:** assert set-equality of arrival *identities* across the two backends, and
  CDF equality within the documented tolerance (the block path's ~11% residual is the precedent).

---

## 9. Proposed defaults

| Knob | Tests | Runs | Notes / source |
|------|-------|------|----------------|
| `N` (nodes) | 16–32 | swept | as Phase 1 |
| `V` (validators) | `2·N` | swept | k-per-node fan-out; uniform V→N |
| `C` (committees/slot) | 1–2 | per scenario | independent; or `ParamsFromV` |
| `s_c` (committee size) | 8 | per scenario | the per-subnet flood; small in tests |
| `SubnetCount` | 64 | 64 | `ATTESTATION_SUBNET_COUNT` |
| `BackbonePerNode` | 2 | 2 | `SUBNETS_PER_NODE` |
| `AggsPerCommittee` | 16 | 16 | `TARGET_AGGREGATORS_PER_COMMITTEE` |
| `ATTESTATION_DUE` | 3333 bp | 3333 bp | t=4 s @ 12 s slot |
| `Δ_prep` | 0 | 0 | extra processing before emit (open) |
| Attestation size | 240 B | 240 B | `SingleAttestation` |
| Verify hook (attn) | batched | batched | window/base/per-item from prior art |
| `P` peers/node | raised | ~70–100 | for the subnet overlay (§3) |

Startup assertion: `C·s_c ≤ V`.

---

## 10. Decisions & open questions

**Locked (with the user):**
1. **Scope** — attestations only; aggregator *selection* in, the aggregate *message* deferred.
2. **Timing** — `min(block_processed, deadline)`; vote block iff processed `≤` deadline, else
   prior; event-driven.
3. **Fidelity** — real fan-out / backbone / aggregator structure ⇒ subnet-aware topology (§3).
4. **Recording key** — generalize to `(slot, kind, subnet, attester)`; block is the −1 special
   case.
5. **Validators/node** — k-per-node faithful (one message per validator, matches Prysm).
6. **Knobs** — `V`, `C`, `s_c` all independent + configurable; no forced `V/32 = C·s_c`;
   `ParamsFromV` optional.

**Open (the spec lists; not blockers to start):**
- Per-slot aggregator subscribe (faithful, but GRAFT/PRUNE churn each slot) vs. whole-run
  subscribe (simpler, but over-subscribes — document the inflation).
- `Δ_prep` value (default 0).
- Subnet-aware graph construction: intra-subnet ring vs. k-regular per subnet, the inter-subnet
  glue, and the `P` needed for reliable fan-out reachability at each `N`.
- Whether each slot's committees are drawn with once-per-epoch coverage of V (faithful) or
  independently per slot (simpler).
- Faithful epoch-based committee rotation (deferred).

---

## 11. Reuse map

- **Copy / adapt from `../batched-attestation-sim/cmd/attestation/node/`:** `verifier.go` (the
  batched verifier — the M/D/1 flood queue), `joinOne(name, subscribe bool)` + the per-subscription
  receive pattern, `drop_tracer.go`, `topicName(i)`.
- **Write fresh:** the `committee` package (Go assignment from a seed, replacing the prior art's
  Python flag-soup); subnet-aware topology generation; the `MsgID` recording identity; the
  `min`-rule Driver mediation + `Node.SeenBlock`.
- **Explicitly not reused:** the prior art's partial-messages machinery (`partial.go`) — a
  separate research axis (IDONTWANT / partial-message efficiency), orthogonal to modeling the slot
  response.

## 12. Files the implementation will touch (for reference — not part of this spec)

```
committee/                  NEW — pure assignment layer (§2)
pb/attestation.proto        NEW — pb.Attestation, 240 B (§6)
node/node.go                multi-topic Join/Subscribe, per-node batched verifier,
                            decode/route KindAttestation, SeenBlock (§4)
validator/validator.go      AttestDuties(slot) (content-free) + makeAttestation (§5)
driver/driver.go            min-rule mediation, generalized loopback skip, per-slot subscribe (§5)
metrics/tracer.go           MsgID identity; per-subnet expected; count orphans (§7)
netsim/ + simctl/topology.py  subnet-aware peer graph (§3)
analysis/check_arrivals.py  per-subnet expected/missing + leaked + fraction-voted-block (§7,§8.5)
simnetrun/run_test.go,      carry V/C/s_c/subnet params; each Shadow host derives its
cmd/slot-sim-node/main.go,    own SlotPlan from the seed (§2, §8.5)
simctl/config.py
```

## 13. Summary

After the block disseminates, each attester emits one `SingleAttestation` (240 B) on its
committee's subnet at `min(block_processed, deadline)`, voting for the block iff it arrived in
time. A new pure **`committee`** layer maps `V` validators over `N` nodes and, per slot, seeds
committees → subnets → aggregators, with `V`, `C`, `s_c` as independent knobs. Plain attesters
**fan-out-publish** without subscribing, so the **topology generator becomes subnet-aware** to
stand in for discovery. The **Driver** mediates the block→attestation edge, keeping Node and
Validator pure. The recording key generalizes to `(slot, kind, subnet, attester)`, and the
headline result is the **fraction of attestations that voted the block vs. the prior head** as a
function of block arrival. Everything is testable as **exact counts from a seeded assignment**,
with the assignment layer proven before any network, the coupling proven by forcing one node's
block past the deadline, and the flood's CPU queue proven by the ported batched-verifier suite.
