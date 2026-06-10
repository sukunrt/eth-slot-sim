# Partial-message attestation spec — batched floods over the gossipsub partial-messages extension

Implements `../ethp2p/specs/draft-committee-attestation.md` for this simulator's two
attestation-class floods: the **standard attestation** (the t≈4 s per-subnet flood,
`attestation-spec.md`) and the **finality attestation** (the decoupled per-round burst,
`decoupled-consensus-spec.md` + `validator-segregation-spec.md`). One config toggle selects the
transport — `classic` (today's one-`Publish`-per-message gossipsub) or `partial` — on the same
`schedule.json`, so classic-vs-partial runs are comparable by construction, on both backends.

The mechanics are a port of the working implementation in
`../batched-attestation-sim/cmd/attestation/node/partial.go` (the prior art), generalized from
"one committee per topic, one slot group" to this repo's per-kind registry. **Zero dependency
changes**: both repos pin the same `go-libp2p-pubsub` fork
(`v0.16.1-0.20260515125344-5ac7695ba01b`), which carries the `partialmessages` extension, and
its tests already prove the extension parks cleanly under `testing/synctest` on simnet.

> Spec, not code. §11 lists locked decisions and §12 the deferred ones.

---

## 0. Scope & non-goals

**In:**

- Standard attestations (`KindAttestation`) when the attestation phase is on.
- Finality attestations (`KindFinalityVote`) when decoupled is on — base **and** segregated.
- Verification entering the existing per-class batch verifiers from the partial RPC handler.
- Arrival metrics identical in shape and semantics to classic (unchanged CSV/slog contract,
  unchanged `analysis/check_arrivals.py`).

**Out (unchanged, stay classic):** blocks, columns, aggregates, sync messages/contributions,
**AC votes** (locked: the per-slot AC flood keeps the classic transport), finality aggregates.

**Out (deferred):** byte-split bandwidth tracing (att_data / signature / control bytes, the
prior art's `rpc_tracer` + `prelim-analysis` comparison) — a follow-up spec; real signature
aggregation (the draft spec explicitly forwards signatures unaggregated).

---

## 1. Protocol recap (what the transport does)

From the draft spec + prior art, per topic:

- **The unit** is one committee member's attestation, split into the three parts
  `(attestation_data, position, signature)`. Attestations sharing `attestation_data` batch into
  one `BatchedAttestation{attestation_data, attestor_indices[], signatures[]}` — the data sent
  once per batch, ~97 B marginal per extra vote (96 B sig + varint position) instead of a full
  ~240 B message + per-message gossipsub framing.
- **Mesh peers get eager push**: every `publish_interval` (20 ms) tick, a node sends each mesh
  peer the validated attestations it hasn't yet sent them (per-peer `available` bookkeeping; a
  per-attestation forward cap bounds redundancy). Mesh peers get **no metadata**.
- **Gossip (non-mesh) peers get metadata** at heartbeat cadence:
  `CommitteeAttestationPartsMetadata{slot, attestation_data, available, requests}` with
  fixed-width bitmaps over the committee index space. `requests` is a **non-persistent** IWANT:
  the receiver satisfies what it can on its next tick and forgets the rest. Up to
  `max_iwant_per_position` (10) gossip peers are asked for any one missing position.
- **State is per `(topic, group, attestation_data)` bucket**, so forks at the same slot coexist;
  nodes MUST NOT dedupe by `(slot, position)` across buckets (committee is a function of the
  data). `group` is the extension's groupID — our slot key (§3).
- Partial-published messages never enter mcache ⇒ **no classic IHAVE/IWANT for them at all**;
  the metadata gossip above is their gossip layer. This is the IHAVE-overhead fix the draft spec
  targets.

---

## 2. Wire format — the three-way split

New protos in `pb/` (mirroring the prior art's `attestation.proto`):

```proto
// PartialAttData is the shared attestation_data: the vote + filler. Deterministically built
// (all-ones filler, sizedFiller) so every attester voting the same way produces byte-identical
// data — the bucket key IS these bytes. voted_origin: the voted block's origin, the prior-head
// sentinel (max uint32), or 0 for vote-free kinds (finality attestations this cut).
message PartialAttData {
  uint32 slot         = 1; // the group key (slot / round key), for debuggability + uniqueness
  uint32 subnet       = 2;
  uint32 voted_origin = 3;
  bytes  payload      = 4; // filler to attestation_data_size
}

message BatchedAttestation {
  bytes  attestation_data       = 1; // marshaled PartialAttData (opaque to the transport)
  repeated uint32 attestor_indices = 2; // committee positions, ascending; < committee size
  repeated bytes  signatures    = 3; // one per index, signature_size B filler each
}

message PartsMetadata {
  uint32 slot             = 1;
  bytes  attestation_data = 2;
  bytes  available        = 3; // bitmap[committeeSize] — positions we hold (validated only)
  bytes  requests         = 4; // bitmap[committeeSize] — positions we want; non-persistent
}

message ControlEnvelope     { repeated PartsMetadata metadatas = 1; } // one per (peer, tick)
message BatchedAttestationEnvelope { repeated BatchedAttestation batches = 1; }
```

- `attestation_data_size` (default **128 B** ≈ real `AttestationData`) and `signature_size`
  (default **96 B**, BLS) are knobs; classic's 240 B `pb.Attestation` is untouched
  (128 + 96 + identity overhead ≈ 240 — the splits are consistent by construction).
- **Fork buckets**: a standard attestation's vote lives INSIDE `attestation_data`, so
  block-voters and prior-head-voters of one committee form two buckets — exactly the draft
  spec's fork case, exercised on every slot where any node misses the deadline. Finality
  attestations are vote-free this cut ⇒ one bucket per (topic, group).
- Receivers never need to parse `attestation_data` for metrics (identity comes from
  (topic, group, position), §3); parsing it for the vote is available for debugging.

## 3. Index spaces — position per kind

The committee index space (bitmap width, `attestor_indices` domain) is per kind:

| | standard attestation | finality attestation |
|---|---|---|
| topic | `beacon_attestation_<subnet>` | `finality_vote_<subnet>` |
| groupID | slot | round key: fslot (base) / AC slot (segregated) — same key `FinalityVoteID` rides |
| position | `AttesterRef.Position` (already in schedule.json) | rank of `val` in its cell (derived, below) |
| committee size | `Params.Sc` (uniform) | `ValidatorsPerSubnet[i]` / `ValidatorsPerRoundSubnet[r][i]` |
| buckets/group | ≤2 (block vote, prior-head vote) | 1 (vote-free) |

- Committee → subnet is identity in the generator (`subnet = ci`, `simctl/schedule.py`), so
  (topic, group) pins the committee; positions are unambiguous.
- **Finality position derivation (locked: in Go, no schedule.json change):**
  `position(val) = index of val in sorted{v : FinalitySubnetOf[v] == subnet}` — intersected with
  `FinalityRoundOf[v] == group % k` under segregation. Computable identically from
  `schedule.json` fields both sides already carry; cell sizes equal the carried
  `ValidatorsPerSubnet` / `ValidatorsPerRoundSubnet` counts by construction (pinned by test,
  §10). Built once per (subnet[, round]) in `schedule.Load`-adjacent helpers, both directions
  (val→position, position→val).
- `schedule.AttestDuty` gains `Position int` (from `AttesterRef.Position` for standard duties;
  the derived rank for `FinalityVoteDuties`). `ACVoteDuties` leaves it zero (AC stays classic).
- groupID encoding: `binary.BigEndian.AppendUint32` of the group key (prior art's
  `slotGroupID`/`groupIDToSlot`, verbatim).

---

## 4. The partial flood manager (`node/partial.go`)

One manager per node, owning all partial-kind topics — the prior art's
`partialAttestationManager` with the per-topic scalars (committee size, group semantics,
identity resolution) supplied by a small per-kind config resolved through the registry (§6).

State (ported verbatim in shape):

- `buckets[topic][group][string(attestation_data)]` → `attestations map[position]entry`,
  `validating`/`validated` sets, `sendCount[position]`, `requestCount[position]`,
  `peers map[peer.ID]{available, pendingWant bitmaps}`.
- Extension peerState `{gossipPeer, sendAvailableList bool}` with the prior art's semantics:
  `OnEmitGossip` marks gossip peers + arms a once-per-heartbeat Available advertisement; a
  publish tick serves each gossip peer once then drops it from peerStates (an incoming Want
  re-adds it); mesh peers are re-added every tick by the extension.

Behavior, per tick (`publishActions`, ported):

1. Select IWANTs: for each bucket × missing position advertised by a gossip peer, pick up to
   `max_iwant_per_position − already-asked` target peers; bump `requestCount`.
2. Per peer: build one `BatchedAttestationEnvelope` (mesh: everything validated, unsent, under
   the `max_peers_per_attestation` cap; gossip: only their pending Wants) and one
   `ControlEnvelope` (gossip peers only; Available gated to the heartbeat flag, Requests always
   served). Clear the peer's `pendingWant` unconditionally — requests are non-persistent.
3. Self-publish (`publishLocal`): store own attestation in its bucket, **validated
   immediately** (never re-verify your own signature — classic's own-publish bypass), picked up
   by the next tick.
4. Non-member duty publish (`fanoutPublish`): one eager `PublishPartial` of a single batch to
   all current peers — the extension's `MeshPeers` falls back to
   `getFanoutPeersForPublishing`, which the existing Join + dial-2-subscribers warmup feeds
   (standard: `beginSlot`; finality: `prejoinFinality`). Eager-once, like the prior art's
   fanout nodes; no tick-loop membership for foreign topics.

Loop & lifecycle:

- One goroutine ticking every `publish_interval` (default 20 ms) over all (topic, group)s with
  live buckets, started at `JoinTopics` when any partial kind is active, stopped by `Close`.
- **Initial jitter must be seeded** (`seed`, node num) — the prior art's `rand.Int64N` start
  offset would break synctest determinism and cross-run reproducibility.
- Extension wiring: `pubsub.WithPartialMessagesExtension`, `GroupTTLByHeatbeat: 10`,
  `OnEmitGossip`/`OnIncomingRPC` as above. `disable_metadata_gossip` knob (prior art's
  `disable_ihave_gossip`): `OnEmitGossip` returns early — the no-gossip variant.
- **Bucket GC is runner-driven** (the extension TTLs its own pubsub-side state): the runner
  calls `nd.PrunePartial(topic-kind, group)` one slot after the group ends — `endSlot(slot)`
  prunes standard group `slot−1`; `reapFinality` prunes the reaped round's group. A grace slot
  keeps late stragglers countable rather than re-creating buckets.

---

## 5. Verification — same queues, new entry point

Classic registers a gossipsub topic validator that `submitAndWait`s on the class's batch
verifier, blocking delivery per message. Partial mode registers **no topic validator** for
partial-kind topics (no normal messages flow there at all); verification enters from
`OnIncomingRPC`:

1. Decode the envelope; reject malformed batches (position ≥ committee size,
   indices/signatures length mismatch) by returning an error.
2. Drop positions already held; store new ones as **validating**; infer the sender's
   `available |= positions`.
3. `submit()` (non-blocking, existing API) ONE `verificationItem` whose `Attestations` length =
   the new-position count, to the kind's existing class queue — `vcConsensus` for standard,
   `vcFCVote` for finality — with a callback that promotes those positions
   **validating → validated** and fires the arrival metric (§8). A 1000-vote batch therefore
   costs `base + 1000·perItem` once in the shared window: same M/D/1 model, with the batching
   win priced honestly.
4. Only **validated** positions forward: `claimAttestationsToSend` and the `available` bitmap
   read the validated set only. Validating positions are held — deduped against (not
   re-requested) but never advertised or sent.

Per-hop modeled latency: receive → verifier (window + sleep) → next tick → send. Receipt no
longer parks a pubsub validation goroutine, sidestepping the validate-queue/throttle pressure
the FC burst caused at 10k scale (the `WithValidateQueueSize/Throttle` bumps in `node.go` stay,
for the classic kinds).

---

## 6. Node + registry integration

- **Registry** (`node/registry.go`): descriptors for `KindAttestation` and `KindFinalityVote`
  gain a partial-mode config — committee-size-for-topic, group semantics, and the identity
  resolver hook (below). The verify `class` field is reused as-is for queue routing. All other
  kinds are untouched; `Decode`/topic lookup unchanged (partial RPCs never reach `decode`).
- **Join**: when the transport is partial, `joinLocked` passes `pubsub.RequestPartialMessages()`
  for partial-kind topics and skips `registerVerifyHook` for them. `Subscribe` is still what
  makes a node a mesh member (GRAFT) — its receive goroutine simply stays idle for these
  topics, since no normal messages flow.
- **Identity resolver** (driver-injected, keeps `node/` schedule-free): the manager synthesizes
  arrivals via a `func(kind, subnet, group, position) (val, originNode int)` set on the Node
  before `JoinTopics`. The driver builds it from the schedule: standard —
  `Slots[group].Committees[subnet][position]` (`.Val`, `.Node`); finality — the §3 rank inverse
  + the hosting rule (`ValidatorCounts` contiguous ranges, else `val % N`).
- **Outward hand-off**: on validation the manager calls `OnReceive` with
  `Received{Kind, ID: Identity{group, subnet, val, origin-per-kind-policy}, Origin: originNode,
  At: now}` and `Obj: nil` (the runner never reads `Obj` for these kinds). Loopback cannot
  occur (own positions are validated locally, never round-tripped), so the runner's
  `Origin == r.num` skip is inert here — kept for uniformity.

## 7. Driver integration

Emit paths swap `Publish` for manager calls; all timing, coupling, warmup, and teardown logic
is **unchanged**:

- `emit` (standard): per duty, on a subscribed subnet → `publishLocal(topic, slot,
  d.Position, sig, data)`; on a non-member duty subnet → collect into one eager
  `fanoutPublish` batch per (topic, vote-bucket) after the existing Join + dial-2 (`beginSlot`
  already did the dialing). `data = MakePartialAttData(slot, subnet, votedOrigin)`.
- `emitFinalityVotes`: same split — own finality subnet → `publishLocal`; foreign duty subnets
  (pre-joined + dialed by `prejoinFinality`) → eager `fanoutPublish`. Group = the round key the
  function already receives.
- `tracer.OnPublish` stays exactly where it is (`AttestID`/`FinalityVoteID` + votedBlock at
  emit time) — the publish side of the metric is transport-independent.
- `endSlot`/`reapFinality` additionally prune manager buckets (§4). AC votes, aggregates, sync,
  columns: no changes.
- `driver.New` / `cmd/slot-sim-node` mutual-exclusion checks: `partial` requires the
  attestation phase or decoupled to be on (it is a transport, not a phase); rejected otherwise.

---

## 8. Metrics — contract unchanged

- **Publish**: unchanged (`OnPublish(AttestID|FinalityVoteID, votedBlock, at)` at emit).
- **Receive**: fires when the verifier callback promotes a position (locked: post-validation —
  the same moment classic's validator-gated delivery implies), with the SAME `MsgID` classic
  would produce. The CSV/slog schema, kind ints, `check_arrivals.py`, and the
  `roundtrip_test.go` pins are untouched; coverage expectations (subnet subscribers minus
  publisher) are identical, so **a classic and a partial run on one schedule must produce
  set-equal arrival identity sets** — the headline correctness invariant (§10).
- `FractionVotedBlock` and the vote-flip coupling metrics work unchanged (vote recorded on the
  publish side, joined on receive).
- `max_peers_per_attestation` (default 2·D ≥ mesh degree) cannot starve coverage: mesh
  redundancy delivers every subscriber; the cap only trims duplicate sends.

## 9. Config & flags

```yaml
attestation:
  ...
  transport: classic            # classic | partial — also governs finality votes when decoupled
  partial:                      # consulted only when transport: partial
    publish_interval_ms: 20
    max_peers_per_attestation: 0   # 0 ⇒ 2·D
    max_iwant_per_position: 10
    attestation_data_size: 128
    signature_size: 96
    disable_metadata_gossip: false
```

- Lives on the attestation block (present in every attestation/decoupled run already; pydantic
  validates `transport: partial` ⇒ attestation or decoupled phase on).
- Shadow flags mirror 1:1 (`-transport`, `-partial-publish-interval`, …) in
  `cmd/slot-sim-node/main.go`; `simnetrun`'s params struct and `_simnet_params()` carry the
  same fields (flag names = config field names, per improvements.md §8).
- `schedule.json` is unchanged — same plan drives both transports.

---

## 10. Testing strategy

Inherits the house rule: exact counts from a seeded schedule, never "at least N".

| M | Layer | The invariant |
|---|-------|---------------|
| **P0** | position derivation (pure) | FC rank: bijective per cell; sizes == `ValidatorsPerSubnet` / `ValidatorsPerRoundSubnet` (Σ over rounds = subnet count, ΣΣ = V); same seed ⇒ identical; val→position→val round-trips. Standard: `AttestDuty.Position` == `AttesterRef.Position`. |
| **P1** | codec (pure) | `PartialAttData` marshals to `attestation_data_size` ± framing, deterministic bytes (same scalars ⇒ identical — the bucket-key property); batch/metadata envelope round-trip; bitmap width = ceil(size/8). |
| **P2** | manager unit (synctest, no net) | port the prior art's `partial_unit_test.go` shapes: claim respects cap + per-peer available; gossip peer served once per heartbeat then dropped; requests non-persistent (cleared even when unsatisfied); `max_iwant_per_position` honored; fork buckets independent (no cross-bucket dedup); validating positions never advertised/sent. |
| **P3** | verification routing | RPC-handler entry: malformed batch rejected; new-position count = the per-item multiplier; callback promotes + fires OnReceive exactly once per position; own publish validated with zero verifier cost. |
| **P4** | e2e standard (synctest, small N) | one committee on one subnet, classic vs partial on the same schedule: **arrival identity sets set-equal both directions**, per-subnet exact coverage, no leakage; the vote-flip test (suppress block at one node) still yields two fork buckets and the exact `(s_c−1)/s_c` fraction; non-subscriber duty publish reaches all subscribers (fanout path). |
| **P5** | e2e finality (synctest) | decoupled fullrun variant: per-round coverage == classic run on the same schedule; segregated variant (cells); buckets pruned after reap (no growth across rounds). |
| **P6** | determinism + parity | run P4 twice in one process, byte-identical sorted arrivals (the seeded-jitter guard); simnet vs Shadow arrival-identity parity (existing cross-backend assertion, now per transport). |

Race-skip the sized e2e runs as today; keep `-race` on P0–P3. The prior art's
`partial_end_to_end_test.go` / `partial_gossip_cadence_test.go` are the porting templates.

## 11. Locked decisions (with the user)

1. **Scope** — toggle, not replacement; applies to standard attestations AND decoupled, where
   it covers **finality votes only** (AC votes stay classic). Aggregates/sync/columns classic.
2. **Wire identity** — committee positions + fixed-width bitmaps per the draft spec; validator
   ids recovered from the schedule for metrics.
3. **Arrival metric** — post-validation, single event, unchanged CSV/slog contract.
4. **Vote placement** — inside `attestation_data` as `(vote, filler)` ⇒ fork buckets per
   committee; finality attestations vote-free (one bucket) this cut.
5. **FC positions** — derived in Go by rank from `finality_subnet_of` (+ `finality_round_of`);
   no schedule.json change.
6. **Bandwidth byte-split** — deferred; delay/coverage parity is this spec's deliverable.

## 12. Open questions (not blockers)

- Whether a non-member duty publisher should retry/re-fanout if its eager batch is lost
  (prior art: no; mesh redundancy + metadata gossip recover it).
- Tick-loop sharing: one ticker for all topics (spec'd) vs per-topic loops — revisit only if
  the single loop's lock hold time shows up at 10k scale.
- When FC votes gain fork-choice content (planned extension), their `voted_origin` slots into
  `PartialAttData` unchanged — buckets split exactly like standard attestations.

## 13. Reuse map & files touched

**Port from `../batched-attestation-sim/cmd/attestation/`:** `node/partial.go` (manager:
buckets, peer bitmaps, tick actions, IWANT selection, metadata gating — the core of this spec),
`pb/attestation.proto` (the four partial messages, renamed per §2), test shapes from
`node/partial_unit_test.go` + `partial_end_to_end_test.go`. The verifier needs nothing — this
repo's copy already has `submit(item, callback)`.

```
pb/partial.proto            NEW — PartialAttData, BatchedAttestation, PartsMetadata, envelopes (§2)
validator/attestation.go    MakePartialAttData + size knobs (§2)
schedule/schedule.go        AttestDuty.Position; FC rank tables val↔position (§3)
node/partial.go             NEW — the manager (§4); seeded-jitter tick loop
node/registry.go            partial-kind config on the two descriptors (§6)
node/node.go                extension option, RequestPartialMessages joins, validator skip,
                            resolver field, PrunePartial, Close (§5, §6)
driver/runner.go            emit/emitFinalityVotes partial branches; prune calls (§7)
driver/driver.go            transport plumbing + mutual exclusion (§7)
cmd/slot-sim-node/main.go   -transport + -partial-* flags (§9)
simctl/config.py            transport + partial block, validation (§9)
simctl/runner.py            flag plumbing (§9)
simnetrun/run_test.go       params carry the transport knobs (§9)
```
