# PTC votes (payload_attestation_message) — implementation notes

Handoff doc for the next session. The ePBS two-phase block send is DONE (kinds 10/11,
`epbs` flag on by default, payload lag in the analysis). This doc is the deferred piece:
the Payload Timeliness Committee vote. Delete this file once implemented.

## Spec (consensus-specs, `specs/gloas/`)

- `PayloadAttestationMessage` rides the global `payload_attestation_message` topic
  (p2p-interface.md). One message per PTC validator per slot; no aggregation on gossip.
- PTC = 512 validators per slot (`PTC_SIZE = 2**9`), drawn per slot via `get_ptc`
  (beacon-chain.md). In the sim: a per-slot VRF-style draw in the schedule, like
  `ac_voters`.
- Deadline: the vote must publish within `PAYLOAD_ATTESTATION_DUE_BPS = 7500` — 75% of the
  slot (9 s at 12 s slots). `PAYLOAD_DUE_BPS` is also 7500: an envelope seen by 75% counts
  as present.
- Vote content (validator.md "Payload timeliness attestation"): the attester sets
  `payload_present = true` iff it saw the envelope for the block root before
  `get_payload_due_ms()`, and `blob_data_available = is_data_available(...)`. No block seen
  ⇒ no vote at all.
- Realistic wire size: `PayloadAttestationData` (root 32 + slot 8 + 2 bools) + validator
  index + BLS sig ≈ 150-180 B. Use `sizedFiller`; a good constant is ~176 B.

## Sim design (agreed earlier)

- New kind 12 = `KindPTCVote`, global topic, flood-verified. One message per PTC validator
  per slot. Kinds append only — 12, never renumber.
- The vote is the ePBS twin of the AC vote, but with a FIXED deadline emit (not
  block-coupled): fire at `payload_attestation_due` (75% of slot) and record what the node
  saw by then. Simpler and closer to spec than early-emit; an early-emit variant can come
  later if wanted.
- `payload_present = payloadSeen && columnsComplete` at the due instant — this is where the
  DA check that the epbs change removed from attestations comes back.
- Requires `epbs.enabled`; composes with attest/decoupled like everything else.

## Where everything goes (mirror the AC-vote path throughout)

1. **pb**: add `PTCVote {slot, val, origin, payload_present, payload}` to `pb/epbs.proto`
   (or a new file listed in `pb/doc.go`); `go generate ./pb`.
2. **validator**: `PTCVoteTopic` const + `MakePTCVote(slot, val, origin, payloadPresent,
   size)` in `validator/epbs.go`, `sizedFiller` like `MakeACVote`
   (`validator/decoupled.go`).
3. **node**: `KindPTCVote = 12` in `node/node.go`; registry entry in `node/registry.go`
   with its own verify class (add `vcPTC`, a batched single-server queue like `vcAC` — the
   classes map to `Node.verifiers` automatically). Identity `{Slot, -1, Val, Origin}` —
   copy the AC-vote entry. Every node subscribes the topic: add it in `Prepare()`
   (`driver/runner.go`) gated on `r.epbs != nil`, NOT unconditionally in `JoinTopics`.
   Update the pinned table in `node/registry_test.go`.
4. **metrics**: `PTCVoteID(slot, val, origin)` (copy `ACVoteID`); the `votedBlock` bool on
   `OnPublish` carries `payload_present` (the existing publish-side bool, like AC votes).
   Add the round-trip case in `metrics/roundtrip_test.go`. Consider
   `FractionVotedPTC(slot)` next to `FractionVotedACVote` in `metrics/tracer.go`.
5. **schedule**: Python draws, Go reads. In `simctl/schedule.py`: per-slot `ptc_voters`
   draw (shape identical to `ac_voters`: `[{node, val, subnet:-1, position}]`), size knob
   `ptc_size` (default 512, clamped ≤ V). In Go `schedule/schedule.go`: `PTCDuties` on
   `View`, parsed like `ACVoteDuties`.
6. **driver**: in `setupSlot`, when `r.epbs != nil` and the node has PTC duties this slot,
   arm ONE timer at `slotStart + ptcDue` (new `EPBSParams.PTCDue time.Duration`; 75% of
   slot ⇒ default 9 s at 12 s slots, pass as a duration flag like `-att-due`). The timer
   reads `ss.payloadSeen`, and the column state — under ePBS `ss.columnsComplete` is forced
   true at setup, so track the real custody completion separately or compute it from
   `len(ss.haveColumn) == len(ss.custody)`; do NOT resurrect the vote gate. Emit one
   `MakePTCVote` per duty on the global topic with
   `payload_present = payloadSeen && custodyComplete`; skip the whole burst if
   `!ss.blockSeen` (spec: no block ⇒ no vote). Proposer self-case: `publishExecutionPayload`
   already calls `onPayloadProcessed`, and it holds its own columns — count it present.
7. **binaries/simctl**: `-ptc-due` flag on `cmd/slot-sim-node`; `ptc_size` +
   `ptc_due_ms` in `EPBSConfig` (`simctl/config.py`), plumbed through `_host_args` and
   `_simnet_params` (`simctl/runner.py`) + `simnetrun/run_test.go`. PTC on iff epbs on and
   the schedule carries `ptc_voters` (schedule presence gates, like sync/decoupled).
8. **analysis**: kind 12 in `analysis/check_arrivals.py` — reuse the AC-vote shape
   (`_ac_vote_result` / `analyze_ac_votes`: scheduled (slot, val, origin) reaches every
   node but its publisher; headline `fraction_payload_present` from the publish bool).
   SQL twin in `analysis/duck_report.py` (copy `_ac_votes`). Extend
   `tests/test_duck_report.py` with a PTC fixture. `to_parquet.py` needs nothing.
9. **tests**: driver e2e next to `driver/epbs_test.go` — happy path (all present), a
   held-back payload (drop `KindExecutionPayload` to one PTC member ⇒ it votes
   present=false), and a held-back column (custody incomplete ⇒ present=false while the
   attestation still votes block — the gate moved, not died).

## Contracts to keep

- Kind ints and MsgID encoding are pinned by `node/registry_test.go` +
  `metrics/roundtrip_test.go` + the hardcoded ints in `analysis/check_arrivals.py`; extend
  all three together.
- check_arrivals stays the stdlib-only reference; `analysis/duck_report.py` must produce
  the byte-identical report (pinned by `tests/test_duck_report.py`).
- Terminology: say "finality attestation" for the decoupled finality message; never
  "ladder"/"rung" for the decoupled config series.
- Small atomic jj commits, linear history, `Assisted-By:` trailer, 100-char lines.
