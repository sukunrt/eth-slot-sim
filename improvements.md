# Improvements

> Status: item 4 (AGENTS.md) is DONE — architecture map + contracts + conventions;
> CLAUDE.md symlinks to it.
> Item 1 (message-type registry) is DONE — `node/registry.go` holds the per-kind
> table (topic match → decode → identity/origin → verify class); lookup is exact-topic
> first, then prefix (order-free, init-validated). `metrics/roundtrip_test.go` pins the
> publish/receive identity join; Kind values 1..9 are pinned for the Python CSV contract.
> Go-side per-kind additions are now: proto + `validator.Make*` + one registry entry
> (+ the Python analyzer, until item 3 lands).

Maintainability review of the codebase after the decoupled-consensus milestones (M1–M6).
Overall: the layering is good — `node` is passive transport, `driver` owns timing, `schedule`
is pure data, comments are thorough. The dominant tax is that **every new message type or
protocol phase is added by copy-paste across ~8 places**; decoupled consensus (the third
phase) made the pattern creak. Items below are ordered by leverage.

## 1. Message-type registry (biggest win)

Adding one message kind today touches:

1. a `.proto` in `pb/`
2. a `Make*` constructor + topic helper in `validator/`
3. a `node.Kind` const (`node/node.go`)
4. a case in `node.decode()` (`node/node.go:401-460`)
5. a near-identical case in `NodeRunner.onReceive` (`driver/runner.go:523-584`) — nine
   copies of "type-assert, skip own loopback, trace"
6. a `metrics.*ID` constructor (`metrics/tracer.go`)
7. an analyzer pair in `analysis/check_arrivals.py`
8. test fixtures

Fix: a per-kind descriptor table (topic matcher → unmarshal → MsgID extractor → origin
extractor). `decode` and `onReceive` become lookups; "add a message type" becomes one table
entry plus a proto. Bonus: kills the fragile case-ordering in `decode` (the "contribution
before message prefix — shared stem" comments) by checking prefix ambiguity at init.

Forward-compat note (multi-block slots / fork tests — nodes attesting to different blocks in
the same slot): the registry is orthogonal to this and doesn't make it harder. Message
identity already distinguishes blocks by origin (`BlockID(slot, origin)`), and the wire
attestation already carries `voted_origin`. The work for that experiment lives elsewhere:
`slotState` tracks only the FIRST seen block (`seen/seenAt/seenOrigin`,
driver/runner.go:87-89) and would need to hold all seen blocks plus a vote rule;
`SlotPlan.Proposer` is single-valued; and the tracer's `votedBlock bool` would become a
voted-block identity (see item 6). The registry leaves the decode/trace plumbing unchanged
through all of that.

## 2. Tame NodeRunner (driver/runner.go, 831 lines)

- `NewRunner` takes 14 positional params including two bare bools — pass a config struct
  (a runner-level subset of `driver.Config`).
- The "decoupled forces attest/sync off" invariant is enforced in **both** `driver.New`
  (`driver/driver.go:124-127`) and `cmd/slot-sim-node/main.go`. Encode it once — a phase
  enum (`BlockOnly | Attest | Decoupled`, sync as an orthogonal flag), or let `NewRunner`
  do the forcing itself.
- Split the file by phase: `runner.go` (core loop + column gate), `runner_sync.go`,
  `runner_decoupled.go`. The finality-state machinery (`armFinality` / `prejoinFinality` /
  `reapFinality`) is self-contained and moves cleanly.
- `DecoupledParams` fields are copied one-by-one into `r.k` / `r.fcVoteOffset` /
  `r.fcAggFraction` — just store the struct pointer.

## 3. Python analyzer factory

`analysis/check_arrivals.py` has 8 Shadow+CSV analyzer pairs (~16 functions, ~500 lines)
differing only in kind, key extraction, and expected-coverage set; decoupled consensus added
6 of them by copy-paste. A generic `analyze(kind, key_fn, coverage_fn)` core cuts ~40% and
makes coverage-logic fixes apply everywhere.

Same medicine for `simctl/runner.py`: it gates on `config.X is not None and config.X.enabled`
in three separate functions (`_host_args`, `_simnet_params`, `run_comparison`). A small phase
registry (config class → flag emitter → analyzer) makes a new phase a one-entry change.
Related: `simctl/config.py` has five parallel config classes with three near-identical
`@model_validator` methods — a shared validation pattern or discriminated union would
centralize them.

## 4. Write AGENTS.md / CLAUDE.md (cheapest, highest value)

Both are **empty (0 bytes)**. Write a short architecture map: package roles, the
Python-generates / Go-consumes schedule seam, the two backends (netsim in-process vs Shadow
one-process-per-node), how to run tests, how to regenerate protobufs, and the glossary below.

## 5. Glossary — naming drift

The same concept goes by different names per layer. Canonical-names table (also belongs in
decoupled-consensus-spec.md):

| Concept | Spec / prose | schedule.json (Python) | Go | Other aliases |
|---|---|---|---|---|
| Finality subnet membership (stable node receiver core) | "FS subnets" | `finality_subscribers` | `FinalitySubscribers` | `fin_subs` (simctl/runner.py local), `fcvote` (batch class, node/node.go) |
| Finality voting subnet (per-validator draw) | "validator partition" | `finality_subnet_of` | `FinalitySubnetOf`, `FinalityVoteDuties` (pairs) | — |
| Finality subnet count | `FS subnets` | `fs_subnets` | `FsSubnets` | — |
| AC slots per finality slot | `AC_SLOTS_PER_FINALITY_SLOT`, "k" | `ac_slots_per_finality_slot` | `AcSlotsPerFinalitySlot`, `DecoupledParams.K`, `r.k` | — |
| Availability-chain vote | "AC vote" | `ac_voters`, `ac_vote_size` | `KindACVote`, `ACVoteDuties`, `MakeACVote` | topic `availability_vote`, batch class `ac` |
| Aggregation deadline fraction | `FINALITY_SLOT_AGGREGATION_FRACTION` | — | `FCAggFraction`, `fcAggFraction` | batch class `fcagg` |
| Finality attestation (the per-validator finality-chain message) | "finality attestation" (canonical); "FC vote" / "finality vote" are aliases | — (per-validator, derived) | `KindFinalityVote`, `MakeFinalityVote`, `pb.FinalityVote` | "finality vote" (analysis output), batch class `fcvote` |
| Validator segregation (per-AC-slot finality rounds) | "validator segregation", "round" | `validator_segregation` (config), `finality_round_of` | `Segregated()` (schedule), `DecoupledParams.Segregated`, `FinalityRoundOf` | `-fc-segregated` (flag), `fc_segregated` (simnet params) |
| Round aggregation deadline fraction | `round_aggregation_fraction` | — (config-only) | `RoundAggFraction`, `roundAggFraction` | `-fc-round-agg-fraction` (flag), `fc_round_agg_fraction` (simnet params) |
| (Round, subnet) cell counts | "cell" | `validators_per_round_subnet` | `ValidatorsPerRoundSubnet`, `validatorsPerCell` | — |
| Attestation subnet membership | "subscribers" | `subnet_subscribers` | `SubnetSubscribers`, `Subscribers(s)` | `subscribers` (analysis param) |
| Sync subnet membership | — | `sync_subscribers` | `SyncSubscribers` | — |
| Validator distribution (the Dist seam) | "validator distribution", "skew" | `validator_counts` | `ValidatorCounts` | `dist`/`counts` (schedule.Params), skewed-validators-spec.md |

Decide one prefix per concept (suggest: `finality_*` everywhere, drop `fs_*` / `fc*`;
`ac_*` everywhere for the availability chain) and rename in one pass.

## 6. metrics.MsgID field overloading

`Subnet` carries the column index for columns; `Attester` carries "aggregator node OR
validator id OR member node"; `Origin` is -1 for aggregates because the CSV has no origin
column. Each `*ID` constructor re-explains the smuggling. Either add an origin column to the
CSV and stop overloading, or document the encoding once in a table next to `MsgID`
(`metrics/tracer.go:25`). If the fork experiment (item 1's note) lands, `votedBlock bool`
becomes a voted-block identity (the voted proposer's origin) — prefer the CSV-column route
then, since the bool join breaks anyway.

## 7. Shared test fixtures

`genAssignment` (driver/scenario_test.go), `genSyncAssignment` (driver/sync_fullrun_test.go),
and `sizedDecoupledAssignment` (driver/decoupled_fullrun_test.go) — ~120 lines across three
files — each hand-roll schedule construction. One builder in a shared test helper keeps them
in sync when schedule generation changes.

## 8. Guard generated code and config mirrors

- No check that `pb/*.pb.go` matches the protos: add
  `go generate ./pb && git diff --exit-code` (plus `go vet`, modernize) to CI / a check
  script.
- `simnetrun/run_test.go`'s params struct hand-mirrors the Python `SimConfig`; config plumbing
  is duplicated three ways (Python config → simctl CLI flags, → simnet params JSON, →
  `driver.Config`). At minimum keep flag names = config field names and add a round-trip test
  on the param names.

## 9. schedule.View membership lookups

`SyncSubnet`, `FinalitySubnet`, `CustodyColumns`, `SubscribedSubnets` scan full subscriber
lists with `slices.Contains` per call, per slot, per node. Harmless at current scale, but a
per-node index built once in `Load` is both faster and clearer.

## 10. Doc drift

`decoupled-consensus-spec.md` §1's reuse table and `run.md` reference code locations that
have since moved (e.g. flag plumbing lives in `cmd/slot-sim-node/main.go`, runner wiring in
`driver/runner.go:Prepare`, not `coupling.go` alone; `run.md` misses the `meshJoinStagger`
logic in main.go). Add a short "code locations" section per spec and refresh it when files
move.

---

Suggested order: 4 (docs) → 1 (registry) → 2 (runner) → 3 (Python) → rest opportunistically.
