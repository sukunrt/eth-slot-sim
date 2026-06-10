# Validator segregation — per-AC-slot finality rounds (decoupled-consensus variant)

Implementation-ready **delta spec** on `decoupled-consensus-spec.md` (read that first; everything
not mentioned here is inherited unchanged). Opt-in via
`decoupled_consensus.validator_segregation: true`.

**The idea:** instead of *all* validators voting once per finality slot in one giant burst, the
validator set is segregated into `k` (= `ac_slots_per_finality_slot`) **round groups** by a stable
per-validator draw. Each AC slot `s` is **round `r = s % k`** of finality slot `n = s / k`: the
~`V/k` validators in round group `r` vote early in the slot, and their votes are aggregated and
disseminated **within that same AC slot** — the aggregation deadline sits at the start of the last
third of the slot (~8 s into 12 s), leaving the last third for aggregate dissemination. After `k`
rounds the finality slot has heard from every validator exactly once.

**What this changes about the measurement:** the base variant's FC traffic is one 25 000-vote/subnet
burst every `k` slots; segregation flips it to a steady ~2 500-vote/subnet hum **every** slot, plus
a per-slot aggregate flood. Same total votes per finality slot, radically different contention
profile against the AC block/column/vote traffic — burst vs. hum is the comparison this variant
exists to produce.

> This is a spec, not code. Decisions are locked in §10; §12 lists risks.

---

## 0. Non-goals (this cut)

Everything in decoupled-consensus-spec §0, plus:

- **Exact (stratified) round/subnet splits.** The round draw is an independent uniform per
  validator, so each (round, subnet) cell is binomial: `≈ V/(k·fs_subnets)` ± noise (~±3 % at 2σ
  for V=100k, k=10, fs=2). **Agreed: random now, exact later** — the fix is swapping the
  `_finality_round_of` draw for a seeded shuffle-and-chunk; nothing downstream changes except the
  cell counts becoming exact (they're carried in the schedule either way, §4).
- **No finality-slot close-out.** Nothing fires when round `k−1` ends — no aggregate-of-aggregates,
  no fslot-level message. The measurement stops at the per-round aggregates (mirrors the base spec
  stopping before on-chain folding).
- **No round-vote content.** Round votes stay fixed-time, un-gated, dissemination-only — the
  content/coupling extension of base §4b applies here identically (per-round, it would couple to
  the round's own AC block, a natural fit later).

---

## 1. Delta summary — what changes vs. the base decoupled spec

| Piece | Base (per-finality-slot) | Segregated (per-AC-slot rounds) |
|---|---|---|
| Who votes when | all V validators at `fslotStart(n) + fc_vote_offset_ms` | round group `r = s % k` (≈`V/k`) at `slotStart(s) + fc_vote_offset_ms` |
| Votes/subnet/event | ≈`V/fs_subnets` (25 000) every `k` slots | ≈`V/(k·fs_subnets)` (2 500) **every slot** |
| Aggregator draw | `fs_aggregators`/subnet per **finality slot** | `fs_aggregators`/subnet per **AC slot** (round) |
| Aggregate deadline | `finality_slot_aggregation_fraction`% of the **finality slot** (50 % ⇒ +60 s) | `round_aggregation_fraction`% of the **AC slot** (67 % ⇒ +8 s) |
| Aggregates/fslot | `fs_subnets·fs_aggregators` = 640, once | 640 **per AC slot** ⇒ 6 400/fslot, 10× count, each ~10× smaller bitfield |
| Aggregate size | `328 + ⌈vps/8⌉` (≈3.4 KB @ 25 k) | `328 + ⌈cell/8⌉` (≈641 B @ 2 500) |
| Runner FC state | `finals` keyed by **finality slot**, spans `k` AC slots | keyed by **AC slot**, spans 2 (pre-join slot + own slot) |
| Pre-join cadence | once per fslot, at AC slot `n·k−1` | **every slot** `s` prepares slot `s+1`'s round |
| Unchanged | AC chain (block + columns + 512-vote flood), subnet membership (`finality_subscribers`), `finality_subnet_of`, topology trees, batched verifier, wire formats/topics/kinds, MsgID/CSV schema | — |

Total FC bytes per fslot rise ~1.9× (votes identical; aggregates 6 400 × ~641 B ≈ 4.1 MB vs.
640 × 3 453 B ≈ 2.2 MB) but spread evenly across the `k` slots instead of bursting.

---

## 2. Clock & knobs

Same one clock (the AC slot). AC slot `s` ⇒ finality slot `n = s/k`, **round `r = s % k`**. Every
AC slot is a round; rounds never span slots. The AC keeps producing its block + columns +
`ac_vote_size` votes every slot, so each round's FC traffic contends with that slot's AC traffic
on each node's single batched verifier — by design.

### New / reinterpreted knobs

| Knob | Meaning | Mainnet | Tests |
|---|---|---|---|
| `validator_segregation` | enable this variant (requires `decoupled_consensus.enabled`) | false | true |
| `round_aggregation_fraction` | % of the **AC slot** when round aggregates publish (last `100−f`% is aggregate-dissemination time) | 67 | 67 |
| `fc_vote_offset_ms` | **reinterpreted**: offset into the **AC slot** (was: finality slot) for the round's vote burst | 1000 | 1000 |
| `fs_aggregators` | **reinterpreted**: aggregator validators per subnet **per round** (was: per fslot) | 16 | 2 |

`finality_slot_aggregation_fraction` is **ignored** under segregation (the round fraction replaces
it); `ac_slots_per_finality_slot` (`k`) now also sets the number of round groups. All other knobs
(`ac_vote_size`, `fs_subnets`, the attestation/data-columns blocks) inherit unchanged.

**Constraints (asserted at generation, added to base's):**
`fc_vote_offset_ms < round_aggregation_fraction% · slot_duration` (votes precede the round's
aggregate — note: a per-AC-slot bound, much tighter than base's per-fslot one);
`0 < round_aggregation_fraction < 100`; `k ≥ 1`. The base constraints (`fs_subnets ≤ N`,
`V ≥ N`, …) all still apply. No `k ≤ fs_subnets` constraint — every subnet is active every round.

---

## 3. The round draw — independent, uniform, stable

**`finality_round_of[v]`** = the round (0..k−1) in which validator `v` votes — an independent
seeded uniform draw per validator (`_rng(seed, 13)`, slot-independent; next free stream after the
base spec's 8/9/10/12), exactly parallel to `finality_subnet_of`. Stable for the whole run: a
validator votes in the same round of every finality slot. Rides `schedule.json` as a plain array
of `V` small ints, next to `finality_subnet_of`.

The two draws are independent, so each **(round, subnet) cell** holds
`≈ V/(k·fs_subnets)` validators ± binomial noise (e.g. N=1000, V=100k, fs=2, k=10 ⇒ 5 000 ± ~69
per cell). **Every subnet is active every round** — round `r`'s votes land on all `fs_subnets`
subnets at ~1/k of the base burst size. Cell counts are carried as
**`validators_per_round_subnet[r][i]`** (`Σ_r` per subnet = `validators_per_subnet[i]`, `ΣΣ = V`)
— the aggregate size model (§6) and the coverage denominator (§8) read them.

Subnet **membership** (`finality_subscribers`, the stable node receiver core, persistent
subscribe at `Prepare`) is untouched: all nodes stay in their one subnet for the whole run;
segregation only changes *which validators' votes* arrive there each slot.

---

## 4. Schedule — generation (Python) and view (Go)

`simctl/schedule.py`, under `decoupled` **and** `validator_segregation`:

- `_finality_round_of(p)` — the per-validator round draw (`_rng(seed, 13)`), emitted as
  `finality_round_of`; `validators_per_round_subnet` accumulated from the two draws (k×fs ints).
- **`fc_aggregators` move from per-finality-slot to per-AC-slot**: every `SlotPlan` (not just
  finality boundaries) gets `finality_aggregators[i]` = `fs_aggregators` validator refs per subnet,
  drawn from the **whole** validator set (`_rng(seed, 10, slot)` — the base stream, now keyed by
  AC slot; the modes are mutually exclusive so the streams can't collide). Same `(val, node)` ref
  shape, same host-dedup rule.
- Everything else (`ac_voters`, `finality_subscribers`, `finality_subnet_of`,
  `validators_per_subnet`, skipping committee/sync gen) is inherited.

`schedule/schedule.go`:

- `Assignment.FinalityRoundOf []int` + `ValidatorsPerRoundSubnet [][]int` (json tags
  `finality_round_of`, `validators_per_round_subnet`). Presence of `finality_round_of` is how Go
  and the analysis side detect the variant from the plan itself.
- `View.FinalityVoteDuties(slot int)` — **gains the slot parameter**: under segregation it filters
  the node's hosted validators to `FinalityRoundOf[val] == slot % k`; in base mode it ignores
  `slot` and returns all (callers: base passes the fslot's first AC slot). Same `(val, subnet)`
  duty shape.
- `View.FinalityAggregations(slot int)` — keyed by **AC slot** under segregation (reads
  `Slots[slot].FinalityAggregators`); base keeps reading the boundary slot `n·k`.

---

## 5. Emit — the per-slot round, riding the existing decoupled paths

The AC-vote path (§6a of the base spec) is untouched. The FC paths retime:

### 5a. Round vote — fixed-time, per-hosted-validator-in-round, un-gated

At `slotStart(s) + fc_vote_offset_ms`, every node publishes one `FinalityVote` per duty in
`view.FinalityVoteDuties(s)` — its hosted validators whose round is `s % k` — each on **its
validator's** drawn subnet (`finality_subnet_of`, unchanged), fan-out where the host isn't a
member. ≈`V/(k·fs_subnets)` messages per subnet per slot. No coupling, no DA gate (locked: the
round vote mirrors the base FC vote; coupling to the round's own block is the §0 content
extension).

### 5b. Round aggregate — per-slot deadline, cell-scaled size

The hosts of `view.FinalityAggregations(s)`'s refs each publish **one** `FinalityAggregate` per
aggregated subnet (host-deduped) on the **global** `FinalityAggregateTopic` at
`slotStart(s) + round_aggregation_fraction% · slot_duration` (~8 s), sized
`FinalityAggregateSize(validators_per_round_subnet[s%k][i])` ≈ 641 B at mainnet cells. 640
aggregates flood globally **every slot**; every node downloads all (`N−1`). The last
`100−round_aggregation_fraction`% of the slot (~4 s — the brief's "last 1/3rd") is the aggregate's
dissemination window, the headline CDF.

### 5c. Pre-join — every slot prepares the next

At `beginSlot(s)`, prepare slot `s+1`'s round (slot 0 prepares itself at its own boundary,
mirroring base `n=0`): vote hosts Join + dial 2 stable members of each non-member duty subnet in
`FinalityVoteDuties(s+1)`; aggregator hosts for `FinalityAggregations(s+1)` dial 2 members +
**Subscribe** each (generally foreign) subnet. Teardown at slot `s+1`'s aggregation deadline,
right after publishing — Unsubscribe + drop dials, **sparing whatever slot `s+2`'s pre-join (live
since `beginSlot(s+1)`) still lists**. The base teardown-sparing logic
(`runner.go:dropFinality`) already handles overlapping pre-joins; under segregation the overlap
happens *every* slot instead of once per fslot, so it's exercised constantly (§12).

### Round timeline (12 s AC slot, `round_aggregation_fraction` = 67)

```
 AC slot:      s−1                          s                              s+1
                |============= round r = s%k of fslot s/k =================|
 pre-join for s ▲   ▲ fc_vote_offset (~1 s):        ▲ +67% (~8 s):         | (pre-join for s+1
 (vote Join+dial│   │ round-r validators' votes     │ 640 cell-scaled      |  ran at beginSlot(s))
  + agg host    │   │ ride their drawn subnets      │ aggregates flood     |
  Subscribe)    │   │ (~V/(k·fs) per subnet)        │ globally; teardown   |
                │   └── votes disseminate ──────────┘  └─ last ~1/3: agg ──┘
                                                          dissemination
 Meanwhile, the same slot: AC block + columns at t=0, 512 AC votes by ~4 s — every slot.
```

---

## 6. Runner state — rounds collapse the fslot-spanning map

Base's one structural novelty — `finals map[int]*finalityState` spanning `k` AC slots — **shrinks**
under segregation: a round lives in exactly two slots (pre-join at `s−1`, everything else inside
`s`). Reuse `finalityState` and the `finals` map, **keyed by AC slot** when segregated:

- `prejoinFinality` runs every `beginSlot` (for `s+1`), not just at `(s+1) % k == 0`; it stores
  `finals[s+1]` with the round's duties, dials, vote/aggregate timers armed off `slotStart(s+1)`.
- The aggregator collects round votes from `onReceive`'s `KindFinalityVote` case into
  `finals[slot]` (votes carry the AC slot, §7) until the round's aggregation deadline.
- Prune `finals[s]` at the round's teardown (the aggregation deadline) — no multi-slot survival
  needed; late vote arrivals still trace via `onReceive` regardless (tracing never depended on
  state). The deliberate-outliving comment on `finals` gets a "base mode only" qualifier.

`DecoupledParams` gains `Segregated bool` + `RoundAggFraction int`; `NodeRunner` branches on
`r.segregated` at the three sites (pre-join cadence, timer instants, duty/aggregation lookups by
slot). Everything else — emit bodies, fan-out, teardown sparing, `aggOnce`/vote once-guards — is
shared with base.

---

## 7. Wire, topics, verify — no changes

**No new proto, kind, topic, or size constant.** `pb.FinalityVote` / `pb.FinalityAggregate` are
reused; under segregation their `finality_slot` field **carries the AC slot** (round `= s % k` and
fslot `= s / k` are derivable; the field is a uint32 either way — a doc-comment note on the proto,
no regeneration). `MakeFinalityVote` / `MakeFinalityAggregate` signatures unchanged (the first arg
is "the slot key"); the aggregate's `vps` arg receives the **cell** count. `batchedTopic` already
routes both topics through the shared batched verifier — the steady per-slot FC load joins the
same single queue as the AC traffic, which is the contention being measured.

---

## 8. Metrics & analysis

- `FinalityVoteID` / `FinalityAggregateID` unchanged — the `fslot` argument carries the **AC
  slot** under segregation (same MsgID fields, no CSV/slog schema change; per-slot keys are
  distinct by construction).
- Coverage per vote: `fs_subscribers[i]` ∪ **that slot's** aggregator hosts for `i`, minus the
  publisher when in the set (same rule, per-slot aggregator set).
- `FinalityCoverageAtDeadline(slot, subnet, due)` — evaluated **per AC slot** at the round's
  aggregation deadline: the fraction of cell `(s%k, i)`'s votes that reached the slot's
  aggregators by `round_aggregation_fraction%`. The denominator is the **actual emitted duty
  count** for the cell (from the schedule draws), not a uniform expectation.
- **Headline metrics:** per-round FC-vote arrival CDF + coverage-at-deadline (now one data point
  per AC slot — `k` per fslot, enough to see warm-up/steady-state), the global round-aggregate
  arrival CDF **relative to the round's aggregation-deadline publish instant** (does it close
  within the slot's last third? — the variant's headline question), and the AC metrics unchanged.
  The cross-variant comparison: base burst CDFs vs. segregated steady-state CDFs at equal
  total-votes-per-fslot.
- `analysis/check_arrivals.py`: `analyze_finality_votes` / `analyze_finality_aggregates` detect
  the variant by `finality_round_of` in `schedule.json` (a `load_finality_rounds` next to
  `load_finality_subscribers`) and build per-AC-slot expected sets — expected voters of slot `s` =
  `{v : finality_round_of[v] == s % k}`, expected aggregates = the slot's draw. Coverage/no-leak/
  dup logic and kinds 8/9 wiring are shared.

---

## 9. Config surface

```yaml
decoupled_consensus:
  enabled: true
  validator_segregation: true       # THE variant flag (default false = base behavior)
  ac_vote_size: 512                 # unchanged
  ac_slots_per_finality_slot: 10    # k: now ALSO the number of round groups
  fs_subnets: 40                    # unchanged; every subnet active every round
  fs_aggregators: 16                # per subnet PER ROUND when segregated (640 aggs/slot)
  round_aggregation_fraction: 67    # % of the AC slot when round aggregates publish (~8 s of 12 s)
  fc_vote_offset_ms: 1000           # now relative to the AC slot start
  # finality_slot_aggregation_fraction: ignored when validator_segregation is on
```

`_check_decoupled_consensus` additions: `validator_segregation` requires `enabled`;
`0 < round_aggregation_fraction < 100`;
`fc_vote_offset_ms < round_aggregation_fraction% · slot_duration`. All base checks (attestation
block for V + deadline, data_columns enabled, `super_node_fraction > 0`, `V ≥ N`,
`fs_subnets ≤ N`, exclusivity with sync/old-attestation) inherit.

---

## 10. Decisions (all answered)

- **Round mapping:** an **independent uniform per-validator draw** (`finality_round_of`,
  stable across the run) — *not* subnet-derived; all subnets active every round at ~1/k load.
  **Random splits accepted now; exact stratified splits are a planned follow-up** (swap the draw).
- **Vote timing:** **fixed-time, un-gated** at `fc_vote_offset_ms` into the AC slot —
  dissemination-only, mirroring the base FC vote; content/coupling deferred (§0).
- **Aggregation deadline:** a **fraction knob** (`round_aggregation_fraction`, default 67 % of the
  AC slot ⇒ publish ~8 s in; the last ~1/3 is aggregate-dissemination time, per the brief).
- **Finality-slot close:** **none** — stop at per-round aggregates.
- **Aggregator scale:** keep `fs_aggregators` = 16 per subnet **per round** (640 aggregates per AC
  slot, 6 400/fslot — 10× base count, each bitfield ~10× smaller, ~1.9× total aggregate bytes
  spread evenly). Bitfield sized to the **(round, subnet) cell** population.
- **Pre-join cadence:** the base 1-slot-ahead dial+Subscribe pattern, now firing **every** slot
  for the next slot's round; the existing teardown-sparing logic covers the constant overlap.
- **Flag shape:** `validator_segregation: bool` inside the `decoupled_consensus` block; when on,
  the per-fslot FC vote/aggregate path is replaced by the per-round path; AC chain, membership,
  topology identical.
- **Identity:** keyed by **AC slot** directly (fslot + round derivable) — wire messages, Make*
  signatures, MsgID shapes, and the CSV/slog schema all unchanged.

---

## 11. Build / test milestones (TDD — each a failing test first, green before the next)

Reuses the base milestones' skeletons (`driver/shared_test.go`, `assertCoverageNoLeakage`,
`raceEnabled` skip). No wire milestone — §7 changes nothing on the wire.

1. **Schedule.** Python emits `finality_round_of` (V ints in [0,k)), per-AC-slot
   `finality_aggregators`, `validators_per_round_subnet` (sums: per-subnet = base counts, total =
   V); Go parses them; `FinalityVoteDuties(s)` returns exactly the hosted validators with round
   `s % k`; `FinalityAggregations(s)` reads slot `s`. Asserts: every validator in exactly one
   round; aggregator refs whole-set, per-slot fresh; the new constraint guards fire;
   same-seed-identical / seed+1-differs. (`tests/test_schedule.py`, `schedule/schedule_test.go`.)
2. **Round vote — segregation + coverage.** In AC slot `s`, **only** round-`s%k` validators emit
   (exact seeded counts per cell); each vote reaches the subnet's members ∪ the **slot's**
   aggregator hosts, minus the publisher when in the set (`got == want` both directions);
   fan-out for non-member hosts; over one full fslot (`k` slots) **every validator votes exactly
   once**. (mirror the base M4 test.)
3. **Round aggregate — per-slot deadline, cell size, overlapping pre-join.** Aggregates publish at
   `slotStart(s) + 67%·slotDur`, size `== 328 + ⌈cell/8⌉` (the cell count, not the subnet count),
   `N−1` global coverage; a non-member aggregator host dialed + Subscribed at slot `s−1` and tore
   down at the deadline; **a host aggregating the same subnet in rounds r and r+1 keeps the
   subscription across the boundary** (the sparing logic under per-slot overlap). (mirror base M5.)
4. **Full run + metrics + analysis.** A sized run (≥ `k` + settle slots) writes per-slot FC-vote
   CDFs + coverage-at-deadline and per-slot aggregate CDFs; assert the aggregate CDF is measured
   against the per-round deadline; Python `analyze_*` confirm round-filtered coverage/no-leak.
   Two bubbles, same seed → byte-identical sorted arrivals. Race-skip if sized. (mirror base M6.)

**Sizing for M4:** the base M6 box — `N=16`, `V=32`, `k=2`, `fs_subnets=2`, `fs_aggregators=2`,
`ac_vote_size=8`, `num_columns=8` — now means 2 round groups of ~16 validators, ~8 votes per
(round, subnet) cell per slot, 4 aggregates **per slot**. Measure rounds in fslot 1 (AC slots
2–3); run ≥5 AC slots.

---

## 12. Open implementation risks

- **Per-slot pre-join/teardown churn.** The dial+Subscribe/Unsubscribe cycle and the
  teardown-sparing logic now run every slot instead of every `k` — a host can simultaneously hold
  round `r`'s mesh (until its deadline) and round `r+1`'s pre-join. M3's
  consecutive-rounds-same-subnet assertion pins the sparing; watch for dial-capacity pressure at
  small N (16 nodes × 2 aggregators × 2 dials per slot is fine; verify at mainnet sizing).
- **The tight per-slot ordering constraint.** `fc_vote_offset (1 s) < agg deadline (8 s) < slot
  end (12 s)` leaves ~7 s for vote spread + verify under full AC contention and ~4 s for the
  aggregate flood. If the vote tail crosses the deadline, coverage-at-deadline drops — that's the
  *metric*, not a bug, but M4 should sanity-check the aggregate CDF closes within the slot.
- **Binomial cell noise at test sizes.** With V=32, k=2, fs=2, a cell is 8 ± ~2.4; tests must
  assert against the **actual seeded draw counts** (as the base membership tests do), never the
  uniform expectation. An empty cell is legal (an aggregator then publishes a base-size aggregate
  over zero votes) — pick the test seed to avoid it or assert it harmlessly.
- **Mode detection in analysis.** The analyzers key the expected sets off `finality_round_of`'s
  presence in `schedule.json`. A schedule generated with segregation but a binary run without (or
  vice versa) must fail loudly — thread the flag through `cmd/slot-sim-node` (`-fc-segregated`)
  and assert it against the schedule's shape at startup.

---

## 13. Implementation map (files, smallest-first — matches §11)

- **`simctl/schedule.py`:** `_finality_round_of` (`_rng(seed,13)`); `validators_per_round_subnet`;
  per-AC-slot `fc_aggregators` (`_rng(seed,10,slot)`) when segregated; `to_dict` keys; `Params`
  field; the new asserts.
- **`schedule/schedule.go`:** `FinalityRoundOf` + `ValidatorsPerRoundSubnet` on `Assignment`;
  `FinalityVoteDuties(slot)` (round filter) + `FinalityAggregations(slot)` (per-slot key).
- **`simctl/config.py`:** `validator_segregation` + `round_aggregation_fraction` on
  `DecoupledConsensusConfig`; `_check_decoupled_consensus` additions (§9).
- **`driver/driver.go`:** `DecoupledParams.Segregated` + `RoundAggFraction`.
  **`driver/runner.go`:** per-slot `prejoinFinality` cadence; `finals` keyed by AC slot when
  segregated; vote/aggregate timers off `slotStart(s)`; prune at the round teardown; duty lookups
  by slot. Emit bodies shared.
- **`pb/decoupled.proto`:** doc-comment only (`finality_slot` carries the AC slot when
  segregated); no regeneration needed.
- **`metrics/tracer.go`:** doc-comment on `FinalityVoteID`/`FinalityAggregateID` (slot semantics);
  `FinalityCoverageAtDeadline` already takes `(fslot, subnet, due)` — called per AC slot.
- **`analysis/check_arrivals.py`:** `load_finality_rounds`; round-filtered expected sets in
  `analyze_finality_votes` / `analyze_finality_aggregates`; per-slot aggregate expectations.
- **`cmd/slot-sim-node/main.go`:** `-fc-segregated` + `-fc-round-agg-fraction` flags; the startup
  shape assert (§12). **`simnetrun/run_test.go`:** the segregation key. **`simctl/runner.py`:**
  thread both into Params / host args / simnet params.
- **Unchanged:** `validator/decoupled.go`, `node/` (registry, verifier, `batchedTopic`),
  `netsim/subnet.go`, `simctl/topology.py`, the CSV/slog schema.

---

## 14. Summary

Validator segregation turns the finality chain's once-per-fslot burst into `k` per-AC-slot
**rounds**: a stable uniform draw (`finality_round_of`) splits the validator set into `k` groups;
in AC slot `s`, group `s % k` (≈`V/k`) emits fixed-time, un-gated `FinalityVote`s on their drawn
subnets (~`V/(k·fs_subnets)` per subnet, all subnets active every round), and a per-slot draw of
`fs_aggregators` whole-set validators per subnet publishes cell-scaled `FinalityAggregate`s
(`328 + ⌈cell/8⌉` ≈ 641 B) on the global topic at `round_aggregation_fraction` (67 %) of the slot
— the last third is aggregate-dissemination time. Pre-join runs one slot ahead, every slot. Same
total votes per finality slot as base, ~1.9× aggregate bytes, spread evenly — **burst vs. hum** on
the same shared batched verifier, against the same AC traffic. Nothing changes on the wire, in the
topology, or in the metrics schema; the schedule grows two arrays and re-keys the aggregator draw;
the runner re-keys its finality state from fslots to slots. Headline output: per-round vote CDFs +
coverage-at-the-round-deadline and the per-round aggregate CDF — does the round's aggregate close
inside its own slot's last third?
