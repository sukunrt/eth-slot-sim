# Task: finality subnets must partition VALIDATORS, not nodes

## The bug (design, present since the decoupled cut — not introduced by skew)

`fs_subnets` is a **node** partition: `_finality_subscribers` assigns every node one random
subnet (`simctl/schedule.py`), and all validators a node hosts vote on that one subnet
(`FinalityVoteDuties` + `FinalitySubnet` in `schedule/schedule.go`). Consequences:

- validators-per-subnet is only balanced when hosting is uniform; under tiered skew a
  1000-key supernode dumps all 1000 votes on one subnet (lumpy `validators_per_subnet`),
- even uniform hosting balances only because V/N is constant per node.

This is NOT the intended design: subnets should partition the **validator set** (like
attestation committees), each getting ~V/fs_subnets votes regardless of where keys live.

## Intended semantics (per the user, 2026-06-10)

- Validator v's finality subnet is **completely random**: an independent seeded uniform
  draw over fs_subnets per validator, in Python (the one-plan principle — do NOT derive it
  with a closed form like v % S, and do NOT re-draw in Go). Subnet sizes are then ~V/S ±
  binomial noise, not exact.
- The map must ride schedule.json: `finality_subnet_of[v]` (array of V small ints; ~1–3 MB
  JSON at V = 400k). Each of the 4000 Shadow processes parses it once at startup — fine
  inside the 180 s bring-up window. If size ever matters, per-subnet validator lists or a
  packed encoding are equivalent; start with the plain array.
- **Publish is fanout, like non-decoupled attestations** (att-subnet.md): nodes do NOT
  subscribe wherever they have keys. `finality_subscribers[i]` stays a STABLE subscriber
  set (the subnet's mesh/receiver core — keeping the existing node partition as that set is
  fine), and when a node's validator votes on a subnet the node is not subscribed to, it
  dials that subnet's subscribers and publishes via gossipsub fanout. The attestation phase
  already has exactly this machinery (publishers dial into the subscriber set per slot) —
  reuse it, don't invent a new path.
- Coverage expectation per vote: `finality_subscribers[subnet]`, minus the publisher only
  when the publisher is itself a member. `validators_per_subnet[i]` = the draw's per-subnet
  counts (~V/S ± noise), now decoupled from member-node hosting.

## Touchpoints

- `simctl/schedule.py`: draw `finality_subnet_of[v]` (seeded, after the Dist draw so V is
  known); `_finality_subscribers` derives node membership from hosting + the draw;
  `_validators_per_subnet` counts the draw; emit `finality_subnet_of` in to_dict.
- `schedule/schedule.go`: load `FinalitySubnetOf []int`; `FinalitySubnet() (int, bool)` →
  `FinalitySubnets() []int` (or keep + add); `FinalityVoteDuties() []int` →
  `[]AttestDuty`-style (val, subnet) pairs from the map.
- `driver/runner.go` + `node/`: subscription stays membership-based (the stable
  finality_subscribers partition); vote emission groups the node's hosted validators by
  finality_subnet_of and publishes each group on its subnet — fanout (dial + publish
  without subscribe) when not a member, mirroring the attestation publish path.
- `analysis/check_arrivals.py`: `_finality_votes` reads `finality_subnet_of` for each
  hosted val; expected receivers = the subnet's stable members (publisher excluded only
  if a member).
- `validator/decoupled.go` MakeFinalityVote: unchanged (already takes subnet).
- Tests: schedule (membership = derived, balanced vps, uniform S|N edge), Go duties pairs,
  driver fullrun (a node with keys on 2 subnets votes on both), analyzer.
- Spec updates: decoupled-consensus-spec.md (the "node-partitioned" language, §10 config
  comments), skewed-validators-spec.md §6 (the lumpy-vps bullet becomes obsolete),
  configs/*.yaml comments ("node-partitioned (every node on one)").

## Sequencing (one fresh session)

Do this BEFORE the per_item_verification_delay.md rerun — it changes finality-flood load
distribution materially (supernodes go from 1 subnet to ~all), so the honest n4000 rerun
should carry both.
