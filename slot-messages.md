# Slot Messages — Ethereum Consensus-Layer Message Catalog

Reference for the slot simulator. We model **message size**, **emit timing**, and
**response pattern** only. All payloads are opaque bytes — no crypto, no validity, no
contents. A "block" is N bytes that arrive, get "processed" for some delay, and trigger
the next message.

- **Spec basis:** `consensus-specs` v1.7.0-alpha.7, `configs/mainnet.yaml` + `presets/mainnet`.
- **Live fork:** Fulu / Fusaka (mainnet 2025-12-03). PeerDAS is live; blob gossip is replaced
  by data-column gossip.
- **Next fork (not live):** Gloas / ePBS — restructures the slot; see §8.

---

## 1. System model & notation

| Symbol  | Meaning                                                        | Mainnet scale (≈2026) |
|---------|---------------------------------------------------------------|-----------------------|
| `V`     | active validators                                             | ~1.0–1.1 M            |
| `N`     | beacon (P2P) nodes — the gossip graph                         | ~1×10⁴                |
| `V/N`   | validators per node (mean; **highly skewed** by big operators)| ~100                  |
| `C`     | committees per slot                                           | 64                    |
| `s_c`   | committee size (validators)                                   | ~512                  |
| `B`     | blobs in the slot's block (0 … `MAX_BLOBS_PER_BLOCK`)         | 0–9 (rising, §7)      |
| `D`     | GossipSub mesh degree per topic                               | ~8                    |

**Validators vs nodes.** `V` validators are *duties*; `N` nodes are the *machines* that
gossip. One node runs 0…thousands of validators. Message **counts** scale with `V`
(duties); dissemination **hops/latency** scale with `N` (graph size). Keep them separate —
this split is the lever for the scaling model (separate doc).

**Derived per-slot quantities** (every validator attests exactly once per epoch):

```
attesters/slot      = V / SLOTS_PER_EPOCH            = V / 32
C (committees/slot)  = min(64, floor((V/32) / 128))   = 64  for V >= 262,144  (always, at scale)
s_c (committee size) = (V/32) / C                      = V / 2048   (when C=64)
```

---

## 2. Topology — global topics vs. subnets

Each GossipSub **topic** is its own mesh (degree ~`D`). Messages flood the mesh as
full payloads after local validation; `IHAVE`/`IWANT` handle gossip/recovery. Who
subscribes to what is the structure that matters:

| Topic class                | # meshes | Who subscribes                                                            |
|----------------------------|----------|--------------------------------------------------------------------------|
| **Global**                 | 1 each   | **every** node (all `N`)                                                  |
| **Attestation subnets**    | 64       | `SUBNETS_PER_NODE=2` stable backbone (per node-id, 256 epochs) + duty-based short-lived |
| **Sync subnets**           | 4        | nodes holding a sync-committee validator, for the ~27 h period           |
| **Data-column subnets**    | 128      | by **custody**: ≥`CUSTODY_REQUIREMENT=4`; `VALIDATOR_CUSTODY_REQUIREMENT=8` if validating; +1 per +32 ETH; 128 = supernode |

Consequence: a global-topic message crosses a graph of size `N`; a subnet message
crosses a *sub-graph* of only the subscribed nodes. Custody means **no node downloads all
column data** — the core PeerDAS scaling property.

---

## 3. Slot timeline & response pattern

`SLOT_DURATION_MS = 12000`. Phase deadlines are **basis points of the slot**
(1 bp = 0.01 %), so they rescale automatically if you shorten the slot — exactly the knob
this simulator exists to explore.

| Deadline (bp)             | value | % slot | t @ 12 s | Event                                            |
|---------------------------|-------|--------|----------|--------------------------------------------------|
| slot start                | 0     | 0 %    | 0.0 s    | proposer publishes **block + 128 columns**       |
| `PROPOSER_REORG_CUTOFF`   | 1667  | 16.67 %| 2.0 s    | late-block reorg cutoff (proposer-side)          |
| `ATTESTATION_DUE`         | 3333  | 33.33 %| 4.0 s    | **attestations** + **sync messages** due         |
| `SYNC_MESSAGE_DUE`        | 3333  | 33.33 %| 4.0 s    | (same instant)                                   |
| `AGGREGATE_DUE`           | 6667  | 66.67 %| 8.0 s    | **aggregates** + **sync contributions** due      |
| `CONTRIBUTION_DUE`        | 6667  | 66.67 %| 8.0 s    | (same instant)                                   |

```
 t=0s            t=2s              t=4s                       t=8s            t=12s
  |---------------|------------------|--------------------------|---------------|
  block +         reorg            attestation              aggregate        next
  columns         cutoff           + sync-msg due           + contrib due    slot
  (proposer)                       (all attesters)          (aggregators)
```

**Response pattern (the thing the sim measures).** This is the dependency chain to encode:

```
proposer ── block+columns ─▶ peer receives ─▶ "process" Δp ─▶ (have block by ATTESTATION_DUE?)
                                                                   │
                          attester emits SingleAttestation ◀───────┘  at max(now, ATTESTATION_DUE)
                                   │
            subnet aggregator collects subnet ─▶ "process" Δa ─▶ emits Aggregate at AGGREGATE_DUE
```

Each edge = a message of known size; each node = a processing delay before it emits. The
output we want: **CDF of arrival time** for each message type across the `N` nodes.

---

## 4. Message catalog

Counts/sizes are per slot. Sizes are SSZ-serialized (pre-snappy); see §5 for the byte math.

### 4.1 Block phase — emitted at t=0 by the proposer

| Message (topic)                  | Container            | Scope        | Count/slot      | Size                              |
|----------------------------------|----------------------|--------------|-----------------|-----------------------------------|
| `beacon_block`                   | `SignedBeaconBlock`  | global       | 1               | **variable**; exec payload dominates. Typ. ~100–300 KB; cap `MAX_PAYLOAD_SIZE` = 10 MiB |
| `data_column_sidecar_{0..127}`   | `DataColumnSidecar`  | 128 subnets  | 128 (if `B`>0)  | **`B·2144 + 356` B/column** (B=9 → ~19 KiB; B=72 → ~151 KiB) |

- The proposer publishes the block on the global topic **and** one column on each of the
  128 column subnets (in MEV-boost today the builder's block reaches the proposer via the
  relay, which shifts the effective t=0 — model as a per-slot proposer-publish offset).
- `blob_sidecar_{subnet_id}` is **deprecated** in Fulu — do not model blob gossip; use columns.

### 4.2 Attestation phase — emitted at `ATTESTATION_DUE` (t≈4 s)

| Message (topic)                | Container             | Scope       | Count/slot                     | Size   |
|--------------------------------|-----------------------|-------------|--------------------------------|--------|
| `beacon_attestation_{0..63}`   | `SingleAttestation`   | 64 subnets  | `V/32` total; ≈`s_c` per subnet (one committee→one subnet when C=64) | 240 B  |
| `sync_committee_{0..3}`        | `SyncCommitteeMessage`| 4 subnets   | 512 total; 128 per subnet      | 144 B  |

- Trigger: validator emits at `max(block_seen_time + Δprocess, ATTESTATION_DUE)`. A node
  that hasn't seen the block by the deadline attests to the prior head (models late blocks).
- **Dominant by message count** — `V/32 ≈ 33 k` single attestations per slot.

### 4.3 Aggregation phase — emitted at `AGGREGATE_DUE` (t≈8 s)

| Message (topic)                              | Container                    | Scope  | Count/slot       | Size                    |
|----------------------------------------------|------------------------------|--------|------------------|-------------------------|
| `beacon_aggregate_and_proof`                 | `SignedAggregateAndProof`    | global | `16·C` = **1024**| ~0.5 KB (grows w/ `s_c`)|
| `sync_committee_contribution_and_proof`      | `SignedContributionAndProof` | global | `16·4` = **64**  | ~360 B                  |

- `TARGET_AGGREGATORS_PER_COMMITTEE = 16`, `TARGET_AGGREGATORS_PER_SYNC_SUBCOMMITTEE = 16`.
- Aggregator listens on its subnet from t≈4 s, aggregates what it received, publishes
  globally at t≈8 s. Every node downloads **all** ~1024 aggregates (global topic).

### 4.4 Sporadic / low-rate — global, negligible for slot timing (listed for completeness)

| Message (topic)            | Container                     | Typical rate | Size                          |
|----------------------------|-------------------------------|--------------|-------------------------------|
| `voluntary_exit`           | `SignedVoluntaryExit`         | ~0/slot      | 112 B                         |
| `proposer_slashing`        | `ProposerSlashing`            | rare         | 416 B                         |
| `attester_slashing`        | `AttesterSlashing` (Electra)  | rare         | large/variable (2× IndexedAttestation) |
| `bls_to_execution_change`  | `SignedBLSToExecutionChange`  | ~0/slot now  | 172 B                         |

Deposits are EL requests embedded in the block (EIP-6110) — **no gossip message**.

### 4.5 Req/Resp domain — unicast (not mesh-amplified), per-slot-relevant subset

| Method                                   | Purpose                                                            |
|------------------------------------------|-------------------------------------------------------------------|
| `DataColumnSidecarsByRoot` / `ByRange`   | **PeerDAS sampling**: each node samples `SAMPLES_PER_SLOT=8` non-custodied columns/slot; + reconstruction / cross-seeding |
| `BeaconBlocksByRoot` / `ByRange`         | block fetch & gap recovery when gossip misses                     |
| `Status`, `Goodbye`, `Ping`, `MetaData`  | peering housekeeping                                              |

Req/Resp is point-to-point (not multiplied by the mesh) but governs **tail latency** and
**DA confirmation** — a node gains data-availability confidence only once sampling
succeeds. Model sampling as `8` small request/response round-trips per node per slot.

---

## 5. Size reference (SSZ byte math — auditable)

Common parts: `Root=32`, `BLSSignature=96`, `KZGCommitment=KZGProof=48`,
`Slot=ValidatorIndex=Epoch=uint64=8`, `Checkpoint=40`, `AttestationData=128`
(`slot 8 + index 8 + beacon_block_root 32 + source 40 + target 40`),
`BeaconBlockHeader=112`, `SignedBeaconBlockHeader=208`.

| Container                     | Size (B)               | Breakdown                                                                 |
|-------------------------------|------------------------|--------------------------------------------------------------------------|
| `SingleAttestation`           | **240**                | committee_index 8 + attester_index 8 + data 128 + sig 96                  |
| `SyncCommitteeMessage`        | **144**                | slot 8 + root 32 + validator_index 8 + sig 96                            |
| `SignedAggregateAndProof`     | **~505** (@ s_c=512)   | sig 96 + aggregator_index 8 + selection_proof 96 + Attestation(data 128 + sig 96 + committee_bits 8 + aggregation_bits ⌈(s_c+1)/8⌉ + offsets); grows ~`s_c/8` |
| `SignedContributionAndProof`  | **360**                | sig 96 + aggregator_index 8 + selection_proof 96 + Contribution(160: slot 8 + root 32 + subcmt 8 + bits 16 + sig 96) |
| `DataColumnSidecar`           | **`B·2144 + 356`**     | index 8 + 3 offsets 12 + header 208 + inclusion_proof(4·32=128) + per-blob: cell 2048 (64 field-elems·32) + commitment 48 + proof 48 |
| `SignedBeaconBlock`           | variable               | consensus body modest (≤`MAX_ATTESTATIONS_ELECTRA=8` on-chain aggregates, each up to ~16 KB w/ EIP-7549 full bits) + **execution payload (transactions) dominates**; cap 10 MiB |
| `BlobSidecar` *(deprecated)*  | ~131.9 KiB             | blob 131072 + commitment 48 + proof 48 + header 208 + inclusion_proof(17·32) — for reference only; not gossiped post-Fulu |

---

## 6. Per-slot volume summary

Origination volume (before mesh duplication). Concrete column: `V=1.05M`, `N=10k`,
`B=9`, `C=64`, `s_c=512`.

| Message                       | Count/slot     | Origin bytes/slot      | Concrete bytes | Reach            |
|-------------------------------|----------------|------------------------|----------------|------------------|
| `beacon_block`                | 1              | ~block size            | ~200 KB        | global (all `N`) |
| `data_column_sidecar`         | 128            | `128·(B·2144+356)`     | ~2.5 MB        | per-subnet (≥4/node custody) |
| `beacon_attestation`          | `V/32`         | `(V/32)·240`           | ~7.9 MB        | per-subnet (~`s_c` each) |
| `beacon_aggregate_and_proof`  | 1024           | `1024·505`             | ~517 KB        | global (all `N`) |
| `sync_committee`              | 512            | `512·144`              | ~74 KB         | per-subnet (4)   |
| `sync_..._contribution`       | 64             | `64·360`               | ~23 KB         | global (all `N`) |

Takeaways for the sim:
- **By count:** single attestations dominate (~33 k/slot) — but each lives on only 1 of 64
  subnets, so per-node attestation load ≈ `s_c` × (subscribed subnets), not the full `V/32`.
- **By bytes:** block + columns dominate (~2.7 MB/slot origin) — but columns are sharded
  128-way and custody-gated, so per-node column ingress ≈ `(custody groups)·(B·2144+356)`
  (~79 KB at 4 groups, B=9), *not* 2.5 MB.
- **Global must-download per node:** block + all aggregates + all sync contributions
  ≈ **~740 KB/slot** before mesh duplication (×~`D` worst-case received copies, ~1–3 typical).
- These are **origination** figures; dissemination latency/bandwidth (mesh fanout `D`, hop
  count ~`log_D N`, per-link RTT/bandwidth) is the subject of the scaling doc.

---

## 7. Constants appendix (mainnet, as used above)

| Constant                              | Value      | Source                  |
|---------------------------------------|------------|-------------------------|
| `SLOT_DURATION_MS`                    | 12000      | configs/mainnet         |
| `SLOTS_PER_EPOCH`                     | 32         | presets                 |
| `ATTESTATION_DUE_BPS` / `SYNC_MESSAGE_DUE_BPS` | 3333 | configs/mainnet      |
| `AGGREGATE_DUE_BPS` / `CONTRIBUTION_DUE_BPS`   | 6667 | configs/mainnet      |
| `PROPOSER_REORG_CUTOFF_BPS`           | 1667       | configs/mainnet         |
| `MAX_COMMITTEES_PER_SLOT`             | 64         | presets/phase0          |
| `TARGET_COMMITTEE_SIZE`               | 128        | presets/phase0          |
| `MAX_VALIDATORS_PER_COMMITTEE`        | 2048       | presets/phase0          |
| `ATTESTATION_SUBNET_COUNT`            | 64         | configs/mainnet         |
| `SUBNETS_PER_NODE`                    | 2          | configs/mainnet         |
| `EPOCHS_PER_SUBNET_SUBSCRIPTION`      | 256        | configs/mainnet         |
| `TARGET_AGGREGATORS_PER_COMMITTEE`    | 16         | specs/phase0/validator  |
| `SYNC_COMMITTEE_SIZE`                 | 512        | presets/altair          |
| `SYNC_COMMITTEE_SUBNET_COUNT`         | 4          | specs/altair            |
| `TARGET_AGGREGATORS_PER_SYNC_SUBCOMMITTEE` | 16    | specs/altair/validator  |
| `MAX_ATTESTATIONS_ELECTRA`            | 8          | presets/electra         |
| `MAX_BLOBS_PER_BLOCK_ELECTRA`         | 9          | configs/mainnet         |
| `BLOB_SCHEDULE` (BPO forks)           | 15, 21, … (→72) | configs/mainnet     |
| `NUMBER_OF_COLUMNS`                   | 128        | presets/fulu            |
| `DATA_COLUMN_SIDECAR_SUBNET_COUNT`    | 128        | configs/mainnet         |
| `NUMBER_OF_CUSTODY_GROUPS`            | 128        | configs/mainnet         |
| `CUSTODY_REQUIREMENT`                 | 4          | configs/mainnet         |
| `VALIDATOR_CUSTODY_REQUIREMENT`       | 8          | configs/mainnet         |
| `SAMPLES_PER_SLOT`                    | 8          | configs/mainnet         |
| `FIELD_ELEMENTS_PER_CELL`             | 64         | presets/fulu            |
| `MAX_PAYLOAD_SIZE`                    | 10 MiB     | configs/mainnet         |

---

## 8. Forward-looking: ePBS (Gloas) — not yet live

Gloas splits today's single block into a **bid** + a separately-propagated **payload**,
and adds a payload-timeliness committee (PTC). This materially changes the slot's
dissemination profile, so flag it but don't model it yet.

New gossip topics / containers:

| Topic                              | Container                        | Role                                  |
|------------------------------------|----------------------------------|---------------------------------------|
| `execution_payload_bid` (signed)   | `SignedExecutionPayloadBid`      | builder's bid (small) — at slot start |
| `execution_payload`                | `SignedExecutionPayloadEnvelope` | the actual payload (large) — mid-slot |
| `payload_attestation_message`      | `PayloadAttestationMessage`      | PTC votes on payload timeliness       |

Timing shifts earlier (GLOAS bp): `ATTESTATION_DUE 2500` (3 s), `AGGREGATE_DUE 5000` (6 s),
`PAYLOAD_ATTESTATION_DUE 7500` (9 s). Net effect: the big payload moves off the critical
t=0 path, and there's an extra attestation-like round late in the slot.
