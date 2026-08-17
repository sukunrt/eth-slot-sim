"""Configuration schema for slot-simulation runs.

Covers block dissemination plus the optional attestation phase (the ``attestation:``
block — committees, subnets, the batched verifier, and the aggregate flood). Partial
messages are still out of scope. ``extra="forbid"`` rejects unknown keys, so a typo or a
stale field fails loudly rather than being silently ignored.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class TopologyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    num_nodes: int = 25
    degree: int = 8  # target peers per node (K); attestation runs need it >= a node's subnet-mates
    type: Literal["random", "ring"] = "random"
    seed: int = 42
    super_node_fraction: float = 0.0
    latency_multiple: float = 1.0
    min_node_to_node_latency_ms: int = 0


class GossipsubParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    D: int = 8
    Dlow: int = 6
    Dhigh: int = 12


class PartialConfig(BaseModel):
    """Partial-message transport knobs (partial-attestation-spec.md §9), consulted only when
    ``attestation.transport`` is ``partial``. Defaults mirror the draft spec: 20 ms publish
    ticks, forward cap 2·D (0 sentinel), 10 IWANTs per missing position, and the 128 + 96 B
    split of classic's 240 B attestation."""

    model_config = ConfigDict(extra="forbid")

    publish_interval_ms: int = 20
    max_peers_per_attestation: int = 0  # 0 ⇒ 2·D
    max_iwant_per_position: int = 10
    attestation_data_size: int = 128
    signature_size: int = 96
    disable_metadata_gossip: bool = False


class AttestationConfig(BaseModel):
    """Attestation phase knobs. Present ⇒ the run adds the block→attestation response.
    V, C, s_c are independent (only C·s_c ≤ V); see committee.py."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True  # False ⇒ build the committee (proposer schedule + subnet topology) but emit no attestations — block-only on the same network
    validators: int = 64  # V, mapped over the N nodes
    committees: int = 1  # C, active attestation subnets per slot
    committee_size: int = 8  # s_c, attesters per committee per slot
    subnet_count: int = 64
    subnets_per_node: int = 2  # subnets each node subscribes (capped at C)
    subscribe_floor: int = 10  # min subscribers per active subnet
    attestation_due_ms: int = 4000  # ATTESTATION_DUE (3333 bp of a 12 s slot)
    prep_ms: int = 0  # Δ_prep before emitting on block receipt
    verify_delay_ms: int = 10  # batched verifier base delay
    per_item_ms: float = 0.01  # batched verifier per-attestation cost (0.01 ⇒ 10µs, batched BLS)
    batch_window_ms: int = 10  # batched verifier window
    batch_max_items: int = 300  # max attestations per verify batch (0 = uncapped)
    # Aggregate phase (the t≈8 s SignedAggregateAndProof flood on the global topic). Each
    # committee's ~target_aggregators aggregators publish one distinct aggregate each.
    target_aggregators: int = 16  # aggregators per committee (TARGET_AGGREGATORS_PER_COMMITTEE)
    aggregate_due_ms: int = 8000  # AGGREGATE_DUE (6667 bp of a 12 s slot); 0 ⇒ aggregates off
    # Transport for the attestation-class floods (standard attestations + finality votes when
    # decoupled): classic one-publish-per-message gossipsub, or the partial-messages extension
    # on the same schedule. A transport, not a phase — validated to need attestations or
    # decoupled on. schedule.json is unchanged, so classic-vs-partial runs are comparable.
    transport: Literal["classic", "partial"] = "classic"
    partial: PartialConfig | None = None  # knobs; consulted only when transport == "partial"


class DataColumnsConfig(BaseModel):
    """Data-columns phase knobs (gossip dissemination + custody + the attestation gate).
    Present+enabled ⇒ the proposer bursts one DataColumnSidecar per column subnet at t=0 and,
    with attestations on, a node votes block only once it has the block AND all its custody
    columns by the deadline. Uniform custody; requires super_node_fraction > 0 and V ≥ N (so
    every node validates ⇒ custody 8). See data-columns-spec.md."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    num_columns: int = 128  # all active each slot (tests: 32)
    blobs: int = 6  # B → column size B*2144+356 ≈ 12.9 KiB
    custody_floor: int = 8  # columns an ordinary node holds (uniform; skew later)
    full_custody_fraction: float = 0.5  # share of supernodes that are full-custody; F=round(frac·n_super)
    column_backbone_floor: int = 3  # min F so every subnet has a backbone core (else generation errors)
    per_subnet_floor: int = 0  # optional thin-subnet backstop (0 = off)
    verify_service_ms: int = 3  # per-column validation-as-sleep
    verify_parallelism_super: int = 16  # P for a full-custody node
    verify_parallelism_regular: int = 4  # P for everyone else


class SyncConfig(BaseModel):
    """Sync-committee phase knobs (node-based membership + per-slot aggregators). Present+enabled ⇒
    each slot a seeded subset of `size` member nodes (each on one of `subnets` subnets) emits a
    SyncCommitteeMessage voting the head at min(block_seen, attestation_due) — un-gated by data
    availability — and `target_aggregators` per subnet emit a SignedContributionAndProof on a
    global topic at aggregate_due. Reuses the attestation deadlines (so it needs the attestation
    block present, even with attestations disabled). See sync-committee-spec.md."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    size: int = 512  # member nodes (SYNC_COMMITTEE_SIZE-scale); a seeded subset of N, asserted ≤ N
    subnets: int = 4  # subcommittees / subnets (SYNC_COMMITTEE_SUBNET_COUNT); asserted ≤ size
    target_aggregators: int = 16  # per subnet → contributions (clamped to a subnet's members)


class DecoupledConsensusConfig(BaseModel):
    """Decoupled-consensus phase knobs (availability + finality chains). Present+enabled ⇒ the run
    replaces attestations and sync with: a global flood of ac_vote_size VRF-selected validator votes
    on the availability chain each slot (block→vote coupled + data-availability gated, like
    attestations); and, every ac_slots_per_finality_slot (k) AC slots, a finality vote per validator
    on ITS drawn subnet (finality_subnet_of partitions the validator set over fs_subnets; hosts fan
    out where they aren't members), then fs_aggregators validators per subnet — sampled from the
    whole set — have their host publish a population-scaled aggregate at
    finality_slot_aggregation_fraction% of the finality slot. Reuses the attestation deadline + data
    columns (the AC gate). See decoupled-consensus-spec.md."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    ac_vote_size: int = 512  # VRF-selected validators voting on the AC each slot (one global topic); ≤ V
    ac_slots_per_finality_slot: int = 10  # k: a finality slot spans k AC slots
    fs_subnets: int = 40  # finality subnets, validator-partitioned (stable node receiver core); ≤ N
    fs_aggregators: int = 16  # aggregator validators per subnet per finality slot (whole-set);
    # per subnet PER ROUND when segregated
    finality_slot_aggregation_fraction: int = 50  # % of the finality slot when aggregates publish;
    # IGNORED under validator_segregation (round_aggregation_fraction replaces it)
    fc_vote_offset_ms: int = 1000  # offset into the finality slot for the per-validator vote burst;
    # reinterpreted as an offset into the AC slot when segregated
    # Validator segregation (validator-segregation-spec.md): k round groups, one per AC slot —
    # round s % k's validators vote in slot s, and per-slot aggregators publish cell-scaled
    # aggregates at round_aggregation_fraction% of the AC slot (the last 100−f% is the
    # aggregate-dissemination window).
    validator_segregation: bool = False
    round_aggregation_fraction: int = 67  # % of the AC slot when round aggregates publish


class EPBSConfig(BaseModel):
    """ePBS two-phase block send (Gloas/EIP-7732), ON by default. The proposer plays the
    builder: it publishes a small consensus block at the block instant and the execution
    payload — sized by the top-level ``block_size`` — plus the column burst 0.5-1 s later
    (``payload_offset_ms`` + U(0, ``payload_jitter_ms``)). Votes (attestation / AC) gate on
    the consensus block alone; the DA check moves to the Payload Timeliness Committee:
    ``ptc_size`` validators per slot each vote payload_present (payload + custody columns
    seen) at ``ptc_due_ms``. PTC needs a schedule (an attestation config); ``ptc_size: 0``
    turns it off. Composes with every phase. Disable with ``epbs: {enabled: false}`` to get
    the legacy single Block."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    consensus_block_size: int = 2048  # bytes: the bid-only Gloas beacon block
    payload_offset_ms: int = 500  # builder reveal delay after the block instant
    payload_jitter_ms: int = 500  # reveal lands in [offset, offset+jitter)
    ptc_size: int = 512  # PTC_SIZE = 2**9; clamped to V by the schedule draw; 0 ⇒ off
    ptc_due_ms: int = 9000  # PAYLOAD_ATTESTATION_DUE_BPS = 7500 (75% of a 12 s slot)


class RegularTierConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    weights: list[float] = [0.65, 0.25, 0.10]  # P(count = index+1): most solo nodes run 1 key


class SuperTierConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: int = 1
    max: int = 1000
    mean: float = 200.0  # truncated log-normal mean (μ solved; σ is an implementation default)


class ValidatorDistributionConfig(BaseModel):
    """Validator→node distribution (the Dist seam; see skewed-validators-spec.md). uniform keeps
    v % N with V = attestation.validators (status quo, no schedule.json change). tiered keys off
    the supernode set: regular nodes draw 1..len(weights) keys (weighted), supernodes draw a
    truncated log-normal on [min, max] with the given mean — and V becomes EMERGENT
    (Σ counts; attestation.validators is ignored). explicit takes per-node counts verbatim."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["uniform", "tiered", "explicit"] = "uniform"
    regular: RegularTierConfig = Field(default_factory=RegularTierConfig)
    super: SuperTierConfig = Field(default_factory=SuperTierConfig)
    counts: list[int] | None = None  # explicit mode only; len == num_nodes
    seed: int = 7  # independent of the schedule draw seed


class SimConfig(BaseModel):
    """Root configuration for a single block-dissemination run."""

    model_config = ConfigDict(extra="forbid")

    topology: TopologyConfig = Field(default_factory=TopologyConfig)
    gossipsub: GossipsubParams = Field(default_factory=GossipsubParams)
    attestation: AttestationConfig | None = None  # present ⇒ run the attestation phase
    data_columns: DataColumnsConfig | None = None  # present+enabled ⇒ run the data-columns phase
    sync: SyncConfig | None = None  # present+enabled ⇒ run the sync-committee phase
    # present+enabled ⇒ run decoupled consensus (replaces attestations + sync)
    decoupled_consensus: DecoupledConsensusConfig | None = None
    # ePBS two-phase block send, ON by default (see EPBSConfig)
    epbs: EPBSConfig = Field(default_factory=EPBSConfig)
    # absent or type=uniform ⇒ validator v on node v % N (the status quo)
    validator_distribution: ValidatorDistributionConfig | None = None
    num_slots: int = 5
    slot_duration_seconds: int = 12
    block_size: int = 128 * 1024
    verify_delay_ms: int = 10
    offset_ms: int = 0
    jitter_ms: int = 2000
    startup_seconds: int = 60
    stop_time_minutes: float = 5.0
    # Shadow's own log level (shadow accepts error|warning|info|debug|trace).
    log_level: Literal["error", "warning", "info", "debug", "trace"] = "info"
    seed: int = 1  # validator rng seed (combined per-node with node number)
    rpc_log_node: int = -1  # node-num to enable gossipsub debug RPC logging on (-1 = off)

    @model_validator(mode="after")
    def _check_data_columns(self) -> "SimConfig":
        dc = self.data_columns
        if dc is None or not dc.enabled:
            return self
        # Custody needs the committee (proposer schedule + column membership), which is built
        # from the attestation block; columns-only runs use attestation.enabled=False.
        if self.attestation is None:
            raise ValueError("data_columns need an attestation block (V and the committee)")
        if self.topology.super_node_fraction <= 0:
            raise ValueError("data_columns need topology.super_node_fraction > 0 (the full-custody backbone)")
        # Under a non-uniform distribution V is emergent (Σ counts ≥ N because every count ≥ 1),
        # so the V ≥ N check only applies to the uniform mapping.
        if self._dist_is_uniform() and self.attestation.validators < self.topology.num_nodes:
            raise ValueError(
                f"data_columns need V ({self.attestation.validators}) >= N "
                f"({self.topology.num_nodes}): every node must validate (uniform custody)"
            )
        return self

    @model_validator(mode="after")
    def _check_sync(self) -> "SimConfig":
        sc = self.sync
        if sc is None or not sc.enabled:
            return self
        # Sync reuses attestation.attestation_due_ms / aggregate_due_ms as its deadlines, so the
        # attestation block must be present (attestations themselves may be disabled).
        if self.attestation is None:
            raise ValueError("sync needs an attestation block (it reuses the attestation deadlines)")
        n = self.topology.num_nodes
        if sc.size > n:
            raise ValueError(f"sync.size ({sc.size}) > N ({n}): members are a subset of the nodes")
        if sc.subnets > sc.size:
            raise ValueError(f"sync.subnets ({sc.subnets}) > sync.size ({sc.size})")
        return self

    @model_validator(mode="after")
    def _check_decoupled_consensus(self) -> "SimConfig":
        dc = self.decoupled_consensus
        if dc is None:
            return self
        if dc.validator_segregation and not dc.enabled:
            raise ValueError("validator_segregation requires decoupled_consensus.enabled")
        if not dc.enabled:
            return self
        # Decoupled reuses the attestation block for V + attestation_due_ms (the AC-vote deadline),
        # so it must be present — but the OLD attestation emit is off (the AC vote replaces it).
        if self.attestation is None:
            raise ValueError("decoupled_consensus needs an attestation block (it reuses V + the deadline)")
        if self.attestation.enabled:
            raise ValueError("decoupled_consensus replaces attestations: set attestation.enabled=false")
        if self.sync is not None and self.sync.enabled:
            raise ValueError("decoupled_consensus and sync are mutually exclusive")
        # Data columns gate the AC vote (the 'availability' chain), so they must be on; they in turn
        # require supernodes (the full-custody backbone) and V ≥ N (uniform custody).
        if self.data_columns is None or not self.data_columns.enabled:
            raise ValueError("decoupled_consensus needs data_columns enabled (they gate the AC vote)")
        n = self.topology.num_nodes
        if self.topology.super_node_fraction <= 0:
            raise ValueError("decoupled_consensus needs topology.super_node_fraction > 0 (column backbone)")
        # V-dependent checks only bind under the uniform mapping; with tiered/explicit, V is
        # emergent (Σ counts) and ac_vote_size ≤ V is enforced at generation time.
        if self._dist_is_uniform():
            if self.attestation.validators < n:
                raise ValueError(
                    f"decoupled_consensus needs V ({self.attestation.validators}) >= N ({n})"
                )
            if dc.ac_vote_size > self.attestation.validators:
                raise ValueError(
                    f"ac_vote_size ({dc.ac_vote_size}) > V ({self.attestation.validators})"
                )
        if dc.fs_subnets > n:
            raise ValueError(f"fs_subnets ({dc.fs_subnets}) > N ({n})")
        # The per-validator vote burst must precede the aggregation deadline. Segregated: a
        # per-AC-slot bound (round_aggregation_fraction% of one slot — much tighter than base's
        # per-fslot one; the base fraction knob is ignored). Base: within the finality slot.
        if dc.validator_segregation:
            if not 0 < dc.round_aggregation_fraction < 100:
                raise ValueError("round_aggregation_fraction must be in (0, 100)")
            agg_ms = dc.round_aggregation_fraction * self.slot_duration_seconds * 1000 // 100
        else:
            if not 0 < dc.finality_slot_aggregation_fraction < 100:
                raise ValueError("finality_slot_aggregation_fraction must be in (0, 100)")
            agg_ms = dc.finality_slot_aggregation_fraction * dc.ac_slots_per_finality_slot * \
                self.slot_duration_seconds * 1000 // 100
        if dc.fc_vote_offset_ms >= agg_ms:
            raise ValueError(
                f"fc_vote_offset_ms ({dc.fc_vote_offset_ms}) must be < the aggregation deadline ({agg_ms} ms)"
            )
        return self

    def _dist_is_uniform(self) -> bool:
        return self.validator_distribution is None or self.validator_distribution.type == "uniform"

    @model_validator(mode="after")
    def _check_partial_transport(self) -> "SimConfig":
        a = self.attestation
        if a is None or a.transport != "partial":
            return self
        # The transport carries the attestation-class floods, so one of them must exist:
        # attestations on, or decoupled on (it governs the finality votes there).
        decoupled_on = self.decoupled_consensus is not None and self.decoupled_consensus.enabled
        if not a.enabled and not decoupled_on:
            raise ValueError(
                "transport: partial is a transport, not a phase — it needs "
                "attestation.enabled or decoupled_consensus.enabled"
            )
        return self

    @model_validator(mode="after")
    def _check_validator_distribution(self) -> "SimConfig":
        vd = self.validator_distribution
        if vd is None or vd.type == "uniform":
            return self
        # Non-uniform distributions still need the attestation block (deadlines + the schedule
        # gate in the runner); V itself is emergent.
        if self.attestation is None:
            raise ValueError("validator_distribution needs an attestation block (the schedule gate)")
        if vd.type == "tiered":
            if self.topology.super_node_fraction <= 0:
                raise ValueError("tiered validator_distribution needs super_node_fraction > 0")
            if not vd.regular.weights or any(w < 0 for w in vd.regular.weights) \
                    or sum(vd.regular.weights) <= 0:
                raise ValueError("regular.weights must be non-empty and non-negative")
            if not 1 <= vd.super.min <= vd.super.mean <= vd.super.max:
                raise ValueError("super tier needs 1 <= min <= mean <= max")
        if vd.type == "explicit":
            if vd.counts is None or len(vd.counts) != self.topology.num_nodes:
                raise ValueError("explicit validator_distribution needs counts of length num_nodes")
            floor = 1 if self.data_columns is not None and self.data_columns.enabled else 0
            if any(c < floor for c in vd.counts):
                raise ValueError(f"explicit counts must all be >= {floor} (data-columns custody)")
        return self


def load_config(path: Path) -> SimConfig:
    """Load and validate a run config from YAML."""
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return SimConfig(**data)
