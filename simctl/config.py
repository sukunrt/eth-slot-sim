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
    per_item_ms: int = 0  # batched verifier per-attestation cost
    batch_window_ms: int = 50  # batched verifier window
    # Aggregate phase (the t≈8 s SignedAggregateAndProof flood on the global topic). Each
    # committee's ~target_aggregators aggregators publish one distinct aggregate each.
    target_aggregators: int = 16  # aggregators per committee (TARGET_AGGREGATORS_PER_COMMITTEE)
    aggregate_due_ms: int = 8000  # AGGREGATE_DUE (6667 bp of a 12 s slot); 0 ⇒ aggregates off


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
    on one of fs_subnets node-partitioned subnets, then fs_aggregators per subnet publish a
    population-scaled aggregate at finality_slot_aggregation_fraction% of the finality slot. Reuses
    the attestation deadline + data columns (the AC gate). See decoupled-consensus-spec.md."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    ac_vote_size: int = 512  # VRF-selected validators voting on the AC each slot (one global topic); ≤ V
    ac_slots_per_finality_slot: int = 10  # k: a finality slot spans k AC slots
    fs_subnets: int = 40  # finality subnets, node-partitioned (every node on one); ≤ N
    fs_aggregators: int = 16  # aggregators per finality subnet per finality slot (clamped to members)
    finality_slot_aggregation_fraction: int = 50  # % of the finality slot when aggregates publish
    fc_vote_offset_ms: int = 1000  # offset into the finality slot for the per-validator vote burst


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
        if self.attestation.validators < self.topology.num_nodes:
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
        if dc is None or not dc.enabled:
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
        if self.attestation.validators < n:
            raise ValueError(f"decoupled_consensus needs V ({self.attestation.validators}) >= N ({n})")
        if dc.ac_vote_size > self.attestation.validators:
            raise ValueError(f"ac_vote_size ({dc.ac_vote_size}) > V ({self.attestation.validators})")
        if dc.fs_subnets > n:
            raise ValueError(f"fs_subnets ({dc.fs_subnets}) > N ({n})")
        if not 0 < dc.finality_slot_aggregation_fraction < 100:
            raise ValueError("finality_slot_aggregation_fraction must be in (0, 100)")
        # The per-validator vote burst must precede the aggregation deadline within the finality slot.
        agg_ms = dc.finality_slot_aggregation_fraction * dc.ac_slots_per_finality_slot * \
            self.slot_duration_seconds * 1000 // 100
        if dc.fc_vote_offset_ms >= agg_ms:
            raise ValueError(
                f"fc_vote_offset_ms ({dc.fc_vote_offset_ms}) must be < the aggregation deadline ({agg_ms} ms)"
            )
        return self


def load_config(path: Path) -> SimConfig:
    """Load and validate a run config from YAML."""
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return SimConfig(**data)
