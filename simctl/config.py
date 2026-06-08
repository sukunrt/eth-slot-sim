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


class SimConfig(BaseModel):
    """Root configuration for a single block-dissemination run."""

    model_config = ConfigDict(extra="forbid")

    topology: TopologyConfig = Field(default_factory=TopologyConfig)
    gossipsub: GossipsubParams = Field(default_factory=GossipsubParams)
    attestation: AttestationConfig | None = None  # present ⇒ run the attestation phase
    data_columns: DataColumnsConfig | None = None  # present+enabled ⇒ run the data-columns phase
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


def load_config(path: Path) -> SimConfig:
    """Load and validate a run config from YAML."""
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return SimConfig(**data)
