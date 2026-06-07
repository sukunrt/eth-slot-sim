"""Configuration schema for block-dissemination Shadow runs.

Slimmed from batched-attestation-sim: only the knobs a single global block topic
needs. All attestation concerns (committees, fanout, partial messages, multiple
topics, validation batching) are intentionally absent; unknown keys are rejected
so a stale attestation config fails loudly rather than being silently ignored.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field


class TopologyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    num_nodes: int = 25
    degree: int = 8
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


class SimConfig(BaseModel):
    """Root configuration for a single block-dissemination run."""

    model_config = ConfigDict(extra="forbid")

    topology: TopologyConfig = Field(default_factory=TopologyConfig)
    gossipsub: GossipsubParams = Field(default_factory=GossipsubParams)
    attestation: AttestationConfig | None = None  # present ⇒ run the attestation phase
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


def load_config(path: Path) -> SimConfig:
    """Load and validate a run config from YAML."""
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return SimConfig(**data)
