"""TDD for the slimmed block-dissemination config."""

import pytest

from simctl import config


def test_load_minimal(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "topology:\n"
        "  num_nodes: 12\n"
        "  degree: 4\n"
        "num_slots: 3\n"
        "block_size: 65536\n"
    )
    cfg = config.load_config(p)
    assert cfg.topology.num_nodes == 12
    assert cfg.topology.degree == 4
    assert cfg.topology.type == "random"  # default
    assert cfg.num_slots == 3
    assert cfg.block_size == 65536
    assert cfg.slot_duration_seconds == 12  # default
    assert cfg.gossipsub.D == 8  # default
    assert cfg.verify_delay_ms == 10  # default
    assert cfg.startup_seconds == 60  # default


def test_defaults_have_no_attestation_fields():
    cfg = config.SimConfig()
    for absent in ("num_attestors", "num_topics", "use_partial_messages",
                   "validation_batch_window_ms", "fanout_nodes_per_topic"):
        assert not hasattr(cfg, absent), f"slim config should not have {absent}"


def test_attestation_enabled_defaults_true_and_toggles():
    assert config.AttestationConfig().enabled is True
    assert config.AttestationConfig(enabled=False).enabled is False


def test_unknown_key_is_rejected(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("num_attestors: 8\n")
    with pytest.raises(Exception):
        config.load_config(p)


def test_attestation_aggregate_defaults():
    a = config.AttestationConfig()
    assert a.target_aggregators == 16  # TARGET_AGGREGATORS_PER_COMMITTEE
    assert a.aggregate_due_ms == 8000  # AGGREGATE_DUE (6667 bp of a 12 s slot)


def test_attestation_loads_aggregate_overrides(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "attestation:\n"
        "  target_aggregators: 4\n"
        "  aggregate_due_ms: 9000\n"
    )
    cfg = config.load_config(p)
    assert cfg.attestation.target_aggregators == 4
    assert cfg.attestation.aggregate_due_ms == 9000


# --- data columns -------------------------------------------------------------


def test_data_columns_defaults():
    dc = config.DataColumnsConfig()
    assert dc.enabled is True
    assert dc.num_columns == 128 and dc.blobs == 6 and dc.custody_floor == 8
    assert dc.full_custody_fraction == 0.5 and dc.column_backbone_floor == 3
    assert dc.per_subnet_floor == 0 and dc.verify_service_ms == 3
    assert dc.verify_parallelism_super == 16 and dc.verify_parallelism_regular == 4


def _columns_yaml(num_nodes, validators, super_frac, body="  num_columns: 32\n"):
    return (
        "topology:\n"
        f"  num_nodes: {num_nodes}\n"
        f"  super_node_fraction: {super_frac}\n"
        "attestation:\n"
        f"  validators: {validators}\n"
        "data_columns:\n" + body
    )


def test_data_columns_loads(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_columns_yaml(20, 40, 0.25))
    cfg = config.load_config(p)
    assert cfg.data_columns.num_columns == 32 and cfg.data_columns.enabled is True


def test_data_columns_require_supernodes(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_columns_yaml(20, 40, 0.0))  # no supernodes ⇒ no backbone
    with pytest.raises(Exception):
        config.load_config(p)


def test_data_columns_require_v_at_least_n(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_columns_yaml(50, 40, 0.25))  # V=40 < N=50
    with pytest.raises(Exception):
        config.load_config(p)


def test_data_columns_require_attestation(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "topology:\n  num_nodes: 20\n  super_node_fraction: 0.25\n"
        "data_columns:\n  num_columns: 32\n"
    )
    with pytest.raises(Exception):
        config.load_config(p)


def test_data_columns_disabled_skips_validation(tmp_path):
    # enabled=False ⇒ the column requirements (supernodes, V≥N, attestation) don't apply.
    p = tmp_path / "c.yaml"
    p.write_text("topology:\n  num_nodes: 20\ndata_columns:\n  enabled: false\n")
    cfg = config.load_config(p)
    assert cfg.data_columns.enabled is False


# --- sync committee -----------------------------------------------------------


def test_sync_defaults():
    sc = config.SyncConfig()
    assert sc.enabled is True
    assert sc.size == 512 and sc.subnets == 4 and sc.target_aggregators == 16


def _sync_yaml(num_nodes, validators, body="  size: 8\n  subnets: 2\n", attest="  enabled: true\n"):
    return (
        "topology:\n"
        f"  num_nodes: {num_nodes}\n"
        "attestation:\n"
        f"  validators: {validators}\n" + attest
        + "sync:\n" + body
    )


def test_sync_loads(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_sync_yaml(16, 16))
    cfg = config.load_config(p)
    assert cfg.sync.size == 8 and cfg.sync.subnets == 2 and cfg.sync.enabled is True


def test_sync_requires_attestation(tmp_path):
    # sync reuses the attestation deadlines, so the attestation block must be present.
    p = tmp_path / "c.yaml"
    p.write_text("topology:\n  num_nodes: 16\nsync:\n  size: 8\n  subnets: 2\n")
    with pytest.raises(Exception):
        config.load_config(p)


def test_sync_allows_attestation_disabled(tmp_path):
    # A sync run with attestations off is allowed (disseminate+measure sync, no attestations).
    p = tmp_path / "c.yaml"
    p.write_text(_sync_yaml(16, 16, attest="  enabled: false\n"))
    cfg = config.load_config(p)
    assert cfg.sync.enabled is True and cfg.attestation.enabled is False


def test_sync_size_exceeds_n_raises(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_sync_yaml(16, 16, "  size: 20\n  subnets: 2\n"))
    with pytest.raises(Exception):
        config.load_config(p)


def test_sync_subnets_exceed_size_raises(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_sync_yaml(16, 16, "  size: 4\n  subnets: 8\n"))
    with pytest.raises(Exception):
        config.load_config(p)


def test_sync_disabled_skips_validation(tmp_path):
    # enabled=False ⇒ sync requirements (attestation present, size≤N) don't apply.
    p = tmp_path / "c.yaml"
    p.write_text("topology:\n  num_nodes: 16\nsync:\n  enabled: false\n  size: 99\n  subnets: 2\n")
    cfg = config.load_config(p)
    assert cfg.sync.enabled is False


# --- decoupled consensus ------------------------------------------------------


def test_decoupled_defaults():
    dc = config.DecoupledConsensusConfig()
    assert dc.enabled is True
    assert dc.ac_vote_size == 512 and dc.ac_slots_per_finality_slot == 10
    assert dc.fs_subnets == 40 and dc.fs_aggregators == 16
    assert dc.finality_slot_aggregation_fraction == 50 and dc.fc_vote_offset_ms == 1000


def _decoupled_yaml(num_nodes=16, validators=32, super_frac=0.5, dc_body=None,
                    attest_enabled=False, columns="  num_columns: 8\n", extra=""):
    dc = dc_body if dc_body is not None else (
        "  ac_vote_size: 8\n  ac_slots_per_finality_slot: 2\n  fs_subnets: 2\n  fs_aggregators: 2\n"
    )
    return (
        "topology:\n"
        f"  num_nodes: {num_nodes}\n"
        f"  super_node_fraction: {super_frac}\n"
        "attestation:\n"
        f"  enabled: {'true' if attest_enabled else 'false'}\n"
        f"  validators: {validators}\n"
        + ("data_columns:\n" + columns if columns else "")
        + "decoupled_consensus:\n" + dc
        + extra
    )


def test_decoupled_loads(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml())
    cfg = config.load_config(p)
    assert cfg.decoupled_consensus.enabled is True
    assert cfg.decoupled_consensus.ac_vote_size == 8 and cfg.decoupled_consensus.fs_subnets == 2


def test_decoupled_requires_attestation_block(tmp_path):
    # decoupled reuses attestation V + attestation_due_ms, so the block must be present.
    p = tmp_path / "c.yaml"
    p.write_text(
        "topology:\n  num_nodes: 16\n  super_node_fraction: 0.5\n"
        "data_columns:\n  num_columns: 8\n"
        "decoupled_consensus:\n  ac_vote_size: 8\n  fs_subnets: 2\n"
    )
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_requires_columns_enabled(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml(columns=""))  # no data_columns block
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_requires_supernodes(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml(super_frac=0.0))
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_requires_v_at_least_n(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml(num_nodes=50, validators=40))  # V < N
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_ac_vote_size_exceeds_v_raises(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml(dc_body="  ac_vote_size: 99\n  fs_subnets: 2\n"))
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_fs_subnets_exceeds_n_raises(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml(dc_body="  ac_vote_size: 8\n  fs_subnets: 99\n"))
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_rejects_attestation_emit(tmp_path):
    # decoupled replaces attestations; the old attestation emit must be off.
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml(attest_enabled=True))
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_rejects_sync(tmp_path):
    # decoupled and sync are mutually exclusive (both replace the finality/attestation phases).
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml(extra="sync:\n  size: 8\n  subnets: 2\n"))
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_vote_offset_must_precede_aggregation(tmp_path):
    # fc_vote_offset_ms must be < finality_slot_aggregation_fraction% · k · slot (votes precede agg).
    body = (
        "  ac_vote_size: 8\n  ac_slots_per_finality_slot: 2\n  fs_subnets: 2\n  fs_aggregators: 2\n"
        "  finality_slot_aggregation_fraction: 50\n  fc_vote_offset_ms: 20000\n"  # 20s > 50%·2·12s=12s
    )
    p = tmp_path / "c.yaml"
    p.write_text(_decoupled_yaml(dc_body=body))
    with pytest.raises(Exception):
        config.load_config(p)


def test_decoupled_disabled_skips_validation(tmp_path):
    # enabled=False ⇒ decoupled requirements don't apply.
    p = tmp_path / "c.yaml"
    p.write_text("topology:\n  num_nodes: 16\ndecoupled_consensus:\n  enabled: false\n  fs_subnets: 99\n")
    cfg = config.load_config(p)
    assert cfg.decoupled_consensus.enabled is False


def test_validator_distribution_tiered_needs_supers(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "topology:\n"
        "  num_nodes: 12\n"
        "attestation:\n"
        "  enabled: false\n"
        "validator_distribution:\n"
        "  type: tiered\n"
    )
    with pytest.raises(ValueError, match="super_node_fraction"):
        config.load_config(p)


def test_validator_distribution_tiered_loads_with_defaults(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "topology:\n"
        "  num_nodes: 12\n"
        "  super_node_fraction: 0.5\n"
        "attestation:\n"
        "  enabled: false\n"
        "validator_distribution:\n"
        "  type: tiered\n"
    )
    cfg = config.load_config(p)
    vd = cfg.validator_distribution
    assert vd.regular.weights == [0.65, 0.25, 0.10]
    assert (vd.super.min, vd.super.max, vd.super.mean) == (1, 1000, 200.0)


def test_validator_distribution_explicit_checks_length(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "topology:\n"
        "  num_nodes: 12\n"
        "attestation:\n"
        "  enabled: false\n"
        "validator_distribution:\n"
        "  type: explicit\n"
        "  counts: [1, 2, 3]\n"
    )
    with pytest.raises(ValueError, match="length num_nodes"):
        config.load_config(p)
