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


def test_unknown_key_is_rejected(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("num_attestors: 8\n")
    with pytest.raises(Exception):
        config.load_config(p)
