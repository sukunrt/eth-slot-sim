"""TDD for the pure runner helpers (GML + shadow.yaml generation)."""

from simctl import config, runner, topology


def _toy_topology() -> topology.Topology:
    nodes = [
        topology.NodeSpec(num=i, upload_bw_mbps=25, download_bw_mbps=50, country="US")
        for i in range(3)
    ]
    # triangle ring 0-1-2-0, both directions
    edges = [
        topology.Edge(0, 1, 10), topology.Edge(1, 0, 10),
        topology.Edge(1, 2, 20), topology.Edge(2, 1, 20),
        topology.Edge(2, 0, 30), topology.Edge(0, 2, 30),
    ]
    return topology.Topology(nodes=nodes, edges=edges, fanout_nodes=set())


def test_compute_peer_lists():
    peers = runner.compute_peer_lists(_toy_topology())
    assert peers[0] == [1, 2]
    assert peers[1] == [0, 2]
    assert peers[2] == [0, 1]


def test_generate_gml_has_bandwidth_and_edges():
    topo = _toy_topology()
    gml = runner.generate_gml(topo)
    assert "graph [" in gml
    assert 'host_bandwidth_up "25 Mbit"' in gml
    assert 'host_bandwidth_down "50 Mbit"' in gml
    # one self-loop per node + one edge per topology edge
    assert gml.count("edge [") == len(topo.nodes) + len(topo.edges)
    assert 'latency "10 ms"' in gml


def test_generate_shadow_yaml_structure():
    topo = _toy_topology()
    cfg = config.SimConfig(topology=config.TopologyConfig(num_nodes=3, degree=2))
    sh = runner.generate_shadow_yaml(cfg, topo, runner.compute_peer_lists(topo))

    assert set(sh["hosts"]) == {"node0", "node1", "node2"}
    h0 = sh["hosts"]["node0"]
    assert h0["network_node_id"] == 0
    proc = h0["processes"][0]
    assert proc["path"] == "./slot-sim-node"
    assert proc["start_time"] == "0 sec"

    args = proc["args"]
    for token in (
        "-node-num=0", "-num-nodes=3", "-num-slots=5", "-slot=12s",
        "-block-size=131072", "-verify-delay=10ms", "-offset=0ms",
        "-jitter=2000ms", "-D=8", "-Dlo=6", "-Dhi=12", "-startup=60s",
        "-peer-nums=1,2",
    ):
        assert token in args, f"missing {token} in {args!r}"

    assert sh["network"]["use_shortest_path"] is True
    assert sh["network"]["graph"]["type"] == "gml"
    assert sh["general"]["stop_time"].endswith("min")


def test_host_args_attestations_toggle():
    # With attestations off, the host still gets -schedule (for the proposer schedule) but
    # -attestations=false so the Go binary runs block-only on the same network.
    cfg_off = config.SimConfig(
        attestation=config.AttestationConfig(enabled=False),
    )
    args_off = runner._host_args(cfg_off, 0, 3, [1, 2], "/run/schedule.json")
    assert "-schedule=/run/schedule.json" in args_off
    assert "-attestations=false" in args_off

    cfg_on = config.SimConfig(attestation=config.AttestationConfig())
    args_on = runner._host_args(cfg_on, 0, 3, [1, 2], "/run/schedule.json")
    assert "-attestations=true" in args_on


def test_simnet_params_carry_attest_flag():
    # The simnet backend learns whether to attest from the same enabled flag.
    cfg = config.SimConfig(attestation=config.AttestationConfig(enabled=False))
    assert runner._simnet_params(cfg)["attest"] is False
    cfg_on = config.SimConfig(attestation=config.AttestationConfig())
    assert runner._simnet_params(cfg_on)["attest"] is True


def test_simnet_params_match_scenario():
    # The simnet backend (a synctest go-test harness) takes the same scenario knobs
    # as a Shadow host; topology and csv paths are added per-run by run_simnet.
    assert runner._simnet_params(config.SimConfig()) == {
        "latency_multiple": 1.0,
        "num_slots": 5,
        "slot_seconds": 12,
        "block_size": 131072,
        "verify_ms": 10,
        "offset_ms": 0,
        "jitter_ms": 2000,
        "d": 8, "dlo": 6, "dhi": 12,
        "seed": 1,
    }


def _attest_config(**att_over) -> config.SimConfig:
    return config.SimConfig(
        topology=config.TopologyConfig(num_nodes=3, degree=2),
        attestation=config.AttestationConfig(**att_over),
    )


def test_schedule_assignment_carries_aggregator_knobs():
    cfg = _attest_config(validators=24, committees=1, committee_size=2,
                         subscribe_floor=2, target_aggregators=4)
    a = runner._schedule_assignment(cfg)
    assert a.params.target_aggregators == 4
    assert len(a.slots[0].aggregators) == cfg.attestation.committees  # one set per committee


def test_attestation_run_args_include_aggregate_due():
    cfg = _attest_config(aggregate_due_ms=8000)
    topo = _toy_topology()
    sh = runner.generate_shadow_yaml(cfg, topo, runner.compute_peer_lists(topo), "schedule.json")
    args = sh["hosts"]["node0"]["processes"][0]["args"]
    assert "-att-due=4000ms" in args
    assert "-agg-due=8000ms" in args


def test_simnet_params_include_aggregate_due():
    p = runner._simnet_params(_attest_config(aggregate_due_ms=8000))
    assert p["att_due_ms"] == 4000
    assert p["agg_due_ms"] == 8000


def test_host_args_include_sync_flag():
    on = config.SimConfig(
        attestation=config.AttestationConfig(), sync=config.SyncConfig(size=8, subnets=2)
    )
    assert "-sync=true" in runner._host_args(on, 0, 8, [1], "/s.json")
    off = config.SimConfig(
        attestation=config.AttestationConfig(), sync=config.SyncConfig(enabled=False)
    )
    assert "-sync=false" in runner._host_args(off, 0, 8, [1], "/s.json")
    # No sync block ⇒ no -sync flag (the Go binary defaults it off).
    none = config.SimConfig(attestation=config.AttestationConfig())
    assert "-sync" not in runner._host_args(none, 0, 8, [1], "/s.json")


def test_simnet_params_carry_sync_flag():
    on = config.SimConfig(
        attestation=config.AttestationConfig(), sync=config.SyncConfig(size=8, subnets=2)
    )
    assert runner._simnet_params(on)["sync"] is True
    # Off or absent ⇒ no sync key (the simnet backend defaults Sync false).
    off = config.SimConfig(
        attestation=config.AttestationConfig(), sync=config.SyncConfig(enabled=False)
    )
    assert "sync" not in runner._simnet_params(off)


def test_schedule_assignment_carries_sync_knobs():
    cfg = config.SimConfig(
        topology=config.TopologyConfig(num_nodes=16, degree=4),
        attestation=config.AttestationConfig(
            validators=16, committees=1, committee_size=2, subscribe_floor=2
        ),
        sync=config.SyncConfig(size=8, subnets=2, target_aggregators=2),
    )
    a = runner._schedule_assignment(cfg)
    assert a.sync_subscribers is not None and len(a.sync_subscribers) == 2
    assert sum(len(s) for s in a.sync_subscribers) == 8  # size member nodes
    assert a.slots[0].sync_aggregators is not None and len(a.slots[0].sync_aggregators) == 2


def _decoupled_config(**overrides):
    # A valid decoupled config: attestation off (supplies V + the AC deadline), data columns on
    # (the AC gate), supernodes for the backbone, V >= N.
    base = dict(
        topology=config.TopologyConfig(num_nodes=16, degree=6, super_node_fraction=0.5),
        attestation=config.AttestationConfig(enabled=False, validators=32),
        data_columns=config.DataColumnsConfig(num_columns=8),
        decoupled_consensus=config.DecoupledConsensusConfig(
            ac_vote_size=8, ac_slots_per_finality_slot=2, fs_subnets=2, fs_aggregators=2
        ),
    )
    base.update(overrides)
    return config.SimConfig(**base)


def test_host_args_include_decoupled_flags():
    on = _decoupled_config()
    args = runner._host_args(on, 0, 16, [1], "/s.json")
    assert "-decoupled=true" in args and "-k=2" in args
    assert "-fc-vote-offset=1000ms" in args and "-fc-agg-fraction=50" in args
    assert "-att-due=4000ms" in args  # the AC-vote deadline is reused, not a new flag
    # No decoupled block ⇒ no -decoupled flag (the Go binary defaults it off).
    none = config.SimConfig(attestation=config.AttestationConfig())
    assert "-decoupled" not in runner._host_args(none, 0, 8, [1], "/s.json")


def test_simnet_params_carry_decoupled_flags():
    p = runner._simnet_params(_decoupled_config())
    assert p["decoupled"] is True and p["k"] == 2
    assert p["fc_vote_offset_ms"] == 1000 and p["fc_agg_fraction"] == 50
    # Absent ⇒ no decoupled key (the simnet backend defaults it off).
    assert "decoupled" not in runner._simnet_params(config.SimConfig())


def test_schedule_assignment_carries_decoupled_knobs():
    a = runner._schedule_assignment(_decoupled_config())
    # Finality subnets partition all N; validators_per_subnet sums to V; no attestation committees.
    assert a.finality_subscribers is not None and len(a.finality_subscribers) == 2
    flat = sorted(node for subs in a.finality_subscribers for node in subs)
    assert flat == list(range(16))
    assert a.validators_per_subnet is not None and sum(a.validators_per_subnet) == 32
    assert a.slots[0].ac_voters is not None and len(a.slots[0].ac_voters) == 8
    assert a.slots[0].committees == []  # decoupled replaces committees
    assert a.slots[0].finality_aggregators is not None  # slot 0 is a finality boundary
    assert a.slots[1].finality_aggregators is None  # a non-boundary slot carries none


def _segregated_config():
    return _decoupled_config(
        decoupled_consensus=config.DecoupledConsensusConfig(
            ac_vote_size=8, ac_slots_per_finality_slot=2, fs_subnets=2, fs_aggregators=2,
            validator_segregation=True,
        ),
    )


def test_host_args_include_segregation_flags():
    args = runner._host_args(_segregated_config(), 0, 16, [1], "/s.json")
    assert "-fc-segregated=true" in args and "-fc-round-agg-fraction=67" in args
    # Plain decoupled ⇒ no segregation flags (the Go binary defaults them off).
    assert "-fc-segregated" not in runner._host_args(_decoupled_config(), 0, 16, [1], "/s.json")


def test_simnet_params_carry_segregation_keys():
    p = runner._simnet_params(_segregated_config())
    assert p["fc_segregated"] is True and p["fc_round_agg_fraction"] == 67
    assert "fc_segregated" not in runner._simnet_params(_decoupled_config())


def test_schedule_assignment_carries_segregation():
    a = runner._schedule_assignment(_segregated_config())
    assert a.finality_round_of is not None and len(a.finality_round_of) == 32
    assert a.validators_per_round_subnet is not None  # k×fs cell counts
    assert a.slots[1].finality_aggregators is not None  # every slot is a round


def test_stop_time_covers_startup_slots_drain():
    # startup 120s + 10 slots * 12s + drain/margin > config's 1 min floor.
    cfg = config.SimConfig(num_slots=10, slot_duration_seconds=12,
                           startup_seconds=120, stop_time_minutes=1)
    sh = runner.generate_shadow_yaml(cfg, _toy_topology(), {})
    minutes = float(sh["general"]["stop_time"].removesuffix(" min"))
    assert minutes * 60 >= 120 + 10 * 12 + 12  # startup + run + at least one drain slot


def test_host_args_partial_flags():
    cfg = config.SimConfig(
        topology=config.TopologyConfig(num_nodes=3),
        attestation=config.AttestationConfig(
            transport="partial", partial=config.PartialConfig(publish_interval_ms=50)),
    )
    args = runner._host_args(cfg, 0, 3, [1], "/run/schedule.json")
    for token in (
        "-transport=partial",
        "-partial-publish-interval=50ms",
        "-partial-max-peers-per-attestation=0",
        "-partial-max-iwant-per-position=10",
        "-partial-attestation-data-size=128",
        "-partial-signature-size=96",
        "-partial-disable-metadata-gossip=false",
    ):
        assert token in args, f"missing {token} in {args!r}"
    # Classic (the default): no transport flags at all — the binary defaults to classic.
    classic = runner._host_args(
        config.SimConfig(topology=config.TopologyConfig(num_nodes=3),
                         attestation=config.AttestationConfig()),
        0, 3, [1], "/run/schedule.json")
    assert "-transport" not in classic


def test_simnet_params_carry_partial_knobs():
    cfg = config.SimConfig(
        topology=config.TopologyConfig(num_nodes=3),
        attestation=config.AttestationConfig(
            transport="partial", partial=config.PartialConfig(max_iwant_per_position=5)),
    )
    p = runner._simnet_params(cfg)
    assert p["transport"] == "partial"
    assert p["partial_publish_interval_ms"] == 20
    assert p["partial_max_peers_per_attestation"] == 0
    assert p["partial_max_iwant_per_position"] == 5
    assert p["partial_attestation_data_size"] == 128
    assert p["partial_signature_size"] == 96
    assert p["partial_disable_metadata_gossip"] is False
    # Classic: no partial keys.
    classic = runner._simnet_params(
        config.SimConfig(topology=config.TopologyConfig(num_nodes=3),
                         attestation=config.AttestationConfig()))
    assert "transport" not in classic
