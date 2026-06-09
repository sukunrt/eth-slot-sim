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


def test_stop_time_covers_startup_slots_drain():
    # startup 120s + 10 slots * 12s + drain/margin > config's 1 min floor.
    cfg = config.SimConfig(num_slots=10, slot_duration_seconds=12,
                           startup_seconds=120, stop_time_minutes=1)
    sh = runner.generate_shadow_yaml(cfg, _toy_topology(), {})
    minutes = float(sh["general"]["stop_time"].removesuffix(" min"))
    assert minutes * 60 >= 120 + 10 * 12 + 12  # startup + run + at least one drain slot
