"""Smoke test for the topology logic copied from batched-attestation-sim."""

from pathlib import Path

from simctl import topology


def _adjacency(topo: topology.Topology) -> dict[int, set[int]]:
    adj: dict[int, set[int]] = {n.num: set() for n in topo.nodes}
    for e in topo.edges:
        adj[e.source].add(e.target)
        adj[e.target].add(e.source)
    return adj


def test_random_topology_is_connected_with_latencies():
    topo = topology.generate_random_topology(num_nodes=20, degree=6, seed=42)

    assert len(topo.nodes) == 20
    assert {n.num for n in topo.nodes} == set(range(20))
    assert topo.edges, "expected edges"
    assert all(e.latency_ms > 0 for e in topo.edges), "edges need positive latency"

    # Bidirectional adjacency.
    adj = _adjacency(topo)
    for e in topo.edges:
        assert e.source in adj[e.target] and e.target in adj[e.source]

    # Connected: BFS from node 0 reaches everyone (no isolated nodes).
    seen, stack = {0}, [0]
    while stack:
        for nb in adj[stack.pop()]:
            if nb not in seen:
                seen.add(nb)
                stack.append(nb)
    assert seen == set(range(20)), "graph must be connected"


def test_topology_round_trips_through_json(tmp_path: Path):
    topo = topology.generate_random_topology(num_nodes=8, degree=3, seed=1)
    path = tmp_path / "topology.json"
    topo.save(path)
    loaded = topology.load_topology(path)
    assert {n.num for n in loaded.nodes} == {n.num for n in topo.nodes}
    assert len(loaded.edges) >= len(topo.edges)  # load ensures bidirectionality


def test_super_node_fraction_assigns_high_bandwidth():
    topo = topology.generate_random_topology(
        num_nodes=20, degree=4, seed=7, super_node_fraction=0.5
    )
    supers = [n for n in topo.nodes if n.upload_bw_mbps >= 1024]
    assert supers, "expected some supernodes at fraction 0.5"
