"""Smoke test for the topology logic copied from batched-attestation-sim."""

from pathlib import Path

from simctl import committee, topology


def _adjacency(topo: topology.Topology) -> dict[int, set[int]]:
    adj: dict[int, set[int]] = {n.num: set() for n in topo.nodes}
    for e in topo.edges:
        adj[e.source].add(e.target)
        adj[e.target].add(e.source)
    return adj


def _connected(adj: dict[int, set[int]], members) -> bool:
    """True if members form one connected piece using only edges internal to the set."""
    members = list(members)
    if len(members) < 2:
        return True
    inset = set(members)
    seen, stack = {members[0]}, [members[0]]
    while stack:
        for nb in adj[stack.pop()]:
            if nb in inset and nb not in seen:
                seen.add(nb)
                stack.append(nb)
    return seen == inset


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


def test_supernode_ids_deterministic_and_matches_topology_bandwidth():
    # Pure + deterministic: the proposer schedule and the topology bandwidth must agree
    # on exactly the same set, so this function is the single source of truth for both.
    a = topology.supernode_ids(20, 0.5, 7)
    assert a == topology.supernode_ids(20, 0.5, 7)
    assert 0 in a, "node 0 is the always-on supernode (block builder) when fraction is on"
    assert all(0 <= i < 20 for i in a)
    assert 0 < len(a) < 20

    topo = topology.generate_random_topology(
        num_nodes=20, degree=4, seed=7, super_node_fraction=0.5
    )
    bw_supers = {n.num for n in topo.nodes if n.upload_bw_mbps >= 1024}
    assert bw_supers == a, "topology's 1024-Mbps nodes must equal supernode_ids(...)"


def test_supernode_ids_empty_when_fraction_off():
    assert topology.supernode_ids(20, 0.0, 7) == set()
    topo = topology.generate_random_topology(num_nodes=20, degree=4, seed=7)
    assert all(n.upload_bw_mbps < 1024 for n in topo.nodes), "no supernodes at fraction 0"


def test_subnet_topology_connects_every_subnet_within_k():
    a = committee.generate(
        committee.Params(
            n=30, v=60, c=4, sc=4, subnets_per_node=2, subscribe_floor=10, seed=1, num_slots=1
        )
    )
    k = 12
    topo = topology.generate_subnet_topology(num_nodes=30, k=k, seed=42, assignment=a)

    assert {n.num for n in topo.nodes} == set(range(30))
    assert all(e.latency_ms > 0 for e in topo.edges), "edges need positive latency"

    adj = _adjacency(topo)
    for e in topo.edges:  # symmetric
        assert e.source in adj[e.target] and e.target in adj[e.source]
    assert _connected(adj, range(30)), "block topic would partition"
    for subnet, subs in enumerate(a.subnet_subscribers):
        assert _connected(adj, subs), f"subnet {subnet} subscribers not connected: {subs}"

    # Soft target K: fill ran (mean near K, well above the tree-only ~2), none exceeds N-1.
    degrees = [len(adj[i]) for i in range(30)]
    assert max(degrees) <= 29
    assert sum(degrees) / 30 >= k - 1, "fill did not reach K"


def test_subnet_topology_degrades_gracefully_for_small_n():
    # K far larger than N-1, plus a singleton and an empty subnet: no crash/spin, degree
    # capped at N-1, still connected.
    a = committee.Assignment(
        params=committee.Params(n=3, v=3, c=3, sc=1),
        subnet_subscribers=[[0, 1, 2], [0], []],
        slots=[],
    )
    topo = topology.generate_subnet_topology(num_nodes=3, k=10, seed=1, assignment=a)
    adj = _adjacency(topo)
    assert all(len(adj[i]) <= 2 for i in range(3)), "degree must not exceed N-1"
    assert _connected(adj, range(3))
