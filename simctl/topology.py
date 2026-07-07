"""Topology generation with latency computation."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path


@dataclass
class NodeSpec:
    num: int
    upload_bw_mbps: int
    download_bw_mbps: int
    country: str


@dataclass
class Edge:
    source: int
    target: int
    latency_ms: int


@dataclass
class Topology:
    nodes: list[NodeSpec]
    edges: list[Edge]
    fanout_nodes: set[int]

    def to_dict(self) -> dict:
        return {
            "nodes": [
                {
                    "num": n.num,
                    "upload_bw_mbps": n.upload_bw_mbps,
                    "download_bw_mbps": n.download_bw_mbps,
                    "country": n.country,
                }
                for n in self.nodes
            ],
            "edges": [
                {"source": e.source, "target": e.target, "latency_ms": e.latency_ms}
                for e in self.edges
            ],
            "fanout_nodes": sorted(self.fanout_nodes),
        }

    def save(self, path: Path) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)


def load_topology(path: Path) -> Topology:
    """Load a topology from a JSON file."""
    with open(path) as f:
        data = json.load(f)
    nodes = [
        NodeSpec(
            num=n["num"],
            upload_bw_mbps=n["upload_bw_mbps"],
            download_bw_mbps=n["download_bw_mbps"],
            country=n["country"],
        )
        for n in data["nodes"]
    ]
    edges = [
        Edge(source=e["source"], target=e["target"], latency_ms=e["latency_ms"])
        for e in data["edges"]
    ]
    # Ensure bidirectionality: if (u,v) exists but (v,u) doesn't, add (v,u).
    existing = {(e.source, e.target): e.latency_ms for e in edges}
    for (u, v), latency in list(existing.items()):
        if (v, u) not in existing:
            edges.append(Edge(source=v, target=u, latency_ms=latency))
            existing[(v, u)] = latency
    return Topology(
        nodes=nodes, edges=edges, fanout_nodes=set(data.get("fanout_nodes", []))
    )


def load_latencies() -> dict[str, dict[str, int]]:
    """Load country-to-country latencies."""
    data_dir = Path(__file__).parent.parent / "data"
    with open(data_dir / "country_latencies.json") as f:
        return json.load(f)


def load_weights() -> dict[str, int]:
    """Load country weights for random selection."""
    data_dir = Path(__file__).parent.parent / "data"
    with open(data_dir / "country_weights.json") as f:
        return json.load(f)


class CountrySelector:
    def __init__(self, weights: dict[str, int], rng: random.Random):
        self.countries = list(weights.keys())
        self.cumulative = []
        total = 0
        for c in self.countries:
            total += weights[c]
            self.cumulative.append(total)
        self.total = total
        self.rng = rng

    def select(self) -> str:
        r = self.rng.randint(0, self.total - 1)
        for i, cum in enumerate(self.cumulative):
            if r < cum:
                return self.countries[i]
        return self.countries[-1]


# Distinct RNG stream constant so the supernode draw gets its own generator and doesn't
# perturb the country-selection sequence.
_SUPER_STREAM = 7

# Same-subnet neighbor floor for finality-subnet members (~10-15 after in-edges): the vote
# flood needs every member able to form a full D=8 mesh, which the connectivity tree alone
# (degree ~2, leaves 1) cannot provide. Mirrored by netsim's finalityGroupDegree.
FINALITY_GROUP_DEGREE = 12


def supernode_ids(num_nodes: int, super_node_fraction: float, seed: int) -> set[int]:
    """The node ids that get supernode (1024/1024 Mbit) bandwidth — a pure, seeded function
    so the committee proposer schedule (committee.py) and the topology bandwidth agree on
    exactly the same set. Node 0 is always a supernode when the fraction is on (it is the
    block builder); every other node is an independent draw. Empty when the fraction is ~0
    (node 0 is then a 25/50 block builder)."""
    if super_node_fraction <= 0.0001:
        return set()
    rng = random.Random(seed * 1_000_003 + _SUPER_STREAM)
    supers = {0}
    for i in range(1, num_nodes):
        if rng.random() < super_node_fraction:
            supers.add(i)
    return supers


class _Graph:
    """Undirected simple-graph builder mirroring netsim/graph.go: random spanning trees give
    connectivity, random fill tops nodes up to a target degree. Edges are symmetric;
    self-loops and duplicates are dropped."""

    def __init__(self, n: int):
        self.n = n
        self.adj: dict[int, set[int]] = {i: set() for i in range(n)}

    def add(self, i: int, j: int) -> None:
        if i == j or not (0 <= i < self.n and 0 <= j < self.n):
            return
        self.adj[i].add(j)
        self.adj[j].add(i)

    def random_tree(self, ids: list[int], rng: random.Random) -> None:
        """Link ids into one connected component: each id after the first attaches to a
        random earlier one. Fewer than two ids is a no-op (trivially connected)."""
        for i in range(1, len(ids)):
            self.add(ids[i], ids[rng.randrange(i)])

    def group_fill(self, ids: list[int], k: int, rng: random.Random) -> None:
        """Top every id up toward k neighbors WITHIN ids. Gossipsub meshes only form over
        existing links, so a flood-bearing membership group needs ~D internal degree per
        member — the tree alone leaves leaves at 1. Bounded retries like fill's."""
        members = set(ids)
        target = min(k, len(ids) - 1)
        for i in ids:
            in_group = len(self.adj[i] & members)
            tries = 0
            while in_group < target and tries < k * 4:
                peer = ids[rng.randrange(len(ids))]
                tries += 1
                if peer == i or peer in self.adj[i]:
                    continue
                self.add(i, peer)
                in_group += 1

    def fill(self, k: int, rng: random.Random) -> None:
        """Top every node up toward degree k with random peers; bounded retries so a small
        or dense graph degrades gracefully instead of spinning."""
        for i in range(self.n):
            tries = 0
            while len(self.adj[i]) < k and tries < k * 4:
                self.add(i, rng.randrange(self.n))
                tries += 1


def _make_country_nodes(
    num_nodes: int, supers: set[int], selector: CountrySelector
) -> list[NodeSpec]:
    """One NodeSpec per node: a supernode (1024/1024) iff its id is in `supers`, else a
    home-staker 25/50 link, plus a weighted-random country from the selector's rng."""
    nodes = []
    for i in range(num_nodes):
        up, down = (1024, 1024) if i in supers else (25, 50)
        nodes.append(
            NodeSpec(num=i, upload_bw_mbps=up, download_bw_mbps=down, country=selector.select())
        )
    return nodes


def _edges_from_adjacency(
    adj: dict[int, set[int]],
    nodes: list[NodeSpec],
    latencies: dict[str, dict[str, int]],
    min_latency_ms: int,
) -> list[Edge]:
    """One directed Edge per adjacency entry with the source→target country latency, so a
    symmetric adjacency yields both directions. Latency is floored at min_latency_ms."""
    country = {n.num: n.country for n in nodes}
    edges = []
    for u in sorted(adj):
        for v in adj[u]:
            lat = max(latencies.get(country[u], {}).get(country[v], 100), min_latency_ms)
            edges.append(Edge(source=u, target=v, latency_ms=lat))
    return edges


def generate_random_topology(
    num_nodes: int,
    degree: int,
    seed: int,
    super_node_fraction: float = 0.0,
    fanout_nodes: int = 0,
    fanout_node_mesh_peers: int = 1,
    min_latency_ms: int = 0,
) -> Topology:
    """Generate a random topology with the given parameters.

    Creates num_nodes mesh nodes connected with the given degree.
    If fanout_nodes > 0, creates additional fanout nodes (numbered
    num_nodes..num_nodes+fanout_nodes-1) each connected to
    fanout_node_mesh_peers random mesh nodes.
    """
    rng = random.Random(seed)
    weights = load_weights()
    latencies = load_latencies()
    selector = CountrySelector(weights, rng)

    supers = supernode_ids(num_nodes, super_node_fraction, seed)
    nodes = _make_country_nodes(num_nodes, supers, selector)

    # Create mesh edges - first ensure connectivity
    adjacency: dict[int, set[int]] = {i: set() for i in range(num_nodes)}
    for i in range(1, num_nodes):
        j = rng.randint(0, i - 1)
        adjacency[i].add(j)
        adjacency[j].add(i)

    # Add more edges to reach desired degree
    max_attempts = num_nodes * 10
    for u in range(num_nodes):
        attempts = 0
        while len(adjacency[u]) < degree and attempts < max_attempts:
            v = rng.randint(0, num_nodes - 1)
            if v != u and v not in adjacency[u] and len(adjacency[v]) < degree:
                adjacency[u].add(v)
                adjacency[v].add(u)
            attempts += 1

    edges = _edges_from_adjacency(adjacency, nodes, latencies, min_latency_ms)

    # Create fanout nodes and connect each to random mesh peers
    fanout_node_nums = set()
    if fanout_nodes > 0:
        fanout_rng = random.Random(seed + 1000)
        mesh_ids = list(range(num_nodes))
        k = min(fanout_node_mesh_peers, num_nodes)
        for i in range(fanout_nodes):
            node_num = num_nodes + i
            up, down = (1024, 1024) if fanout_rng.random() < super_node_fraction else (25, 50)
            node = NodeSpec(
                num=node_num,
                upload_bw_mbps=up,
                download_bw_mbps=down,
                country=selector.select(),
            )
            nodes.append(node)
            fanout_node_nums.add(node_num)
            peers = sorted(fanout_rng.sample(mesh_ids, k=k))
            for peer_num in peers:
                lat = max(
                    latencies.get(node.country, {}).get(nodes[peer_num].country, 100),
                    min_latency_ms,
                )
                edges.append(Edge(source=node_num, target=peer_num, latency_ms=lat))
                edges.append(Edge(source=peer_num, target=node_num, latency_ms=lat))

    return Topology(nodes=nodes, edges=edges, fanout_nodes=fanout_node_nums)


def generate_subnet_topology(
    num_nodes: int,
    k: int,
    seed: int,
    assignment,
    super_node_fraction: float = 0.0,
    min_latency_ms: int = 0,
) -> Topology:
    """discv5-biased topology: every node targets K long-lived peers, biased so each subnet's
    subscribers form a connected subgraph (an attestation handed to a couple of them floods to
    all of them); the block topic rides the same graph. Mirrors netsim/subnet.go discv5Graph —
    a global random spanning tree, then a random spanning tree per subnet's subscribers, a
    random spanning tree per column's custodiers (when the data-columns phase is on), a random
    spanning tree per sync subnet's members (when the sync phase is on), then random fill up to
    K (subnet/column/sync edges counted toward K). K is clamped to N-1 and the fill is
    best-effort, so a small or dense graph degrades gracefully.

    `assignment` is a simctl.schedule.Assignment; its subnet_subscribers (and, for the column
    phase, column_subscribers) drive the bias.
    """
    rng = random.Random(seed)
    selector = CountrySelector(load_weights(), rng)
    latencies = load_latencies()
    supers = supernode_ids(num_nodes, super_node_fraction, seed)
    nodes = _make_country_nodes(num_nodes, supers, selector)

    g = _Graph(num_nodes)
    g.random_tree(list(range(num_nodes)), rng)  # global: keep the block topic connected
    for subs in assignment.subnet_subscribers:
        g.random_tree(list(subs), rng)  # each subnet's subscribers: one connected piece
    for subs in assignment.column_subscribers or []:
        g.random_tree(list(subs), rng)  # each column's custodiers: one connected piece (DA backbone)
    for subs in assignment.sync_subscribers or []:
        g.random_tree(list(subs), rng)  # each sync subnet's members: one connected piece
    for subs in assignment.finality_subscribers or []:
        # Finality subnets carry the vote flood: the tree gives connectivity, the group fill
        # gives every member enough co-member links (~10-15) to form a real D=8 mesh.
        g.random_tree(list(subs), rng)
        g.group_fill(list(subs), FINALITY_GROUP_DEGREE, rng)
    g.fill(min(k, num_nodes - 1), rng)

    edges = _edges_from_adjacency(g.adj, nodes, latencies, min_latency_ms)
    return Topology(nodes=nodes, edges=edges, fanout_nodes=set())


def generate_ring_topology(
    num_nodes: int,
    seed: int,
    super_node_fraction: float = 0.0,
    min_latency_ms: int = 0,
) -> Topology:
    """Generate a ring topology."""
    rng = random.Random(seed)
    weights = load_weights()
    latencies = load_latencies()
    selector = CountrySelector(weights, rng)

    supers = supernode_ids(num_nodes, super_node_fraction, seed)
    nodes = _make_country_nodes(num_nodes, supers, selector)

    edges = []
    for i in range(num_nodes):
        j = (i + 1) % num_nodes
        src_country = nodes[i].country
        dst_country = nodes[j].country
        latency = max(
            latencies.get(src_country, {}).get(dst_country, 100), min_latency_ms
        )
        edges.append(Edge(source=i, target=j, latency_ms=latency))

    return Topology(nodes=nodes, edges=edges, fanout_nodes=set())
