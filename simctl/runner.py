"""Shadow run orchestration for block dissemination.

Adapted from batched-attestation-sim's runner, stripped of committees / fanout /
publish schedules: the cyclic proposer is computed in Go from -node-num and
-num-nodes, so Python only generates the topology, the GML network, and one host
per node.
"""

import math
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from simctl.config import SimConfig
from simctl.manifest import format_dir_timestamp, random_suffix
from simctl.topology import (
    Topology,
    generate_random_topology,
    generate_ring_topology,
)


def get_root() -> Path:
    """Repo root (parent of the simctl package)."""
    return Path(__file__).parent.parent.resolve()


def compute_peer_lists(topology: Topology) -> dict[int, list[int]]:
    """Per-node sorted neighbor list extracted from topology edges."""
    peers: dict[int, set[int]] = {node.num: set() for node in topology.nodes}
    for edge in topology.edges:
        peers[edge.source].add(edge.target)
        peers[edge.target].add(edge.source)
    return {num: sorted(s) for num, s in peers.items()}


def generate_gml(topology: Topology, latency_multiple: float = 1.0) -> str:
    """GML network graph for Shadow: per-node bandwidth, self-loops, and one
    edge per topology edge with its latency."""
    lines = ["graph [", "  directed 0"]
    for node in topology.nodes:
        lines.extend([
            "  node [",
            f"    id {node.num}",
            f'    host_bandwidth_up "{node.upload_bw_mbps} Mbit"',
            f'    host_bandwidth_down "{node.download_bw_mbps} Mbit"',
            "  ]",
        ])
    for node in topology.nodes:
        lines.extend([
            "  edge [",
            f"    source {node.num}",
            f"    target {node.num}",
            '    latency "1 ms"',
            "    packet_loss 0.0",
            "  ]",
        ])
    for edge in topology.edges:
        lines.extend([
            "  edge [",
            f"    source {edge.source}",
            f"    target {edge.target}",
            f'    latency "{round(edge.latency_ms * latency_multiple)} ms"',
            "    packet_loss 0.0",
            "  ]",
        ])
    lines.append("]")
    return "\n".join(lines)


def _host_args(config: SimConfig, node_num: int, num_nodes: int, peers: list[int]) -> str:
    """Command-line args for one slot-sim-node host. Shared flags are identical
    across hosts; node-num and peer-nums are per-host."""
    args = [
        f"-node-num={node_num}",
        f"-num-nodes={num_nodes}",
        f"-num-slots={config.num_slots}",
        f"-slot={config.slot_duration_seconds}s",
        f"-block-size={config.block_size}",
        f"-verify-delay={config.verify_delay_ms}ms",
        f"-offset={config.offset_ms}ms",
        f"-jitter={config.jitter_ms}ms",
        f"-D={config.gossipsub.D}",
        f"-Dlo={config.gossipsub.Dlow}",
        f"-Dhi={config.gossipsub.Dhigh}",
        f"-seed={config.seed}",
        f"-startup={config.startup_seconds}s",
    ]
    if peers:
        args.append(f"-peer-nums={','.join(str(p) for p in peers)}")
    return " ".join(args)


def _stop_time_minutes(config: SimConfig) -> int:
    """Shadow stop_time in whole minutes (Shadow rejects fractional values):
    enough for startup + the run + a drain slot + margin, never below the
    config's floor."""
    drain = config.slot_duration_seconds
    needed = config.startup_seconds + config.num_slots * config.slot_duration_seconds + drain + 30
    return max(math.ceil(config.stop_time_minutes), math.ceil(needed / 60))


def generate_shadow_yaml(
    config: SimConfig,
    topology: Topology,
    peer_lists: dict[int, list[int]],
    binary_path: str = "./slot-sim-node",
) -> dict[str, Any]:
    """Shadow configuration: one host per node, plus the GML network."""
    num_nodes = len(topology.nodes)
    gml = generate_gml(topology, config.topology.latency_multiple)
    hosts: dict[str, Any] = {}
    for node in topology.nodes:
        hosts[f"node{node.num}"] = {
            "network_node_id": node.num,
            "processes": [{
                "path": binary_path,
                "args": _host_args(config, node.num, num_nodes, peer_lists.get(node.num, [])),
                "start_time": "0 sec",
            }],
        }
    return {
        "general": {
            "stop_time": f"{_stop_time_minutes(config)} min",
            "log_level": config.log_level,
            "progress": True,
            "heartbeat_interval": "10s",
        },
        "network": {
            "graph": {"type": "gml", "inline": gml},
            "use_shortest_path": True,
        },
        "hosts": hosts,
    }


def write_shadow_config(config_dict: dict[str, Any], path: Path) -> None:
    with open(path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)


def build_node(output_path: Path) -> None:
    """Build the slot-sim-node Go binary to output_path."""
    subprocess.run(
        ["go", "build", "-buildvcs=false", "-o", str(output_path.resolve()), "./cmd/slot-sim-node"],
        check=True,
        cwd=get_root(),
    )


def _build_topology(config: SimConfig) -> Topology:
    tc = config.topology
    if tc.type == "random":
        return generate_random_topology(
            num_nodes=tc.num_nodes,
            degree=tc.degree,
            seed=tc.seed,
            super_node_fraction=tc.super_node_fraction,
            min_latency_ms=tc.min_node_to_node_latency_ms,
        )
    return generate_ring_topology(
        num_nodes=tc.num_nodes,
        seed=tc.seed,
        super_node_fraction=tc.super_node_fraction,
        min_latency_ms=tc.min_node_to_node_latency_ms,
    )


def _create_run_dir(output_dir: Path, config: SimConfig) -> Path:
    for _ in range(20):
        name = (
            f"run-{format_dir_timestamp(datetime.now())}-{random_suffix()}"
            f"-n{config.topology.num_nodes}-D{config.gossipsub.D}"
        )
        candidate = output_dir / name
        try:
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        except FileExistsError:
            continue
    raise RuntimeError("could not create a unique run directory after 20 attempts")


def prepare_run_dir(config: SimConfig, output_dir: Path) -> Path:
    """Create a unique run dir and write topology.json, config.yaml, and
    shadow.yaml — everything except the Go binary and the shadow run itself.
    Shared by run_simulation and --dry-run."""
    topology = _build_topology(config)
    peer_lists = compute_peer_lists(topology)

    run_dir = _create_run_dir(output_dir, config)
    topology.save(run_dir / "topology.json")
    with open(run_dir / "config.yaml", "w") as f:
        yaml.dump(config.model_dump(), f, default_flow_style=False, sort_keys=False)
    write_shadow_config(generate_shadow_yaml(config, topology, peer_lists), run_dir / "shadow.yaml")
    return run_dir


def run_simulation(config: SimConfig, output_dir: Path) -> tuple[Path, subprocess.CompletedProcess]:
    """Build, configure, and run one block-dissemination Shadow simulation.

    Returns (run_dir, completed_process).
    """
    run_dir = prepare_run_dir(config, output_dir)
    build_node(run_dir / "slot-sim-node")

    log_path = run_dir / "shadow.log"
    with open(log_path, "w") as log_file:
        result = subprocess.run(["shadow", "shadow.yaml"], cwd=run_dir, stdout=log_file, stderr=log_file)

    print(f"Shadow completed. Results in: {run_dir}")
    print(f"Exit code: {result.returncode}")
    return run_dir, result
