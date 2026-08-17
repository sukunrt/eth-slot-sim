"""Shadow + simnet run orchestration.

Generates the schedule assignment (when the config has an attestation block — present
even with enabled: false for the column/sync/decoupled phases, which build on it), the
topology and GML network, and one Shadow host per node. The per-slot block proposer
(a supernode) lives in schedule.json; the attestation duties are computed in Go from
schedule.json + flags. So Python sets up inputs and both backends read the same files.
Also drives the simnet backend and the cross-backend comparison.
"""

import json
import math
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from analysis import check_arrivals
from simctl import schedule
from simctl import config as config_module
from simctl.config import SimConfig
from simctl.manifest import format_dir_timestamp, random_suffix, write_json_atomic
from simctl.topology import (
    Topology,
    generate_random_topology,
    generate_ring_topology,
    generate_subnet_topology,
    supernode_ids,
)


def _schedule_assignment(config: SimConfig) -> schedule.Assignment | None:
    """The schedule assignment for this run, or None if the config has no attestation block.
    Presence of the block — not attestation.enabled — is the gate: the column/sync/decoupled
    phases keep the block (enabled: false) because it supplies V and the deadline, and config
    validation enforces that. Any assignment also switches _build_topology to the
    subnet-biased graph, so every membership group it carries is wired as a connected piece.
    Block proposers are drawn from the topology's supernode set (the same supernode_ids the
    topology bandwidth uses, keyed by the topology seed), so only supernodes propose."""
    a = config.attestation
    if a is None:
        return None
    tc = config.topology
    supers = sorted(supernode_ids(tc.num_nodes, tc.super_node_fraction, tc.seed))
    col_kwargs: dict[str, Any] = {}
    dc = config.data_columns
    if dc is not None and dc.enabled:  # the data-columns phase adds custody to schedule.json
        col_kwargs = dict(
            num_columns=dc.num_columns,
            custody_floor=dc.custody_floor,
            full_custody_fraction=dc.full_custody_fraction,
            column_backbone_floor=dc.column_backbone_floor,
            per_subnet_floor=dc.per_subnet_floor,
        )
    sync_kwargs: dict[str, Any] = {}
    sc = config.sync
    if sc is not None and sc.enabled:  # the sync phase adds node-based membership to schedule.json
        sync_kwargs = dict(
            sync_size=sc.size,
            sync_subnets=sc.subnets,
            sync_target_aggregators=sc.target_aggregators,
        )
    dist_kwargs: dict[str, Any] = {}
    vd = config.validator_distribution
    if vd is not None and vd.type != "uniform":  # the Dist seam: V becomes Σ counts (emergent)
        if "validators" in a.model_fields_set:
            print(f"validator_distribution={vd.type}: V is emergent — attestation.validators ignored")
        dist_kwargs = dict(
            dist=vd.type,
            regular_weights=tuple(vd.regular.weights),
            super_min=vd.super.min,
            super_max=vd.super.max,
            super_mean=vd.super.mean,
            dist_seed=vd.seed,
            explicit_counts=tuple(vd.counts) if vd.counts is not None else None,
        )
    dcc_kwargs: dict[str, Any] = {}
    dcc = config.decoupled_consensus
    if dcc is not None and dcc.enabled:  # decoupled replaces committee + sync gen with AC/FC membership
        dcc_kwargs = dict(
            decoupled=True,
            ac_vote_size=dcc.ac_vote_size,
            ac_slots_per_finality_slot=dcc.ac_slots_per_finality_slot,
            fs_subnets=dcc.fs_subnets,
            fs_aggregators=dcc.fs_aggregators,
            validator_segregation=dcc.validator_segregation,
        )
    return schedule.generate(
        schedule.Params(
            n=tc.num_nodes,
            v=a.validators,
            c=a.committees,
            sc=a.committee_size,
            subnet_count=a.subnet_count,
            subnets_per_node=a.subnets_per_node,
            subscribe_floor=a.subscribe_floor,
            target_aggregators=a.target_aggregators,
            seed=config.seed,
            num_slots=config.num_slots,
            **col_kwargs,
            **sync_kwargs,
            **dcc_kwargs,
            **dist_kwargs,
        ),
        supers=supers,
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


def _host_args(
    config: SimConfig, node_num: int, num_nodes: int, peers: list[int], schedule_path: str
) -> str:
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
        f"-rpc-log-node={config.rpc_log_node}",
    ]
    # Always passed explicitly (the Go flag also defaults to true): -block-size sizes the
    # payload, the big message.
    if config.epbs.enabled:
        e = config.epbs
        args += [
            "-epbs=true",
            f"-consensus-block-size={e.consensus_block_size}",
            f"-payload-offset={e.payload_offset_ms}ms",
            f"-payload-jitter={e.payload_jitter_ms}ms",
        ]
    else:
        args.append("-epbs=false")
    if schedule_path:
        # Always passed (absolute: a host's cwd is its own data dir). It carries the proposer
        # schedule, which applies whether or not attestations are on; -attestations alone gates
        # attestation traffic.
        args.append(f"-schedule={schedule_path}")
    if config.attestation is not None:
        a = config.attestation
        args += [
            f"-attestations={'true' if a.enabled else 'false'}",  # false ⇒ block-only, schedule kept
            f"-att-due={a.attestation_due_ms}ms",
            f"-agg-due={a.aggregate_due_ms}ms",
            f"-prep={a.prep_ms}ms",
            f"-attest-verify-delay={a.verify_delay_ms}ms",
            f"-attest-per-item={a.per_item_ms}ms",
            f"-attest-batch-window={a.batch_window_ms}ms",
            f"-attest-batch-max={a.batch_max_items}",
        ]
        if a.transport == "partial":
            pp = a.partial or config_module.PartialConfig()
            args += [
                "-transport=partial",
                f"-partial-publish-interval={pp.publish_interval_ms}ms",
                f"-partial-max-peers-per-attestation={pp.max_peers_per_attestation}",
                f"-partial-max-iwant-per-position={pp.max_iwant_per_position}",
                f"-partial-attestation-data-size={pp.attestation_data_size}",
                f"-partial-signature-size={pp.signature_size}",
                f"-partial-disable-metadata-gossip={'true' if pp.disable_metadata_gossip else 'false'}",
            ]
    if config.sync is not None:
        # size/subnets/aggregators ride schedule.json; the deadlines reuse -att-due/-agg-due.
        args.append(f"-sync={'true' if config.sync.enabled else 'false'}")
    dcc = config.decoupled_consensus
    if dcc is not None and dcc.enabled:
        # ac_vote_size/fs_subnets/fs_aggregators ride schedule.json; the AC deadline reuses -att-due.
        args += [
            "-decoupled=true",
            f"-k={dcc.ac_slots_per_finality_slot}",
            f"-fc-vote-offset={dcc.fc_vote_offset_ms}ms",
            f"-fc-agg-fraction={dcc.finality_slot_aggregation_fraction}",
        ]
        if dcc.validator_segregation:  # per-AC-slot rounds; the binary asserts the schedule agrees
            args += [
                "-fc-segregated=true",
                f"-fc-round-agg-fraction={dcc.round_aggregation_fraction}",
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
    schedule_path: str = "",
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
                "args": _host_args(config, node.num, num_nodes, peer_lists.get(node.num, []), schedule_path),
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
    """Build the slot-sim-node Go binary (one Shadow host) to output_path."""
    _go_build("./cmd/slot-sim-node", output_path)


def _go_build(pkg: str, output_path: Path) -> None:
    subprocess.run(
        ["go", "build", "-buildvcs=false", "-o", str(output_path.resolve()), pkg],
        check=True,
        cwd=get_root(),
    )


def _simnet_params(config: SimConfig) -> dict[str, Any]:
    """Scenario knobs for the simnet (synctest) backend, matching the Shadow run.
    The harness reads these as JSON via SIMRUN_PARAMS; topology and csv paths are
    added per-run by run_simnet. latency_multiple mirrors the scaling generate_gml
    applies for Shadow."""
    params: dict[str, Any] = {
        "latency_multiple": config.topology.latency_multiple,
        "num_slots": config.num_slots,
        "slot_seconds": config.slot_duration_seconds,
        "block_size": config.block_size,
        "verify_ms": config.verify_delay_ms,
        "offset_ms": config.offset_ms,
        "jitter_ms": config.jitter_ms,
        "d": config.gossipsub.D,
        "dlo": config.gossipsub.Dlow,
        "dhi": config.gossipsub.Dhigh,
        "seed": config.seed,
    }
    if config.epbs.enabled:
        params.update(
            epbs=True,
            consensus_block_size=config.epbs.consensus_block_size,
            payload_offset_ms=config.epbs.payload_offset_ms,
            payload_jitter_ms=config.epbs.payload_jitter_ms,
        )
    if config.attestation is not None:
        a = config.attestation
        params.update(
            attest=a.enabled,
            att_due_ms=a.attestation_due_ms,
            agg_due_ms=a.aggregate_due_ms,
            prep_ms=a.prep_ms,
            att_verify_ms=a.verify_delay_ms,
            att_per_item_ms=a.per_item_ms,
            att_batch_window_ms=a.batch_window_ms,
            att_batch_max=a.batch_max_items,
        )
        if a.transport == "partial":
            pp = a.partial or config_module.PartialConfig()
            params.update(
                transport="partial",
                partial_publish_interval_ms=pp.publish_interval_ms,
                partial_max_peers_per_attestation=pp.max_peers_per_attestation,
                partial_max_iwant_per_position=pp.max_iwant_per_position,
                partial_attestation_data_size=pp.attestation_data_size,
                partial_signature_size=pp.signature_size,
                partial_disable_metadata_gossip=pp.disable_metadata_gossip,
            )
    dc = config.data_columns
    if dc is not None and dc.enabled:  # custody lives in schedule.json; these size the verifier
        params.update(
            col_verify_service_ms=dc.verify_service_ms,
            col_verify_super=dc.verify_parallelism_super,
            col_verify_regular=dc.verify_parallelism_regular,
        )
    sc = config.sync
    if sc is not None and sc.enabled:  # membership lives in schedule.json; this just flips it on
        params.update(sync=True)
    dcc = config.decoupled_consensus
    if dcc is not None and dcc.enabled:  # membership/voters live in schedule.json; these are the knobs
        params.update(
            decoupled=True,
            k=dcc.ac_slots_per_finality_slot,
            fc_vote_offset_ms=dcc.fc_vote_offset_ms,
            fc_agg_fraction=dcc.finality_slot_aggregation_fraction,
        )
        if dcc.validator_segregation:  # per-AC-slot rounds; TestRun asserts the schedule agrees
            params.update(
                fc_segregated=True,
                fc_round_agg_fraction=dcc.round_aggregation_fraction,
            )
    return params


def _build_topology(config: SimConfig, assignment: schedule.Assignment | None) -> Topology:
    tc = config.topology
    if assignment is not None:
        # Any scheduled run (attestation / columns / sync / decoupled): one discv5-biased
        # graph at target degree K (= tc.degree) where every membership group the schedule
        # carries — attestation subnets, column custodiers, sync subnets, finality subnets —
        # is a connected piece, so each topic floods within its group and the block topic
        # rides the same peers. tc.type only applies to block-only runs (no schedule).
        return generate_subnet_topology(
            num_nodes=tc.num_nodes,
            k=tc.degree,
            seed=tc.seed,
            assignment=assignment,
            super_node_fraction=tc.super_node_fraction,
            min_latency_ms=tc.min_node_to_node_latency_ms,
        )
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
    assignment = _schedule_assignment(config)
    topology = _build_topology(config, assignment)
    peer_lists = compute_peer_lists(topology)

    run_dir = _create_run_dir(output_dir, config)
    topology.save(run_dir / "topology.json")
    schedule_path = ""
    if assignment is not None:
        schedule_path = str((run_dir / "schedule.json").resolve())
        write_json_atomic(run_dir / "schedule.json", assignment.to_dict())
    with open(run_dir / "config.yaml", "w") as f:
        yaml.dump(config.model_dump(), f, default_flow_style=False, sort_keys=False)
    write_shadow_config(
        generate_shadow_yaml(config, topology, peer_lists, schedule_path), run_dir / "shadow.yaml"
    )
    return run_dir


def _run_shadow(run_dir: Path) -> subprocess.CompletedProcess:
    """Build the node binary into run_dir and run shadow there, logging to
    shadow.log. Assumes shadow.yaml + topology.json already exist in run_dir."""
    build_node(run_dir / "slot-sim-node")
    with open(run_dir / "shadow.log", "w") as log_file:
        return subprocess.run(["shadow", "shadow.yaml"], cwd=run_dir, stdout=log_file, stderr=log_file)


def run_simulation(config: SimConfig, output_dir: Path) -> tuple[Path, subprocess.CompletedProcess]:
    """Build, configure, and run one block-dissemination Shadow simulation.

    Returns (run_dir, completed_process).
    """
    run_dir = prepare_run_dir(config, output_dir)
    result = _run_shadow(run_dir)
    print(f"Shadow completed. Results in: {run_dir}")
    print(f"Exit code: {result.returncode}")
    if result.returncode == 0:
        _convert_to_parquet(run_dir)
    return run_dir, result


def _convert_to_parquet(run_dir: Path) -> None:
    """Convert the run's slog output to parquet event tables (analysis/to_parquet.py) so
    analysis never re-parses the raw JSON — and so the remote flow can tar the small
    parquet view separately. Additive only (raw logs untouched); a conversion failure
    must never taint the run, so it only warns."""
    try:
        from analysis import to_parquet

        out = to_parquet.convert(run_dir)
        print(f"Parquet event tables in: {out}")
    except Exception as e:  # noqa: BLE001 — the raw logs remain the source of truth
        print(f"WARNING: parquet conversion failed ({e}); analyze from the raw logs")


def run_simnet(config: SimConfig, run_dir: Path) -> Path:
    """Run the in-process simnet backend on run_dir/topology.json under synctest —
    the only clock under which simnet's timing is meaningful (a plain binary on the
    OS clock measures scheduler latency, not the network) — with the same scenario
    knobs as the Shadow run, logging to simnet.log. synctest runs only under
    `go test`, so the backend is the TestRun harness driven via -tags simnetrun and
    a SIMRUN_PARAMS JSON. Returns the arrival CSV path."""
    csv_path = run_dir / "simnet_arrivals.csv"
    params = _simnet_params(config)
    params["topology"] = str((run_dir / "topology.json").resolve())
    params["csv"] = str(csv_path.resolve())
    schedule_path = run_dir / "schedule.json"
    if schedule_path.exists():
        params["schedule"] = str(schedule_path.resolve())
    params_path = run_dir / "simnet_params.json"
    write_json_atomic(params_path, params)

    env = {**os.environ, "SIMRUN_PARAMS": str(params_path.resolve())}
    cmd = ["go", "test", "-tags", "simnetrun", "-run", "TestRun",
           "-count=1", "-timeout", "600s", "./simnetrun"]
    with open(run_dir / "simnet.log", "w") as log_file:
        subprocess.run(cmd, cwd=get_root(), env=env, stdout=log_file, stderr=log_file, check=True)
    return csv_path


def run_comparison(config: SimConfig, output_dir: Path) -> dict[str, Any]:
    """Run one topology on both backends and return (and save) their arrival CDFs.

    A single run dir is generated; its topology.json is consumed by both Shadow
    and simnet, so the only difference between the two runs is the network
    backend and the CDFs compare like for like. Writes compare.json."""
    run_dir = prepare_run_dir(config, output_dir)

    shadow_proc = _run_shadow(run_dir)
    if shadow_proc.returncode != 0:
        raise RuntimeError(f"shadow failed (exit {shadow_proc.returncode}); see {run_dir}/shadow.log")
    pubs, arrs, node_nums = check_arrivals.load_run(run_dir)
    shadow_res = check_arrivals.analyze(pubs, arrs, node_nums)

    csv_path = run_simnet(config, run_dir)
    simnet_delays = check_arrivals.delays_from_csv(csv_path)

    comparison: dict[str, Any] = {
        "run_dir": str(run_dir),
        "expected_arrivals": shadow_res.expected,
        "shadow": {
            "arrivals": shadow_res.arrivals,
            "missing": len(shadow_res.missing),
            "duplicates": len(shadow_res.duplicates),
            "cdf_ms": check_arrivals.cdf(shadow_res.delays_ms),
        },
        "simnet": {
            "arrivals": len(simnet_delays),
            "cdf_ms": check_arrivals.cdf(simnet_delays),
        },
    }

    # ePBS: both halves are block-shaped, so the block analyzer covers them per kind (the
    # legacy blocks section above is all-zero when epbs is on).
    if config.epbs.enabled:
        for name, kind in (
            ("consensus_blocks", check_arrivals.CONSENSUS_BLOCK_KIND),
            ("execution_payloads", check_arrivals.EXECUTION_PAYLOAD_KIND),
        ):
            sres = check_arrivals.analyze(pubs, arrs, node_nums, kind=kind)
            sim_delays = check_arrivals.delays_from_csv(csv_path, kind=kind)
            comparison[name] = {
                "expected_arrivals": sres.expected,
                "shadow": {
                    "arrivals": sres.arrivals,
                    "missing": len(sres.missing),
                    "duplicates": len(sres.duplicates),
                    "cdf_ms": check_arrivals.cdf(sres.delays_ms),
                },
                "simnet": {
                    "arrivals": len(sim_delays),
                    "cdf_ms": check_arrivals.cdf(sim_delays),
                },
            }

    # Attestation phase: report both backends' coverage/no-leak + CDF. The simnet check
    # against schedule.json is the only automated coverage test of the real topology.json
    # graph (the Go suites only exercise the in-process discv5Graph). Skipped when
    # attestations are disabled — schedule.json still exists (for the proposer schedule),
    # but no attestation traffic was emitted, so coverage would be a spurious all-missing.
    attest_on = config.attestation is not None and config.attestation.enabled
    subscribers = check_arrivals.load_committee(run_dir) if attest_on else None
    if subscribers is not None:
        schedule_data = json.loads((run_dir / "schedule.json").read_text())
        shadow_att = check_arrivals.analyze_attestations(pubs, arrs, subscribers)
        simnet_att = check_arrivals.analyze_attestations_csv(csv_path, schedule_data)
        comparison["attestations"] = {
            "expected": shadow_att.expected,
            "shadow": _att_summary(shadow_att),
            "simnet": _att_summary(simnet_att),
        }
        if schedule_data["slots"] and schedule_data["slots"][0].get("aggregators"):
            shadow_agg = check_arrivals.analyze_aggregates(pubs, arrs, schedule_data)
            simnet_agg = check_arrivals.analyze_aggregates_csv(csv_path, schedule_data)
            comparison["aggregates"] = {
                "expected": shadow_agg.expected,
                "shadow": _agg_summary(shadow_agg),
                "simnet": _agg_summary(simnet_agg),
            }

    # Data-columns phase: report both backends' column coverage/no-leak + CDF. Independent of
    # the attestation gate above (a columns-only run still disseminates + measures columns).
    columns_on = config.data_columns is not None and config.data_columns.enabled
    if columns_on:
        schedule_data = json.loads((run_dir / "schedule.json").read_text())
        custodiers = {col: set(m) for col, m in enumerate(schedule_data["column_subscribers"])}
        shadow_col = check_arrivals.analyze_columns(pubs, arrs, custodiers)
        simnet_col = check_arrivals.analyze_columns_csv(csv_path, schedule_data)
        comparison["columns"] = {
            "expected": shadow_col.expected,
            "shadow": _col_summary(shadow_col),
            "simnet": _col_summary(simnet_col),
        }

    # Sync-committee phase: per-subnet message coverage/no-leak + fraction-voted-head, and the
    # global contribution coverage. Independent of the attestation/column phases above.
    sync_on = config.sync is not None and config.sync.enabled
    if sync_on:
        schedule_data = json.loads((run_dir / "schedule.json").read_text())
        sync_subs = {i: set(m) for i, m in enumerate(schedule_data["sync_subscribers"])}
        shadow_sm = check_arrivals.analyze_sync_messages(pubs, arrs, sync_subs)
        simnet_sm = check_arrivals.analyze_sync_messages_csv(csv_path, schedule_data)
        comparison["sync_messages"] = {
            "expected": shadow_sm.expected,
            "shadow": _sync_msg_summary(shadow_sm),
            "simnet": _sync_msg_summary(simnet_sm),
        }
        if schedule_data["slots"] and schedule_data["slots"][0].get("sync_aggregators"):
            shadow_sc = check_arrivals.analyze_sync_contributions(pubs, arrs, schedule_data)
            simnet_sc = check_arrivals.analyze_sync_contributions_csv(csv_path, schedule_data)
            comparison["sync_contributions"] = {  # AggregateResult shape; reuse _agg_summary
                "expected": shadow_sc.expected,
                "shadow": _agg_summary(shadow_sc),
                "simnet": _agg_summary(simnet_sc),
            }

    # Decoupled-consensus phase: global AC-vote coverage + fraction-voted-block (AttestResult shape),
    # per-subnet finality-vote coverage/no-leak (SyncMessageResult shape), and global finality-aggregate
    # coverage (AggregateResult shape). Mutually exclusive with attestation/sync above.
    decoupled_on = config.decoupled_consensus is not None and config.decoupled_consensus.enabled
    if decoupled_on:
        schedule_data = json.loads((run_dir / "schedule.json").read_text())
        shadow_ac = check_arrivals.analyze_ac_votes(pubs, arrs, schedule_data)
        simnet_ac = check_arrivals.analyze_ac_votes_csv(csv_path, schedule_data)
        comparison["ac_votes"] = {
            "expected": shadow_ac.expected,
            "shadow": _att_summary(shadow_ac),
            "simnet": _att_summary(simnet_ac),
        }
        shadow_fv = check_arrivals.analyze_finality_votes(pubs, arrs, schedule_data)
        simnet_fv = check_arrivals.analyze_finality_votes_csv(csv_path, schedule_data)
        comparison["finality_votes"] = {
            "expected": shadow_fv.expected,
            "shadow": _sync_msg_summary(shadow_fv),
            "simnet": _sync_msg_summary(simnet_fv),
        }
        if any(sp.get("finality_aggregators") for sp in schedule_data["slots"]):
            shadow_fa = check_arrivals.analyze_finality_aggregates(pubs, arrs, schedule_data)
            simnet_fa = check_arrivals.analyze_finality_aggregates_csv(csv_path, schedule_data)
            comparison["finality_aggregates"] = {  # AggregateResult shape; reuse _agg_summary
                "expected": shadow_fa.expected,
                "shadow": _agg_summary(shadow_fa),
                "simnet": _agg_summary(simnet_fa),
            }

    # Proposer guard: every scheduled proposer is a supernode, and every Shadow block was
    # published by its slot's proposer. Both backends read this one schedule.json, so a
    # failure means the schedule or the two generated files disagree.
    proposers = check_arrivals.load_proposers(run_dir)
    supernodes = check_arrivals.load_supernodes(run_dir)
    if proposers is not None and supernodes is not None:
        problems = check_arrivals.check_proposers(
            proposers, supernodes, check_arrivals.block_origins(pubs)
        )
        if problems:
            raise RuntimeError("proposer guard failed: " + "; ".join(problems))
        comparison["proposers_are_supernodes"] = True

    write_json_atomic(run_dir / "compare.json", comparison)
    return comparison


def _agg_summary(res: check_arrivals.AggregateResult) -> dict[str, Any]:
    """One backend's aggregate result as a compare.json sub-dict."""
    return {
        "arrivals": res.arrivals,
        "expected": res.expected,
        "published": res.published,
        "missing": len(res.missing),
        "leaked": len(res.leaked),
        "duplicates": len(res.duplicates),
        "cdf_ms": check_arrivals.cdf(res.delays_ms),
    }


def _col_summary(res: check_arrivals.ColumnResult) -> dict[str, Any]:
    """One backend's column result as a compare.json sub-dict."""
    return {
        "arrivals": res.arrivals,
        "expected": res.expected,
        "published": res.published,
        "missing": len(res.missing),
        "leaked": len(res.leaked),
        "duplicates": len(res.duplicates),
        "cdf_ms": check_arrivals.cdf(res.delays_ms),
    }


def _att_summary(res: check_arrivals.AttestResult) -> dict[str, Any]:
    """One backend's attestation result as a compare.json sub-dict."""
    return {
        "arrivals": res.arrivals,
        "expected": res.expected,
        "missing": len(res.missing),
        "leaked": len(res.leaked),
        "duplicates": len(res.duplicates),
        "fraction_voted_block": res.fraction_voted_block,
        "cdf_ms": check_arrivals.cdf(res.delays_ms),
    }


def _sync_msg_summary(res: check_arrivals.SyncMessageResult) -> dict[str, Any]:
    """One backend's sync-message result as a compare.json sub-dict (with fraction_voted_head,
    the un-gated head vote that sits next to the attestation's column-gated fraction_voted_block)."""
    return {
        "arrivals": res.arrivals,
        "expected": res.expected,
        "missing": len(res.missing),
        "leaked": len(res.leaked),
        "duplicates": len(res.duplicates),
        "fraction_voted_head": res.fraction_voted_head,
        "cdf_ms": check_arrivals.cdf(res.delays_ms),
    }
