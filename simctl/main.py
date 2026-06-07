"""CLI entry point for eth-slot-sim block-dissemination Shadow runs."""

from pathlib import Path

import click

from . import config as cfgmod
from . import remote, runner


@click.group()
def cli() -> None:
    """simctl: run block-dissemination simulations on Shadow."""


def _run_remote(remote_host: str, simctl_args: list[str], output_dir: str, dry_run: bool) -> None:
    """Sync the repo to the remote, run `simctl <args>` there exactly as locally,
    then tarball the output dir and remove the original. Raises SystemExit on any
    non-zero step."""
    r = remote.Runner(dry_run=dry_run)

    click.echo(f"Syncing to {remote_host}...")
    sync_res, remote_cwd = r.sync_to_remote(remote_host, runner.get_root())
    if sync_res.returncode != 0:
        raise SystemExit(sync_res.returncode)

    click.echo(f"Running on {remote_host}:{remote_cwd}...")
    exit_code = r.run_remote_simctl(remote_host, remote_cwd, simctl_args)
    tar_rc = r.tar_and_cleanup(remote_host, remote_cwd, output_dir)
    rc = exit_code or tar_rc
    if rc != 0:
        raise SystemExit(rc)


@cli.command()
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to the run config YAML.",
)
@click.option("--output-dir", default="./runs", help="Parent directory for the run output.")
@click.option("--remote", "remote_host", default=None, help="Run on a remote host (user@hostname).")
@click.option(
    "--dry-run",
    is_flag=True,
    help="With --remote, print the rsync/ssh commands without executing. Otherwise "
    "generate the run dir + shadow.yaml without building or running shadow.",
)
def run(config_path: str, output_dir: str, remote_host: str | None, dry_run: bool) -> None:
    """Run one block-dissemination simulation on Shadow."""
    if remote_host:
        _run_remote(
            remote_host,
            ["run", "--config", config_path, "--output-dir", output_dir],
            output_dir,
            dry_run,
        )
        return
    cfg = cfgmod.load_config(Path(config_path))
    if dry_run:
        run_dir = runner.prepare_run_dir(cfg, Path(output_dir))
        click.echo(f"[dry-run] wrote {run_dir}/shadow.yaml (binary not built, shadow not run)")
        return
    _, result = runner.run_simulation(cfg, Path(output_dir))
    if result.returncode != 0:
        raise SystemExit(result.returncode)


@cli.command()
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to the run config YAML.",
)
@click.option("--output-dir", default="./runs", help="Parent directory for the run output.")
@click.option("--remote", "remote_host", default=None, help="Run on a remote host (user@hostname).")
@click.option(
    "--dry-run",
    is_flag=True,
    help="With --remote, print the rsync/ssh commands without executing.",
)
def compare(config_path: str, output_dir: str, remote_host: str | None, dry_run: bool) -> None:
    """Run the same topology on both Shadow and simnet and print the arrival-CDF
    comparison (one topology.json feeds both backends)."""
    if remote_host:
        _run_remote(
            remote_host,
            ["compare", "--config", config_path, "--output-dir", output_dir],
            output_dir,
            dry_run,
        )
        return
    cfg = cfgmod.load_config(Path(config_path))
    result = runner.run_comparison(cfg, Path(output_dir))
    _print_comparison(result)


def _print_comparison(result: dict) -> None:
    """Render run_comparison's dict as a side-by-side CDF table."""
    shadow, simnet = result["shadow"], result["simnet"]
    click.echo(f"\nrun dir: {result['run_dir']}")
    click.echo(f"expected arrivals: {result['expected_arrivals']}")
    click.echo(
        f"shadow: {shadow['arrivals']} arrivals "
        f"(missing {shadow['missing']}, duplicates {shadow['duplicates']})"
    )
    click.echo(f"simnet: {simnet['arrivals']} arrivals")
    click.echo("\narrival-delay CDF (ms):")
    _print_cdf_table(shadow["cdf_ms"], simnet["cdf_ms"])
    if result.get("attestations"):
        _print_attestations(result["attestations"])


def _print_cdf_table(shadow_cdf: dict, simnet_cdf: dict) -> None:
    """Side-by-side shadow/simnet percentile table with the simnet delta."""
    click.echo(f"  {'pct':<5}{'shadow':>11}{'simnet':>11}{'Δ%':>9}")
    for p in ("p50", "p90", "p99", "p100"):
        s, m = shadow_cdf[p], simnet_cdf[p]
        delta = (m - s) / s * 100 if s else 0.0
        click.echo(f"  {p:<5}{s:>11.1f}{m:>11.1f}{delta:>8.1f}%")


def _print_attestations(att: dict) -> None:
    """Render the attestation coverage + CDF for both backends."""
    shadow, simnet = att["shadow"], att["simnet"]
    click.echo(f"\nattestations (expected {att['expected']}):")
    for name, r in (("shadow", shadow), ("simnet", simnet)):
        click.echo(
            f"  {name}: {r['arrivals']} arrivals "
            f"(missing {r['missing']}, leaked {r['leaked']}, dup {r['duplicates']}), "
            f"voted-block {r['fraction_voted_block']:.3f}"
        )
    click.echo("  attestation CDF (ms):")
    _print_cdf_table(shadow["cdf_ms"], simnet["cdf_ms"])


if __name__ == "__main__":
    cli()
