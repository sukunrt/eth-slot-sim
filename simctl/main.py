"""CLI entry point for eth-slot-sim block-dissemination Shadow runs."""

from pathlib import Path

import click

from . import config as cfgmod
from . import runner


@click.group()
def cli() -> None:
    """simctl: run block-dissemination simulations on Shadow."""


@cli.command()
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to the run config YAML.",
)
@click.option("--output-dir", default="./runs", help="Parent directory for the run output.")
@click.option("--dry-run", is_flag=True, help="Generate the run dir + shadow.yaml without building or running shadow.")
def run(config_path: str, output_dir: str, dry_run: bool) -> None:
    """Run one block-dissemination simulation on Shadow."""
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
def compare(config_path: str, output_dir: str) -> None:
    """Run the same topology on both Shadow and simnet and print the arrival-CDF
    comparison (one topology.json feeds both backends)."""
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
    click.echo(f"  {'pct':<5}{'shadow':>11}{'simnet':>11}{'Δ%':>9}")
    for p in ("p50", "p90", "p99", "p100"):
        s, m = shadow["cdf_ms"][p], simnet["cdf_ms"][p]
        delta = (m - s) / s * 100 if s else 0.0
        click.echo(f"  {p:<5}{s:>11.1f}{m:>11.1f}{delta:>8.1f}%")


if __name__ == "__main__":
    cli()
