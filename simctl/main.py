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


if __name__ == "__main__":
    cli()
