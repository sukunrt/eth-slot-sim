#!/usr/bin/env bash
# Install the DuckDB CLI for interactive queries against a run's parquet event tables
# (e.g. duckdb -c "SELECT ... FROM 'runs/x/run-*/parquet/arrivals.parquet'").
#
# NOTE: the analysis pipeline does NOT need this — the Python duckdb package is a
# pyproject dependency and rides `uv sync` everywhere (locally and on the remote).
# This script only fetches the standalone CLI binary, via DuckDB's official installer,
# and links it into ~/.local/bin.
set -euo pipefail

if command -v duckdb >/dev/null 2>&1; then
    echo "duckdb already installed: $(command -v duckdb) ($(duckdb --version))"
    exit 0
fi

curl -fsSL https://install.duckdb.org | sh

# The official installer lands in ~/.duckdb/cli/latest; make it reachable without a
# shell-profile edit if ~/.local/bin is already on PATH.
mkdir -p "$HOME/.local/bin"
ln -sf "$HOME/.duckdb/cli/latest/duckdb" "$HOME/.local/bin/duckdb"
echo "duckdb CLI linked at ~/.local/bin/duckdb"
