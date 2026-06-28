#!/usr/bin/env bash
set -euo pipefail

# Redistribute staged shards to workers without regenerating TPC-H.
# Usage: ./scripts/benchmark-redistribute.sh <data_dir> [placement]
#   placement: balanced (default) | ring

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${1:?data dir required, e.g. benchmark/data/sf1}"
PLACEMENT="${2:-balanced}"
PYTHON="$("$ROOT/scripts/ensure-python-duckdb.sh")"

"$PYTHON" "$ROOT/scripts/benchmark-redistribute.py" "$ROOT/$DATA_DIR" --placement "$PLACEMENT"
