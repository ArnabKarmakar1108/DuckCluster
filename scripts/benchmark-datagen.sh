#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/benchmark-datagen.sh <scale_factor> [shard_count] [output_dir]
#
# Generates TPC-H data for benchmarking:
#   - baseline.duckdb (single-node DuckDB with all tables)
#   - workers/worker-*/{table}_shard{N}.duckdb

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCALE_FACTOR="${1:-1}"
SHARD_COUNT="${2:-6}"
OUTPUT_DIR="${3:-$ROOT/benchmark/data/sf${SCALE_FACTOR}}"

PYTHON_DUCKDB="$("$ROOT/scripts/ensure-python-duckdb.sh")"

echo "Generating TPC-H benchmark data..."
echo "  Scale factor: $SCALE_FACTOR"
echo "  Shard count:  $SHARD_COUNT"
echo "  Output dir:   $OUTPUT_DIR"
echo

"$PYTHON_DUCKDB" "$ROOT/scripts/benchmark_datagen.py" \
  "$SCALE_FACTOR" \
  "$SHARD_COUNT" \
  "$OUTPUT_DIR"
