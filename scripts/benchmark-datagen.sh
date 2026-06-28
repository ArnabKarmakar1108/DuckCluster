#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/benchmark-datagen.sh <scale_factor> [shard_count] [output_dir] [options]
#
# Options:
#   --replace         Remove output_dir before generating
#   --clean-other-sf  Remove other benchmark/data/sf* dirs (keeps output_dir only)
#
# Generates TPC-H data for benchmarking:
#   - baseline.duckdb (single-node DuckDB with all tables)
#   - workers/worker-*/{table}_shard{N}.duckdb
#
# Before generating a larger SF, free disk with:
#   ./scripts/benchmark-cleanup.sh --keep sf10 --logs

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCALE_FACTOR=""
SHARD_COUNT="6"
OUTPUT_DIR=""
REPLACE=0
CLEAN_OTHER_SF=0

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --replace)
      REPLACE=1
      shift
      ;;
    --clean-other-sf)
      CLEAN_OTHER_SF=1
      shift
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

SCALE_FACTOR="${POSITIONAL[0]:-1}"
SHARD_COUNT="${POSITIONAL[1]:-$SHARD_COUNT}"
OUTPUT_DIR="${POSITIONAL[2]:-$ROOT/benchmark/data/sf${SCALE_FACTOR}}"

if [[ "$CLEAN_OTHER_SF" == "1" ]]; then
  target_base="$(basename "$OUTPUT_DIR")"
  "$ROOT/scripts/benchmark-cleanup.sh" --keep "$target_base" --logs
fi

if [[ "$REPLACE" == "1" && -d "$OUTPUT_DIR" ]]; then
  echo "Removing existing output dir: $OUTPUT_DIR"
  rm -rf "$OUTPUT_DIR"
fi

PYTHON_DUCKDB="$("$ROOT/scripts/ensure-python-duckdb.sh")"

echo "Generating TPC-H benchmark data..."
echo "  Scale factor: $SCALE_FACTOR"
echo "  Shard count:  $SHARD_COUNT"
echo "  Output dir:   $OUTPUT_DIR"
echo

"$PYTHON_DUCKDB" "$ROOT/scripts/benchmark_datagen.py" \
  "$SCALE_FACTOR" \
  "$SHARD_COUNT" \
  "$OUTPUT_DIR" \
  ${PLACEMENT:+--placement "$PLACEMENT"}
