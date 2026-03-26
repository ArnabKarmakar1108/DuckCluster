#!/usr/bin/env bash
set -euo pipefail

# split-and-distribute.sh
# Splits a source data file into N DuckDB shard files and distributes them
# to worker data directories based on consistent hash ring assignment.
#
# Usage:
#   ./split-and-distribute.sh \
#       --source <path>          Source data file (Parquet or CSV)
#       --table <name>           Table name for the shards
#       --key <column>           Shard key column (for hash partitioning)
#       --shards <N>             Number of shards to create
#       --workers <id1,id2,...>  Comma-separated worker IDs
#       --dirs <dir1,dir2,...>   Comma-separated worker data directories (same order as --workers)
#       [--rf <N>]              Replication factor (default: 2)
#       [--vnodes <N>]          Virtual nodes per worker (default: 100)
#       [--format csv|parquet]  Source file format (auto-detected from extension if omitted)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SOURCE=""
TABLE=""
KEY=""
SHARDS=""
WORKERS=""
DIRS=""
RF=2
VNODES=100
FORMAT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)  SOURCE="$2"; shift 2 ;;
    --table)   TABLE="$2"; shift 2 ;;
    --key)     KEY="$2"; shift 2 ;;
    --shards)  SHARDS="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --dirs)    DIRS="$2"; shift 2 ;;
    --rf)      RF="$2"; shift 2 ;;
    --vnodes)  VNODES="$2"; shift 2 ;;
    --format)  FORMAT="$2"; shift 2 ;;
    *)         echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$SOURCE" || -z "$TABLE" || -z "$KEY" || -z "$SHARDS" || -z "$WORKERS" || -z "$DIRS" ]]; then
  echo "Usage: $0 --source <file> --table <name> --key <col> --shards <N> --workers <ids> --dirs <dirs>" >&2
  exit 1
fi

if [[ -z "$FORMAT" ]]; then
  case "$SOURCE" in
    *.parquet) FORMAT="parquet" ;;
    *.csv)     FORMAT="csv" ;;
    *)         echo "Cannot detect format from extension. Use --format csv|parquet" >&2; exit 1 ;;
  esac
fi

if ! command -v duckdb &>/dev/null; then
  echo "Error: duckdb CLI not found in PATH" >&2
  exit 1
fi

COMMON_CLASSES="$ROOT/common/target/classes"
if [[ ! -d "$COMMON_CLASSES" ]]; then
  echo "Building common module for ring assignment..."
  mvn -q -f "$ROOT/common/pom.xml" compile -DskipTests
fi

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "=== Split & Distribute ==="
echo "  Source:  $SOURCE ($FORMAT)"
echo "  Table:   $TABLE"
echo "  Key:     $KEY"
echo "  Shards:  $SHARDS"
echo "  Workers: $WORKERS"
echo "  RF:      $RF"
echo ""

# Step 1: Get ring assignments
echo "Step 1: Computing ring assignments..."
ASSIGNMENTS=$(java -cp "$COMMON_CLASSES" io.duckcluster.common.routing.RingAssignerCli \
    --workers "$WORKERS" --table "$TABLE" --shards "$SHARDS" --rf "$RF" --vnodes "$VNODES")
echo "$ASSIGNMENTS" > "$TMPDIR/assignments.json"
echo "  Ring assignments computed."

# Step 2: Split source data into shard files
echo "Step 2: Splitting source data into $SHARDS shards..."

READ_EXPR=""
case "$FORMAT" in
  parquet) READ_EXPR="read_parquet('$SOURCE')" ;;
  csv)     READ_EXPR="read_csv_auto('$SOURCE')" ;;
esac

for i in $(seq 0 $((SHARDS - 1))); do
  SHARD_FILE="$TMPDIR/${TABLE}_shard${i}.duckdb"
  duckdb "$SHARD_FILE" <<-SQL
    CREATE TABLE ${TABLE} AS
    SELECT * FROM ${READ_EXPR}
    WHERE abs(hash(${KEY})) % ${SHARDS} = ${i};
    CHECKPOINT;
SQL
  ROW_COUNT=$(duckdb "$SHARD_FILE" -noheader -csv "SELECT count(*) FROM ${TABLE};")
  echo "  ${TABLE}_shard${i}.duckdb: $ROW_COUNT rows"
done

# Step 3: Distribute shard files to worker directories
echo "Step 3: Distributing shards to worker directories..."

IFS=',' read -ra WORKER_ARRAY <<< "$WORKERS"
IFS=',' read -ra DIR_ARRAY <<< "$DIRS"

if [[ ${#WORKER_ARRAY[@]} -ne ${#DIR_ARRAY[@]} ]]; then
  echo "Error: --workers and --dirs must have the same number of entries" >&2
  exit 1
fi

declare -A WORKER_DIR_MAP
for idx in "${!WORKER_ARRAY[@]}"; do
  WORKER_DIR_MAP["${WORKER_ARRAY[$idx]}"]="${DIR_ARRAY[$idx]}"
done

MANIFEST="{"
for i in $(seq 0 $((SHARDS - 1))); do
  SHARD_NAME="${TABLE}_shard${i}"
  SHARD_FILE="$TMPDIR/${SHARD_NAME}.duckdb"

  # Parse owners from JSON using grep/sed (avoid jq dependency)
  OWNERS=$(echo "$ASSIGNMENTS" | grep "\"${SHARD_NAME}\"" | \
    sed 's/.*\[//;s/\].*//;s/"//g;s/ //g')

  IFS=',' read -ra OWNER_ARRAY <<< "$OWNERS"
  for owner in "${OWNER_ARRAY[@]}"; do
    TARGET_DIR="${WORKER_DIR_MAP[$owner]:-}"
    if [[ -z "$TARGET_DIR" ]]; then
      echo "  WARNING: No directory mapping for worker '$owner', skipping" >&2
      continue
    fi
    mkdir -p "$TARGET_DIR"
    cp "$SHARD_FILE" "$TARGET_DIR/${SHARD_NAME}.duckdb"
    echo "  $SHARD_NAME -> $owner ($TARGET_DIR)"
  done

  if [[ $i -gt 0 ]]; then MANIFEST+=","; fi
  MANIFEST+="\"${SHARD_NAME}\":[$(echo "$ASSIGNMENTS" | grep "\"${SHARD_NAME}\"" | sed 's/.*\[//;s/\].*//' )]"
done
MANIFEST+="}"

# Step 4: Write manifest
MANIFEST_FILE="$ROOT/data/manifest.json"
mkdir -p "$(dirname "$MANIFEST_FILE")"
echo "$MANIFEST" | python3 -m json.tool > "$MANIFEST_FILE"
echo ""
echo "Manifest written to: $MANIFEST_FILE"
echo ""
echo "=== Done ==="
