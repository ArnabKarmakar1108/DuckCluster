#!/usr/bin/env bash
set -euo pipefail

ROOT=/app
SOURCE="${DUCKCLUSTER_DEMO_CSV:-$ROOT/tests/integration/data/demo-events.csv}"

echo "Preparing demo shard files..."
python3 "$ROOT/scripts/prepare-demo-shards.py" \
  --root "$ROOT" \
  --source "$SOURCE" \
  --table events \
  --key id \
  --shards "${DUCKCLUSTER_SHARD_COUNT:-6}" \
  --workers worker-1,worker-2,worker-3 \
  --dirs /shards/worker-1,/shards/worker-2,/shards/worker-3 \
  --rf "${DUCKCLUSTER_REPLICATION_FACTOR:-2}"
