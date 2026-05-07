#!/usr/bin/env bash
set -euo pipefail
pkill -f 'duckcluster-coordinator-0.1.0' 2>/dev/null || true
pkill -f 'duckcluster-worker-0.1.0' 2>/dev/null || true
sleep 1

export DUCKCLUSTER_COORDINATOR_HTTP_PORT=18080
export DUCKCLUSTER_COORDINATOR_GRPC_PORT=9090
export DUCKCLUSTER_SHARD_COUNT=3

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
java -jar "$ROOT/coordinator/target/duckcluster-coordinator-0.1.0-SNAPSHOT.jar" &
COORD_PID=$!
sleep 2

java -jar "$ROOT/worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar" worker-1 127.0.0.1 9101 &
java -jar "$ROOT/worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar" worker-2 127.0.0.1 9102 &
java -jar "$ROOT/worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar" worker-3 127.0.0.1 9103 &
sleep 5

echo "GROUP BY:"
curl -s -X POST "http://127.0.0.1:18080/v1/query" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT category, COUNT(*) AS cnt FROM events GROUP BY category"}'
echo
echo "PARTIAL AGG:"
curl -s -X POST "http://127.0.0.1:18080/v1/query" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT COUNT(*) AS row_count, SUM(id) AS total FROM events"}'
echo

kill "$COORD_PID" 2>/dev/null || true
pkill -f 'duckcluster-worker-0.1.0' 2>/dev/null || true
