#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export DUCKCLUSTER_COORDINATOR_HOST="${DUCKCLUSTER_COORDINATOR_HOST:-127.0.0.1}"
export DUCKCLUSTER_COORDINATOR_GRPC_PORT="${DUCKCLUSTER_COORDINATOR_GRPC_PORT:-9090}"
export DUCKCLUSTER_COORDINATOR_HTTP_PORT="${DUCKCLUSTER_COORDINATOR_HTTP_PORT:-8080}"
export DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC="${DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC:-5}"

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

echo "Building DuckCluster..."
mvn -q clean package -DskipTests

COORDINATOR_JAR="$ROOT/coordinator/target/duckcluster-coordinator-0.1.0-SNAPSHOT.jar"
WORKER_JAR="$ROOT/worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar"

echo "Starting coordinator..."
java -jar "$COORDINATOR_JAR" &
PIDS+=($!)
sleep 2

WORKER_PORTS=(9101 9102 9103)
WORKER_IDS=(worker-1 worker-2 worker-3)

for i in "${!WORKER_PORTS[@]}"; do
  port="${WORKER_PORTS[$i]}"
  id="${WORKER_IDS[$i]}"
  echo "Starting $id on port $port..."
  java -jar "$WORKER_JAR" "$id" "$DUCKCLUSTER_COORDINATOR_HOST" "$port" &
  PIDS+=($!)
done

sleep 3

echo
echo "Cluster health:"
curl -s "http://${DUCKCLUSTER_COORDINATOR_HOST}:${DUCKCLUSTER_COORDINATOR_HTTP_PORT}/v1/cluster/health" | python3 -m json.tool

echo
echo "Registered workers:"
curl -s "http://${DUCKCLUSTER_COORDINATOR_HOST}:${DUCKCLUSTER_COORDINATOR_HTTP_PORT}/v1/cluster/workers" | python3 -m json.tool

echo
echo "Cluster is running. Press Ctrl+C to stop."
wait
