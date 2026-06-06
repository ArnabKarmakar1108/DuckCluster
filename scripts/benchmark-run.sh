#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/benchmark-run.sh <scale_factor> [options]
#
# Options (environment variables):
#   CONCURRENCY=1,4,8       Client concurrency levels
#   SHARD_COUNT=6           Shards per partitioned table
#   WARMUP=5                Warmup iterations per query
#   ITERATIONS=15           Measured iterations per query
#   SKIP_DATAGEN=1          Skip data generation if output already exists
#   ENGINE=both             duckcluster | duckdb-single | both

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCALE_FACTOR="${1:-1}"
SHARD_COUNT="${SHARD_COUNT:-6}"
CONCURRENCY="${CONCURRENCY:-1}"
WARMUP="${WARMUP:-5}"
ITERATIONS="${ITERATIONS:-15}"
ENGINE="${ENGINE:-both}"
DATA_DIR="$ROOT/benchmark/data/sf${SCALE_FACTOR}"
RESULTS_DIR="$ROOT/benchmark/results"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RUN_ID="sf${SCALE_FACTOR}-${TIMESTAMP}"
OUTPUT_JSON="$RESULTS_DIR/${RUN_ID}.json"

COORDINATOR_HOST="${DUCKCLUSTER_COORDINATOR_HOST:-127.0.0.1}"
COORDINATOR_HTTP_PORT="${DUCKCLUSTER_COORDINATOR_HTTP_PORT:-8080}"
COORDINATOR_GRPC_PORT="${DUCKCLUSTER_COORDINATOR_GRPC_PORT:-9090}"
WORKER_PORTS=(9101 9102 9103)
WORKER_IDS=(worker-1 worker-2 worker-3)

PIDS=()
CLUSTER_STARTED=0

cleanup() {
  if [[ "$CLUSTER_STARTED" -eq 1 ]]; then
    for pid in "${PIDS[@]:-}"; do
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
    done
  fi
}
trap cleanup EXIT INT TERM

mkdir -p "$RESULTS_DIR"

if [[ "${SKIP_DATAGEN:-0}" != "1" || ! -f "$DATA_DIR/baseline.duckdb" ]]; then
  "$ROOT/scripts/benchmark-datagen.sh" "$SCALE_FACTOR" "$SHARD_COUNT" "$DATA_DIR"
else
  echo "Skipping data generation; using cached data in $DATA_DIR"
fi

echo "Building DuckCluster and benchmark harness..."
mvn -q package -DskipTests -pl coordinator,worker,benchmark -am

COORDINATOR_JAR="$ROOT/coordinator/target/duckcluster-coordinator-0.1.0-SNAPSHOT.jar"
WORKER_JAR="$ROOT/worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar"
BENCHMARK_JAR="$ROOT/benchmark/target/duckcluster-benchmark-0.1.0-SNAPSHOT.jar"
QUERIES_DIR="$ROOT/benchmark/src/main/resources/queries"

start_cluster() {
  LOG_DIR="$RESULTS_DIR/logs/$RUN_ID"
  mkdir -p "$LOG_DIR"

  echo "Starting coordinator..."
  java -jar "$COORDINATOR_JAR" >"$LOG_DIR/coordinator.log" 2>&1 &
  PIDS+=($!)
  sleep 2

  for i in "${!WORKER_PORTS[@]}"; do
    port="${WORKER_PORTS[$i]}"
    id="${WORKER_IDS[$i]}"
    worker_data="$DATA_DIR/workers/$id"
    if [[ ! -d "$worker_data" ]]; then
      echo "ERROR: worker data directory not found: $worker_data" >&2
      exit 1
    fi
    echo "Starting $id on port $port (data: $worker_data)..."
    DUCKCLUSTER_DATA_DIR="$worker_data" java -jar "$WORKER_JAR" \
      "$id" "$COORDINATOR_HOST" "$port" >"$LOG_DIR/${id}.log" 2>&1 &
    PIDS+=($!)
  done

  echo "Waiting for cluster health..."
  deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if response=$(curl -sf "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/v1/cluster/health" 2>/dev/null); then
      worker_count=$(curl -sf "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/v1/cluster/workers" \
        | python3 -c "import json,sys; data=json.load(sys.stdin); print(len(data.get('workers', [])))" 2>/dev/null || echo 0)
      status=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('status',''))" "$response" 2>/dev/null || echo "")
      if [[ "$status" == "UP" && "$worker_count" -ge 3 ]]; then
        echo "Cluster ready ($worker_count workers registered)."
        break
      fi
    fi
    sleep 1
  done
  worker_count=$(curl -sf "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/v1/cluster/workers" \
    | python3 -c "import json,sys; data=json.load(sys.stdin); print(len(data.get('workers', [])))" 2>/dev/null || echo 0)
  if [[ "$worker_count" -lt 3 ]]; then
    echo "ERROR: expected 3 registered workers, found $worker_count" >&2
    exit 1
  fi
  CLUSTER_STARTED=1
}

if [[ "$ENGINE" == "both" || "$ENGINE" == "duckcluster" ]]; then
  start_cluster
fi

echo "Running benchmark harness..."
java -jar "$BENCHMARK_JAR" \
  --scale-factor "$SCALE_FACTOR" \
  --concurrency "$CONCURRENCY" \
  --engine "$ENGINE" \
  --coordinator-url "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}" \
  --duckdb-path "$DATA_DIR/baseline.duckdb" \
  --queries-dir "$QUERIES_DIR" \
  --output "$OUTPUT_JSON" \
  --warmup "$WARMUP" \
  --iterations "$ITERATIONS" \
  --run-id "$RUN_ID"

echo
echo "Benchmark complete."
echo "Results: $OUTPUT_JSON"
echo "Document results in docs/BENCHMARK.md as runs complete."
