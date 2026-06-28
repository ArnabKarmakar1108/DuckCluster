#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/benchmark-run.sh <scale_factor> [options]
#   ./scripts/benchmark-run.sh --sf <scale_factor> [--concurrency N] [options]
#
# Options (flags override environment variables):
#   --sf, --scale-factor N
#   --concurrency N       Comma-separated levels (default: 1)
#   --warmup N              Warmup iterations (default: 5)
#   --iterations N          Measured iterations (default: 15)
#   --engine ENGINE         duckcluster | duckdb-single | both (default: both)
#   --shard-count N         Only used if datagen runs (default: 6)
#   --skip-datagen          Never run TPC-H generation
#   --skip-build            Skip mvn package
#   --verbose               Print per-query progress from harness
#
# Environment variables (same names, used when flags omitted):
#   CONCURRENCY, WARMUP, ITERATIONS, ENGINE, SHARD_COUNT, SKIP_DATAGEN, SKIP_BUILD, VERBOSE

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCALE_FACTOR="${SCALE_FACTOR:-1}"
SHARD_COUNT="${SHARD_COUNT:-6}"
CONCURRENCY="${CONCURRENCY:-1}"
WARMUP="${WARMUP:-5}"
ITERATIONS="${ITERATIONS:-15}"
ENGINE="${ENGINE:-both}"
VERBOSE="${VERBOSE:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_DATAGEN="${SKIP_DATAGEN:-0}"

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sf|--scale-factor)
      SCALE_FACTOR="${2:?missing value for $1}"
      shift 2
      ;;
    --concurrency)
      CONCURRENCY="${2:?missing value for $1}"
      shift 2
      ;;
    --warmup)
      WARMUP="${2:?missing value for $1}"
      shift 2
      ;;
    --iterations)
      ITERATIONS="${2:?missing value for $1}"
      shift 2
      ;;
    --engine)
      ENGINE="${2:?missing value for $1}"
      shift 2
      ;;
    --shard-count)
      SHARD_COUNT="${2:?missing value for $1}"
      shift 2
      ;;
    --skip-datagen)
      SKIP_DATAGEN=1
      shift
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --verbose)
      VERBOSE=1
      shift
      ;;
    -h|--help)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    -*)
      echo "ERROR: unknown option: $1 (try --help)" >&2
      exit 1
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [[ ${#POSITIONAL[@]} -gt 0 ]]; then
  SCALE_FACTOR="${POSITIONAL[0]}"
fi
if [[ ${#POSITIONAL[@]} -gt 1 ]]; then
  echo "ERROR: unexpected positional arguments: ${POSITIONAL[*]:1}" >&2
  exit 1
fi

DATA_DIR="$ROOT/benchmark/data/sf${SCALE_FACTOR}"
RESULTS_DIR="$ROOT/benchmark/results"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RUN_ID="sf${SCALE_FACTOR}-${TIMESTAMP}"
OUTPUT_JSON="$RESULTS_DIR/${RUN_ID}.json"

COORDINATOR_HOST="${DUCKCLUSTER_COORDINATOR_HOST:-127.0.0.1}"
COORDINATOR_HTTP_PORT="${DUCKCLUSTER_COORDINATOR_HTTP_PORT:-8080}"
WORKER_PORTS=(9101 9102 9103)
WORKER_IDS=(worker-1 worker-2 worker-3)

PIDS=()

cluster_worker_count() {
  curl -sf "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/v1/cluster/workers" 2>/dev/null \
    | python3 -c "import json,sys; data=json.load(sys.stdin); print(len(data.get('workers', [])))" 2>/dev/null \
    || echo 0
}

cluster_health_status() {
  curl -sf "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/v1/cluster/health" 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))" 2>/dev/null \
    || echo ""
}

has_tpch_catalog() {
  local tables
  tables=$(curl -sf "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/v1/monitor/shards" 2>/dev/null) || return 1
  printf '%s' "$tables" | python3 -c "import json,sys; data=json.load(sys.stdin); names={t['tableName'] for t in data.get('tables', [])}; sys.exit(0 if 'lineitem' in names else 1)" 2>/dev/null
}

require_process_alive() {
  local label="$1"
  local pid="$2"
  local log_file="${3:-}"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "ERROR: $label failed to start (pid=$pid exited)." >&2
    if [[ -n "$log_file" && -f "$log_file" ]]; then
      echo "Last lines of $log_file:" >&2
      tail -20 "$log_file" >&2 || true
    fi
    exit 1
  fi
  if [[ -n "$log_file" && -f "$log_file" ]] && grep -q "Address already in use" "$log_file"; then
    echo "ERROR: $label could not bind (port in use). Stop other clusters first:" >&2
    echo "  $ROOT/scripts/start-cluster.sh --stop" >&2
    tail -10 "$log_file" >&2 || true
    exit 1
  fi
}

wait_for_tpch_catalog() {
  echo "Waiting for TPC-H tables in coordinator catalog..."
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if has_tpch_catalog; then
      echo "TPC-H catalog ready."
      return 0
    fi
    sleep 1
  done
  echo "ERROR: TPC-H table 'lineitem' not registered after 90s." >&2
  echo "  Workers may still be registering shards. Check cluster logs." >&2
  exit 1
}

wait_for_cluster_health() {
  echo "Waiting for cluster health..."
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    local status worker_count
    status=$(cluster_health_status)
    worker_count=$(cluster_worker_count)
    if [[ "$status" == "UP" && "$worker_count" -ge 3 ]]; then
      echo "Cluster ready ($worker_count workers)."
      return 0
    fi
    sleep 1
  done
  local worker_count
  worker_count=$(cluster_worker_count)
  echo "ERROR: expected 3 registered workers, found $worker_count" >&2
  exit 1
}

mkdir -p "$RESULTS_DIR"

# Datagen is a separate step — only run when baseline is missing unless forced.
if [[ -f "$DATA_DIR/baseline.duckdb" ]]; then
  echo "Using existing data: $DATA_DIR"
elif [[ "$SKIP_DATAGEN" == "1" ]]; then
  echo "ERROR: baseline missing at $DATA_DIR/baseline.duckdb and SKIP_DATAGEN=1" >&2
  exit 1
else
  echo "Generating TPC-H data (one-time)..."
  "$ROOT/scripts/benchmark-datagen.sh" "$SCALE_FACTOR" "$SHARD_COUNT" "$DATA_DIR"
fi

if [[ "$SKIP_BUILD" == "1" ]]; then
  echo "Skipping build (SKIP_BUILD=1)"
else
  echo "Building coordinator, worker, benchmark..."
  mvn -q package -DskipTests -pl coordinator,worker,benchmark -am
fi

COORDINATOR_JAR="$ROOT/coordinator/target/duckcluster-coordinator-0.1.0-SNAPSHOT.jar"
WORKER_JAR="$ROOT/worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar"
BENCHMARK_JAR="$ROOT/benchmark/target/duckcluster-benchmark-0.1.0-SNAPSHOT.jar"
QUERIES_DIR="$ROOT/benchmark/src/main/resources/queries"

start_benchmark_cluster() {
  LOG_DIR="$RESULTS_DIR/logs/$RUN_ID"
  mkdir -p "$LOG_DIR"

  echo "Starting benchmark cluster (logs: $LOG_DIR/)..."
  DUCKCLUSTER_LOG_LEVEL="${DUCKCLUSTER_LOG_LEVEL:-INFO}" \
    java -jar "$COORDINATOR_JAR" >"$LOG_DIR/coordinator.log" 2>&1 &
  PIDS+=($!)
  sleep 2
  require_process_alive "coordinator" "${PIDS[-1]}" "$LOG_DIR/coordinator.log"

  for i in "${!WORKER_PORTS[@]}"; do
    port="${WORKER_PORTS[$i]}"
    id="${WORKER_IDS[$i]}"
    worker_data="$DATA_DIR/workers/$id"
    if [[ ! -d "$worker_data" ]]; then
      echo "ERROR: worker data directory not found: $worker_data" >&2
      exit 1
    fi
    echo "Starting $id on port $port..."
    DUCKCLUSTER_DATA_DIR="$worker_data" \
      DUCKCLUSTER_CACHE_MAX_SHARDS="${DUCKCLUSTER_CACHE_MAX_SHARDS:-64}" \
      DUCKCLUSTER_LOG_LEVEL="${DUCKCLUSTER_LOG_LEVEL:-INFO}" \
      java -jar "$WORKER_JAR" \
      "$id" "$COORDINATOR_HOST" "$port" >"$LOG_DIR/${id}.log" 2>&1 &
    PIDS+=($!)
    sleep 1
    require_process_alive "$id" "${PIDS[-1]}" "$LOG_DIR/${id}.log"
  done

  wait_for_cluster_health
  wait_for_tpch_catalog
}

ensure_benchmark_cluster() {
  local status worker_count
  status=$(cluster_health_status)
  worker_count=$(cluster_worker_count)

  if [[ -n "$status" && "$worker_count" -ge 3 ]]; then
    if has_tpch_catalog; then
      echo "Using existing cluster ($worker_count workers, TPC-H catalog)."
      return 0
    fi
    echo "ERROR: A cluster is running but does not have TPC-H benchmark data." >&2
    echo "  Stop it with: $ROOT/scripts/start-cluster.sh --stop" >&2
    echo "  Then re-run this benchmark (it will start a TPC-H cluster)." >&2
    exit 1
  fi

  if [[ -n "$status" ]]; then
    echo "ERROR: Coordinator is reachable but cluster is not ready ($worker_count workers, status=${status:-unknown})." >&2
    echo "  Stop any partial cluster: $ROOT/scripts/start-cluster.sh --stop" >&2
    exit 1
  fi

  start_benchmark_cluster
  echo "Benchmark cluster left running after this script exits."
}

echo ""
echo "=== Benchmark execution (query time only; datagen excluded) ==="
echo "  run_id:      $RUN_ID"
echo "  scale:       SF$SCALE_FACTOR"
echo "  data:        $DATA_DIR"
echo "  engine:      $ENGINE"
echo "  concurrency: $CONCURRENCY"
echo "  warmup:      $WARMUP (not measured)"
echo "  iterations:  $ITERATIONS"
echo "  output:      $OUTPUT_JSON"
echo ""

if [[ "$ENGINE" == "both" || "$ENGINE" == "duckcluster" ]]; then
  ensure_benchmark_cluster
fi

HARNESS_ARGS=(
  --scale-factor "$SCALE_FACTOR"
  --concurrency "$CONCURRENCY"
  --engine "$ENGINE"
  --coordinator-url "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}"
  --duckdb-path "$DATA_DIR/baseline.duckdb"
  --queries-dir "$QUERIES_DIR"
  --output "$OUTPUT_JSON"
  --warmup "$WARMUP"
  --iterations "$ITERATIONS"
  --run-id "$RUN_ID"
)
if [[ "$VERBOSE" == "1" ]]; then
  HARNESS_ARGS+=(--verbose)
fi

echo "Running harness..."
java -jar "$BENCHMARK_JAR" "${HARNESS_ARGS[@]}"

echo ""
echo "Done. Results: $OUTPUT_JSON"
echo "Primary metric: execution_ms (coordinator stats.durationMs for DuckCluster)."
echo "Cluster was not stopped — reuse it for the next run or stop with: $ROOT/scripts/start-cluster.sh --stop"
