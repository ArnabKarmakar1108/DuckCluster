#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PID_FILE="$ROOT/data/cluster.pids"
SKIP_BUILD=false
ACTION=start

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Start or stop the local DuckCluster demo (coordinator + 3 workers).

Options:
  --no-build, -n   Skip mvn package (use existing JARs)
  --stop, -s       Stop a running cluster
  --help, -h       Show this help

Examples:
  $(basename "$0")              # build and start
  $(basename "$0") --no-build   # start without rebuilding
  $(basename "$0") --stop       # stop cluster started by this script
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build|-n) SKIP_BUILD=true; shift ;;
    --stop|-s) ACTION=stop; shift ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

export DUCKCLUSTER_COORDINATOR_HOST="${DUCKCLUSTER_COORDINATOR_HOST:-127.0.0.1}"
export DUCKCLUSTER_COORDINATOR_HTTP_BIND_HOST="${DUCKCLUSTER_COORDINATOR_HTTP_BIND_HOST:-0.0.0.0}"
export DUCKCLUSTER_COORDINATOR_GRPC_PORT="${DUCKCLUSTER_COORDINATOR_GRPC_PORT:-9090}"
export DUCKCLUSTER_COORDINATOR_HTTP_PORT="${DUCKCLUSTER_COORDINATOR_HTTP_PORT:-8080}"
export DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC="${DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC:-5}"
export DUCKCLUSTER_SHARD_COUNT="${DUCKCLUSTER_SHARD_COUNT:-6}"
export DUCKCLUSTER_REPLICATION_FACTOR="${DUCKCLUSTER_REPLICATION_FACTOR:-2}"

WORKER_PORTS=(9101 9102 9103)
WORKER_IDS=(worker-1 worker-2 worker-3)
DATA_BASE="$ROOT/data"

COORDINATOR_JAR="$ROOT/coordinator/target/duckcluster-coordinator-0.1.0-SNAPSHOT.jar"
WORKER_JAR="$ROOT/worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar"

stop_cluster() {
  local pid
  local stopped=false

  if [[ -f "$PID_FILE" ]]; then
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        stopped=true
      fi
    done < "$PID_FILE"
  fi

  if $stopped; then
    sleep 2
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      kill -9 "$pid" 2>/dev/null || true
    done < "$PID_FILE"
  fi

  pkill -9 -f "duckcluster-coordinator-0.1.0-SNAPSHOT.jar" 2>/dev/null || true
  pkill -9 -f "duckcluster-worker-0.1.0-SNAPSHOT.jar" 2>/dev/null || true
  rm -f "$PID_FILE"
}

if [[ "$ACTION" == "stop" ]]; then
  echo "Stopping DuckCluster..."
  stop_cluster
  echo "Cluster stopped."
  exit 0
fi

PIDS=()

cleanup() {
  trap - INT TERM EXIT
  echo
  echo "Stopping cluster..."
  stop_cluster
  exit 0
}

if [[ "$SKIP_BUILD" == "true" ]]; then
  echo "Skipping build (--no-build)."
else
  echo "Building DuckCluster..."
  mvn -q clean package -DskipTests
fi

if [[ ! -f "$COORDINATOR_JAR" || ! -f "$WORKER_JAR" ]]; then
  echo "ERROR: JARs not found. Run without --no-build first." >&2
  exit 1
fi

stop_cluster

# Prepare demo data as shard files (replaces WorkerDemoDataLoader)
DEMO_SOURCE="${DUCKCLUSTER_DEMO_CSV:-$ROOT/tests/integration/data/demo-events.csv}"
if [[ ! -f "$DEMO_SOURCE" ]]; then
  echo "ERROR: Demo CSV not found at $DEMO_SOURCE"
  echo "  Set DUCKCLUSTER_DEMO_CSV or add tests/integration/data/demo-events.csv"
  exit 1
fi

# Set up per-worker data directories
WORKER_DIRS=""
for id in "${WORKER_IDS[@]}"; do
  DIR="$DATA_BASE/$id"
  mkdir -p "$DIR"
  if [[ -n "$WORKER_DIRS" ]]; then WORKER_DIRS+=","; fi
  WORKER_DIRS+="$DIR"
done

# Split and distribute demo data to worker directories
echo "Splitting demo data into shards..."
if command -v duckdb &>/dev/null; then
  "$ROOT/scripts/split-and-distribute.sh" \
      --source "$DEMO_SOURCE" \
      --table events \
      --key id \
      --shards "$DUCKCLUSTER_SHARD_COUNT" \
      --workers "$(IFS=,; echo "${WORKER_IDS[*]}")" \
      --dirs "$WORKER_DIRS" \
      --rf "$DUCKCLUSTER_REPLICATION_FACTOR"
else
  PYTHON_DUCKDB="$("$ROOT/scripts/ensure-python-duckdb.sh")"
  echo "Note: duckdb CLI not found; using Python duckdb package"
  "$PYTHON_DUCKDB" "$ROOT/scripts/prepare-demo-shards.py" \
      --source "$DEMO_SOURCE" \
      --table events \
      --key id \
      --shards "$DUCKCLUSTER_SHARD_COUNT" \
      --workers "$(IFS=,; echo "${WORKER_IDS[*]}")" \
      --dirs "$WORKER_DIRS" \
      --rf "$DUCKCLUSTER_REPLICATION_FACTOR"
fi

mkdir -p "$DATA_BASE"
: > "$PID_FILE"

echo "Starting coordinator..."
java -jar "$COORDINATOR_JAR" &
PIDS+=($!)
echo "${PIDS[-1]}" >> "$PID_FILE"
sleep 2

for i in "${!WORKER_PORTS[@]}"; do
  port="${WORKER_PORTS[$i]}"
  id="${WORKER_IDS[$i]}"
  echo "Starting $id on port $port..."
  DUCKCLUSTER_DATA_DIR="$DATA_BASE/$id" java -jar "$WORKER_JAR" "$id" "$DUCKCLUSTER_COORDINATOR_HOST" "$port" &
  PIDS+=($!)
  echo "${PIDS[-1]}" >> "$PID_FILE"
done

sleep 3

echo
echo "Cluster health:"
curl -s "http://${DUCKCLUSTER_COORDINATOR_HOST}:${DUCKCLUSTER_COORDINATOR_HTTP_PORT}/v1/cluster/health" | python3 -m json.tool

echo
echo "Registered workers:"
curl -s "http://${DUCKCLUSTER_COORDINATOR_HOST}:${DUCKCLUSTER_COORDINATOR_HTTP_PORT}/v1/cluster/workers" | python3 -m json.tool

echo
echo "Cluster is running."
echo "  Dashboard: http://<this-host>:${DUCKCLUSTER_COORDINATOR_HTTP_PORT}/dashboard/index.html"
echo "  Stop:      $0 --stop   (or Ctrl+C)"
echo

trap cleanup INT TERM

while true; do
  alive=false
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      alive=true
      break
    fi
  done
  if ! $alive; then
    echo "All cluster processes exited."
    rm -f "$PID_FILE"
    exit 0
  fi
  sleep 1
done
