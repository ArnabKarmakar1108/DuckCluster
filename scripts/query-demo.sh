#!/usr/bin/env bash
set -euo pipefail

HOST="${DUCKCLUSTER_COORDINATOR_HOST:-127.0.0.1}"
PORT="${DUCKCLUSTER_COORDINATOR_HTTP_PORT:-8080}"

echo "Running demo query against http://${HOST}:${PORT}/v1/query"
curl -s -X POST "http://${HOST}:${PORT}/v1/query" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM events"}' | python3 -m json.tool
