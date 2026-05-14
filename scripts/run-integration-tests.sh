#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS="$ROOT/tests/integration"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "Note: duckdb CLI not found; harness will generate shards via Python duckdb package"
fi

if [[ ! -d "$HARNESS/.venv" ]]; then
  python3 -m venv "$HARNESS/.venv"
  "$HARNESS/.venv/bin/pip" install -r "$HARNESS/requirements.txt"
fi

"$HARNESS/.venv/bin/pytest" -v "$HARNESS" "$@"
# Examples:
#   ./scripts/run-integration-tests.sh
#   ./scripts/run-integration-tests.sh --demo-rows 1000
#   ./scripts/run-integration-tests.sh --demo-csv tests/integration/data/demo-events.csv
