#!/usr/bin/env bash
# Ensures tests/integration/.venv exists with duckdb installed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS="$ROOT/tests/integration"

if [[ ! -d "$HARNESS/.venv" ]]; then
  python3 -m venv "$HARNESS/.venv"
  "$HARNESS/.venv/bin/pip" install -r "$HARNESS/requirements.txt"
fi

echo "$HARNESS/.venv/bin/python"
