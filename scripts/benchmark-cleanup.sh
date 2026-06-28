#!/usr/bin/env bash
set -euo pipefail

# Free disk before generating a larger scale factor.
#
# Usage:
#   ./scripts/benchmark-cleanup.sh                    # remove sf0.01 + sf1, keep sf10
#   ./scripts/benchmark-cleanup.sh --keep sf10        # same as default
#   ./scripts/benchmark-cleanup.sh --keep sf10,sf50   # keep listed dirs under benchmark/data
#   ./scripts/benchmark-cleanup.sh --logs             # also prune old result logs (keep latest per SF)
#   ./scripts/benchmark-cleanup.sh --all              # remove all benchmark/data + results/logs
#
# Environment:
#   DRY_RUN=1   Print removals without deleting

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="$ROOT/benchmark/data"
RESULTS_ROOT="$ROOT/benchmark/results"
LOGS_ROOT="$RESULTS_ROOT/logs"

KEEP="${KEEP:-sf10}"
REMOVE_LOGS=0
REMOVE_ALL=0
DRY_RUN="${DRY_RUN:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep)
      KEEP="${2:?missing value for --keep}"
      shift 2
      ;;
    --logs)
      REMOVE_LOGS=1
      shift
      ;;
    --all)
      REMOVE_ALL=1
      shift
      ;;
    -n|--dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      sed -n '3,14p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "ERROR: unknown option: $1" >&2
      exit 1
      ;;
  esac
done

remove_path() {
  local path="$1"
  if [[ ! -e "$path" ]]; then
    return
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "would remove: $path ($(du -sh "$path" 2>/dev/null | cut -f1))"
  else
    echo "removing: $path ($(du -sh "$path" 2>/dev/null | cut -f1))"
    rm -rf "$path"
  fi
}

if [[ "$REMOVE_ALL" == "1" ]]; then
  remove_path "$DATA_ROOT"
  remove_path "$LOGS_ROOT"
  remove_path "$RESULTS_ROOT"/*.json
  echo "Done."
  exit 0
fi

IFS=',' read -r -a keep_list <<< "$KEEP"
should_keep() {
  local name="$1"
  for k in "${keep_list[@]}"; do
    if [[ "$name" == "$k" ]]; then
      return 0
    fi
  done
  return 1
}

if [[ -d "$DATA_ROOT" ]]; then
  for dir in "$DATA_ROOT"/sf*; do
    [[ -d "$dir" ]] || continue
    base="$(basename "$dir")"
    if should_keep "$base"; then
      echo "keeping data: $dir"
      # Drop staging inside kept SF to reclaim space after redistribute
      if [[ -d "$dir/staging" ]]; then
        remove_path "$dir/staging"
      fi
      continue
    fi
    remove_path "$dir"
  done
fi

if [[ "$REMOVE_LOGS" == "1" && -d "$LOGS_ROOT" ]]; then
  echo "Pruning old benchmark logs..."
  for dir in "$LOGS_ROOT"/sf*; do
    [[ -d "$dir" ]] || continue
    base="$(basename "$dir")"
    # Keep only the newest log dir per scale prefix (sf1-, sf10-, etc.)
    prefix="${base%%-*}"
    newest="$(ls -1dt "$LOGS_ROOT"/${prefix}-* 2>/dev/null | head -1 || true)"
    if [[ -n "$newest" && "$dir" != "$newest" ]]; then
      remove_path "$dir"
    fi
  done
fi

echo "Disk after cleanup:"
du -sh "$DATA_ROOT" "$RESULTS_ROOT" 2>/dev/null || true
