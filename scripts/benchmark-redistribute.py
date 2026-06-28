#!/usr/bin/env python3
"""Redistribute existing benchmark staging shards to workers without regenerating TPC-H."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

# Reuse placement helpers from benchmark_datagen.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark_datagen import (  # noqa: E402
    DIMENSION_TABLES,
    PARTITIONED_TABLES,
    balanced_assignments,
    distribute_shard,
    distribute_to_all_workers,
    ring_assignments,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Redistribute staged benchmark shards to workers (no TPC-H regen)"
    )
    parser.add_argument("data_dir", type=Path, help="Benchmark data dir (contains staging/ and workers/)")
    parser.add_argument("--workers", default="worker-1,worker-2,worker-3")
    parser.add_argument("--rf", type=int, default=2)
    parser.add_argument("--placement", choices=("balanced", "ring"), default="balanced")
    parser.add_argument("--shard-count", type=int, help="Override shard count (default: read metadata.json)")
    parser.add_argument("--root", type=Path)
    args = parser.parse_args()

    root = args.root or Path(__file__).resolve().parent.parent
    data_dir = args.data_dir.resolve()
    staging = data_dir / "staging"
    workers_root = data_dir / "workers"
    metadata_path = data_dir / "metadata.json"

    if not staging.is_dir():
        print(f"ERROR: staging directory not found: {staging}", file=sys.stderr)
        print("Re-run full datagen, or keep staging/ from the last generation.", file=sys.stderr)
        return 1

    shard_count = args.shard_count
    if shard_count is None and metadata_path.is_file():
        shard_count = int(json.loads(metadata_path.read_text())["shard_count"])
    if shard_count is None:
        print("ERROR: pass --shard-count or ensure metadata.json exists", file=sys.stderr)
        return 1

    worker_ids = [item.strip() for item in args.workers.split(",") if item.strip()]
    if workers_root.exists():
        shutil.rmtree(workers_root)
    workers_root.mkdir(parents=True)
    worker_dir_map = {worker_id: workers_root / worker_id for worker_id in worker_ids}

    manifest: dict[str, dict[str, list[str]]] = {}

    print(f"Redistributing {data_dir} ({args.placement} placement, rf={args.rf})...")
    for table in PARTITIONED_TABLES:
        if args.placement == "balanced":
            assignments = balanced_assignments(worker_ids, table, shard_count, args.rf)
        else:
            assignments = ring_assignments(root, worker_ids, table, shard_count, args.rf)
        table_manifest: dict[str, list[str]] = {}
        for shard_id in range(shard_count):
            shard_key = f"{table}_shard{shard_id}"
            shard_path = staging / table / f"{shard_key}.duckdb"
            if not shard_path.is_file():
                print(f"ERROR: missing staged shard {shard_path}", file=sys.stderr)
                return 1
            owners = assignments[shard_key]
            table_manifest[shard_key] = owners
            distribute_shard(shard_path, owners, worker_dir_map)
            print(f"  {shard_key} -> {owners}")
        manifest[table] = table_manifest

    for table in DIMENSION_TABLES:
        shard_key = f"{table}_shard0"
        shard_path = staging / table / f"{shard_key}.duckdb"
        if not shard_path.is_file():
            print(f"ERROR: missing staged shard {shard_path}", file=sys.stderr)
            return 1
        owners = distribute_to_all_workers(shard_path, worker_dir_map)
        manifest[table] = {shard_key: owners}
        print(f"  {shard_key} -> {owners}")

    metadata = {}
    if metadata_path.is_file():
        metadata = json.loads(metadata_path.read_text())
    metadata.update({
        "workers": worker_ids,
        "shard_count": shard_count,
        "replication_factor": args.rf,
        "placement": args.placement,
        "manifest": manifest,
    })
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Updated {metadata_path}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
