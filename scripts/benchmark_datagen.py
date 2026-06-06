#!/usr/bin/env python3
"""Generate TPC-H benchmark data for DuckCluster and single-node baseline."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

PARTITIONED_TABLES = {
    "lineitem": "l_orderkey",
    "orders": "o_orderkey",
}

DIMENSION_TABLES = [
    "customer",
    "nation",
    "part",
    "partsupp",
    "region",
    "supplier",
]


def ring_assignments(
    repo_root: Path,
    worker_ids: list[str],
    table: str,
    shard_count: int,
    replication_factor: int,
) -> dict[str, list[str]]:
    subprocess.run(
        ["mvn", "-q", "-pl", "common", "compile"],
        cwd=repo_root,
        check=True,
    )
    completed = subprocess.run(
        [
            "java",
            "-cp",
            str(repo_root / "common/target/classes"),
            "io.duckcluster.common.routing.RingAssignerCli",
            "--workers",
            ",".join(worker_ids),
            "--table",
            table,
            "--shards",
            str(shard_count),
            "--rf",
            str(replication_factor),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def generate_tpch(source_db: Path, scale_factor: float) -> None:
    if source_db.exists():
        source_db.unlink()
    connection = duckdb.connect(str(source_db))
    connection.execute("INSTALL tpch")
    connection.execute("LOAD tpch")
    connection.execute(f"CALL dbgen(sf := {scale_factor})")
    connection.close()


def export_baseline(source_db: Path, baseline_db: Path) -> None:
    if baseline_db.exists():
        baseline_db.unlink()
    connection = duckdb.connect(str(baseline_db))
    connection.execute(f"ATTACH '{source_db}' AS src (READ_ONLY)")
    for table in list(PARTITIONED_TABLES) + DIMENSION_TABLES:
        connection.execute(f"CREATE TABLE {table} AS SELECT * FROM src.{table}")
    connection.execute("DETACH src")
    connection.close()


def build_partitioned_shard(
    source_db: Path,
    staging_dir: Path,
    table: str,
    key: str,
    shard_id: int,
    shard_count: int,
) -> Path:
    staging_dir.mkdir(parents=True, exist_ok=True)
    shard_name = f"{table}_shard{shard_id}"
    shard_path = staging_dir / f"{shard_name}.duckdb"
    if shard_path.exists():
        shard_path.unlink()

    connection = duckdb.connect(str(shard_path))
    connection.execute(f"ATTACH '{source_db}' AS src (READ_ONLY)")
    connection.execute(
        f"""
        CREATE TABLE {table} AS
        SELECT * FROM src.{table}
        WHERE abs(hash({key})) % {shard_count} = {shard_id}
        """
    )
    connection.execute("DETACH src")
    connection.close()
    return shard_path


def build_dimension_shard(source_db: Path, staging_dir: Path, table: str) -> Path:
    staging_dir.mkdir(parents=True, exist_ok=True)
    shard_name = f"{table}_shard0"
    shard_path = staging_dir / f"{shard_name}.duckdb"
    if shard_path.exists():
        shard_path.unlink()

    connection = duckdb.connect(str(shard_path))
    connection.execute(f"ATTACH '{source_db}' AS src (READ_ONLY)")
    connection.execute(f"CREATE TABLE {table} AS SELECT * FROM src.{table}")
    connection.execute("DETACH src")
    connection.close()
    return shard_path


def distribute_shard(
    shard_path: Path,
    owners: list[str],
    worker_dir_map: dict[str, Path],
) -> None:
    for owner in owners:
        target_dir = worker_dir_map[owner]
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(shard_path, target_dir / shard_path.name)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate TPC-H benchmark datasets")
    parser.add_argument("scale_factor", type=float, help="TPC-H scale factor (e.g. 1, 10)")
    parser.add_argument("shard_count", type=int, help="Number of shards per partitioned table")
    parser.add_argument("output_dir", type=Path, help="Output directory for generated data")
    parser.add_argument("--workers", default="worker-1,worker-2,worker-3")
    parser.add_argument("--rf", type=int, default=2)
    parser.add_argument("--root", type=Path)
    args = parser.parse_args()

    root = args.root or Path(__file__).resolve().parent.parent
    output_dir = args.output_dir.resolve()
    worker_ids = [item.strip() for item in args.workers.split(",") if item.strip()]
    workers_root = output_dir / "workers"
    staging = output_dir / "staging"
    source_db = output_dir / "tpch-source.duckdb"
    baseline_db = output_dir / "baseline.duckdb"

    if staging.exists():
        shutil.rmtree(staging)
    if workers_root.exists():
        shutil.rmtree(workers_root)
    staging.mkdir(parents=True, exist_ok=True)
    workers_root.mkdir(parents=True, exist_ok=True)

    worker_dir_map = {
        worker_id: workers_root / worker_id for worker_id in worker_ids
    }

    print(f"=== TPC-H data generation (SF={args.scale_factor}) ===")
    print("Step 1: Generating TPC-H tables...")
    generate_tpch(source_db, args.scale_factor)

    print("Step 2: Writing single-node baseline...")
    export_baseline(source_db, baseline_db)

    manifest: dict[str, dict[str, list[str]]] = {}

    print(f"Step 3: Partitioning fact tables into {args.shard_count} shards...")
    for table, key in PARTITIONED_TABLES.items():
        table_manifest: dict[str, list[str]] = {}
        assignments = ring_assignments(root, worker_ids, table, args.shard_count, args.rf)
        for shard_id in range(args.shard_count):
            shard_path = build_partitioned_shard(
                source_db,
                staging / table,
                table,
                key,
                shard_id,
                args.shard_count,
            )
            shard_key = f"{table}_shard{shard_id}"
            owners = assignments[shard_key]
            table_manifest[shard_key] = owners
            row_count = duckdb.connect(str(shard_path), read_only=True).execute(
                f"SELECT count(*) FROM {table}"
            ).fetchone()[0]
            print(f"  {shard_key}: {row_count} rows -> {owners}")
            distribute_shard(shard_path, owners, worker_dir_map)
        manifest[table] = table_manifest

    print("Step 4: Replicating dimension tables (1 shard each)...")
    for table in DIMENSION_TABLES:
        assignments = ring_assignments(root, worker_ids, table, 1, args.rf)
        shard_path = build_dimension_shard(source_db, staging / table, table)
        shard_key = f"{table}_shard0"
        owners = assignments[shard_key]
        manifest[table] = {shard_key: owners}
        row_count = duckdb.connect(str(shard_path), read_only=True).execute(
            f"SELECT count(*) FROM {table}"
        ).fetchone()[0]
        print(f"  {shard_key}: {row_count} rows -> {owners}")
        distribute_shard(shard_path, owners, worker_dir_map)

    metadata = {
        "scale_factor": args.scale_factor,
        "shard_count": args.shard_count,
        "workers": worker_ids,
        "replication_factor": args.rf,
        "baseline": str(baseline_db),
        "manifest": manifest,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"Baseline database: {baseline_db}")
    print(f"Worker data root:  {workers_root}")
    print(f"Metadata:          {output_dir / 'metadata.json'}")
    print("=== Done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
