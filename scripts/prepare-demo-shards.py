#!/usr/bin/env python3
"""Split demo CSV into DuckDB shard files and distribute to worker directories.

Python fallback when the duckdb CLI is not installed. Mirrors split-and-distribute.sh.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb


def ring_assignments(
    repo_root: Path,
    worker_ids: list[str],
    table: str,
    shard_count: int,
    replication_factor: int,
    vnodes: int = 100,
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
            "--vnodes",
            str(vnodes),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def build_shard_db(
    source: Path,
    staging_dir: Path,
    table: str,
    key: str,
    shard_id: int,
    shard_count: int,
    source_format: str,
) -> Path:
    staging_dir.mkdir(parents=True, exist_ok=True)
    shard_key = f"{table}_shard{shard_id}"
    shard_path = staging_dir / f"{shard_key}.duckdb"
    if shard_path.exists():
        shard_path.unlink()

    if source_format == "parquet":
        read_expr = f"read_parquet('{source}')"
    else:
        read_expr = f"read_csv_auto('{source}')"

    connection = duckdb.connect(str(shard_path))
    connection.execute(
        f"""
        CREATE TABLE {table} AS
        SELECT * FROM {read_expr}
        WHERE abs(hash({key})) % {shard_count} = {shard_id}
        """
    )
    connection.close()
    return shard_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Split demo data into DuckDB shards")
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--table", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--shards", required=True, type=int)
    parser.add_argument("--workers", required=True, help="Comma-separated worker IDs")
    parser.add_argument("--dirs", required=True, help="Comma-separated worker data dirs")
    parser.add_argument("--rf", type=int, default=2)
    parser.add_argument("--vnodes", type=int, default=100)
    parser.add_argument("--format", choices=("csv", "parquet"))
    parser.add_argument("--root", type=Path)
    args = parser.parse_args()

    root = args.root or Path(__file__).resolve().parent.parent
    source = args.source.resolve()
    if not source.is_file():
        print(f"ERROR: source file not found: {source}", file=sys.stderr)
        return 1

    source_format = args.format
    if source_format is None:
        if source.suffix == ".parquet":
            source_format = "parquet"
        elif source.suffix == ".csv":
            source_format = "csv"
        else:
            print("ERROR: cannot detect format; use --format csv|parquet", file=sys.stderr)
            return 1

    worker_ids = [item.strip() for item in args.workers.split(",") if item.strip()]
    worker_dirs = [item.strip() for item in args.dirs.split(",") if item.strip()]
    if len(worker_ids) != len(worker_dirs):
        print("ERROR: --workers and --dirs must have the same number of entries", file=sys.stderr)
        return 1

    print("=== Split & Distribute (Python) ===")
    print(f"  Source:  {source} ({source_format})")
    print(f"  Table:   {args.table}")
    print(f"  Key:     {args.key}")
    print(f"  Shards:  {args.shards}")
    print(f"  Workers: {','.join(worker_ids)}")
    print(f"  RF:      {args.rf}")
    print()

    assignments = ring_assignments(
        root,
        worker_ids,
        args.table,
        args.shards,
        args.rf,
        args.vnodes,
    )

    staging = root / "data" / ".staging" / args.table
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, list[str]] = {}
    worker_dir_map = dict(zip(worker_ids, worker_dirs, strict=True))

    print(f"Step 2: Splitting source data into {args.shards} shards...")
    for shard_id in range(args.shards):
        shard_key = f"{args.table}_shard{shard_id}"
        shard_path = build_shard_db(
            source,
            staging,
            args.table,
            args.key,
            shard_id,
            args.shards,
            source_format,
        )
        row_count = duckdb.connect(str(shard_path), read_only=True).execute(
            f"SELECT count(*) FROM {args.table}"
        ).fetchone()[0]
        print(f"  {shard_key}.duckdb: {row_count} rows")

        owners = assignments[shard_key]
        manifest[shard_key] = owners
        for owner in owners:
            target_dir = Path(worker_dir_map[owner])
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / shard_path.name
            shutil.copy2(shard_path, target)
            print(f"  {shard_key} -> {owner} ({target_dir})")

    manifest_file = root / "data" / "manifest.json"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print()
    print(f"Manifest written to: {manifest_file}")
    print("=== Done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
