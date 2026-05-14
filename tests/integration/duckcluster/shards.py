"""Shard assignment and fixture helpers for integration tests."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import duckdb


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


def shard_for_id(row_id: int, shard_count: int) -> int:
    return duckdb.sql(
        f"SELECT abs(hash(CAST('{row_id}' AS VARCHAR))) % {shard_count}"
    ).fetchone()[0]


def ids_for_shard(demo_csv: Path, shard_id: int, shard_count: int) -> list[int]:
    connection = duckdb.connect()
    rows = connection.execute(
        f"""
        SELECT id
        FROM read_csv_auto('{demo_csv}')
        WHERE abs(hash(CAST(id AS VARCHAR))) % {shard_count} = {shard_id}
        ORDER BY id
        """
    ).fetchall()
    connection.close()
    return [int(row[0]) for row in rows]


def build_shard_db(
    demo_csv: Path,
    staging_dir: Path,
    table: str,
    shard_id: int,
    shard_count: int,
) -> Path:
    staging_dir.mkdir(parents=True, exist_ok=True)
    shard_key = f"{table}_shard{shard_id}"
    shard_path = staging_dir / f"{shard_key}.duckdb"
    if shard_path.exists():
        shard_path.unlink()
    connection = duckdb.connect(str(shard_path))
    connection.execute(
        f"""
        CREATE TABLE {table} AS
        SELECT * FROM read_csv_auto('{demo_csv}')
        WHERE abs(hash(CAST(id AS VARCHAR))) % {shard_count} = {shard_id}
        """
    )
    connection.close()
    return shard_path


def count_rows_in_shard(shard_path: Path, table: str) -> int:
    connection = duckdb.connect(str(shard_path), read_only=True)
    count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    connection.close()
    return int(count)
