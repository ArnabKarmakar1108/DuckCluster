"""TPC-H integration test helpers."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import requests

HARNESS_ROOT = Path(__file__).resolve().parent.parent
QUERIES_DIR = HARNESS_ROOT / "queries" / "tpch"
DEFAULT_SF = 0.01

# All 22 queries for planning/correctness tracking.
ALL_QUERIES = tuple(f"q{i:02d}" for i in range(1, 23))

# Expression aggregates on lineitem (SUM of expressions, CASE).
EXPRESSION_AGGREGATE_PASSING = ("q01", "q06")

# Derived tables in FROM (inline views). Q08 needs nested aggregate arithmetic.
DERIVED_TABLE_FROM_PASSING = ("q07", "q09", "q13")

# Comma-joins, table aliases, qualified names.
COMMA_JOIN_ALIAS_PASSING = ("q05", "q10", "q12")

# Arithmetic over nested aggregates in SELECT.
NESTED_AGGREGATE_ARITHMETIC_PASSING = ("q08", "q14")

# Reserved SQL aliases (value).
RESERVED_ALIAS_PASSING = ("q11",)

# COUNT(DISTINCT ...) global merge.
COUNT_DISTINCT_PASSING = ("q16",)

# Uncorrelated IN/NOT IN/EXISTS with global subquery semantics.
UNCORRELATED_SUBQUERY_PASSING = ("q18",)

# Correlated EXISTS/NOT EXISTS with co-located shard rewrite.
COLOCATED_EXISTS_PASSING = ("q04", "q21")

# Correlated scalar subqueries and derived-table predicate rewrite.
CORRELATED_SCALAR_SUBQUERY_PASSING = ("q02", "q17", "q20", "q22")

# WITH/CTE coordinator two-step merge.
WITH_CTE_MERGE_PASSING = ("q15",)

# TOP_K ORDER BY on aggregate aliases after joins.
TOPK_AGGREGATE_ALIAS_PASSING = ("q03", "q19")

# Full end-to-end gate: fragment validation, worker execution, merge.
FULL_CORRECTNESS_PASSING = ALL_QUERIES

PASSING_QUERIES = ALL_QUERIES


def tpch_data_dir(repo_root: Path, scale_factor: float = DEFAULT_SF) -> Path:
    label = str(scale_factor).replace(".", "_") if "." in str(scale_factor) else str(int(scale_factor))
    if scale_factor == 0.01:
        return repo_root / "benchmark" / "data" / "sf0.01"
    return repo_root / "benchmark" / "data" / f"sf{label}"


# Default generator layout: both fact tables hash-partitioned by order key (co-partitioned).
DEFAULT_PARTITIONING = {"lineitem": "l_orderkey", "orders": "o_orderkey"}


def ensure_tpch_data(
    repo_root: Path,
    scale_factor: float = DEFAULT_SF,
    data_dir: Path | None = None,
    partitions: tuple[str, ...] = (),
) -> Path:
    """Generate TPC-H shards once. ``partitions`` holds TABLE=KEY:SHARDS layout overrides."""
    data_dir = data_dir or tpch_data_dir(repo_root, scale_factor)
    baseline = data_dir / "baseline.duckdb"
    if baseline.is_file():
        return data_dir

    venv_python = repo_root / "tests" / "integration" / ".venv" / "bin" / "python"
    if not venv_python.is_file():
        subprocess.run(
            ["python3", "-m", "venv", str(repo_root / "tests" / "integration" / ".venv")],
            check=True,
        )
        subprocess.run(
            [str(repo_root / "tests" / "integration" / ".venv" / "bin" / "pip"),
             "install", "-r", str(repo_root / "tests" / "integration" / "requirements.txt")],
            check=True,
        )
        venv_python = repo_root / "tests" / "integration" / ".venv" / "bin" / "python"

    command = [
        str(repo_root / "scripts" / "benchmark-datagen.sh"),
        str(scale_factor),
        "6",
        str(data_dir),
    ]
    for partition in partitions:
        command += ["--partition", partition]
    subprocess.run(command, cwd=repo_root, check=True)
    return data_dir


def tpch_partitioning(data_dir: Path) -> dict[str, tuple[str, int]]:
    """Fact-table layout recorded by the generator (older datasets predate the field)."""
    metadata = json.loads((data_dir / "metadata.json").read_text(encoding="utf-8"))
    recorded = metadata.get("partitioning")
    if recorded:
        return {table: (spec["shard_key"], int(spec["shard_count"])) for table, spec in recorded.items()}
    shard_count = int(metadata["shard_count"])
    return {table: (key, shard_count) for table, key in DEFAULT_PARTITIONING.items()}


def declare_tpch_tables(base_url: str, data_dir: Path) -> None:
    """Declare fact-table shard keys so the planner can co-partition or shuffle joins."""
    for table, (shard_key, shard_count) in tpch_partitioning(data_dir).items():
        response = requests.post(
            f"{base_url}/v1/catalog/tables",
            json={"name": table, "shardCount": shard_count, "shardKey": shard_key},
            timeout=30,
        )
        response.raise_for_status()


def explain(base_url: str, sql: str) -> dict:
    response = requests.post(f"{base_url}/v1/query/explain", json={"sql": sql}, timeout=60)
    response.raise_for_status()
    return response.json()


def load_tpch_sql(query_id: str) -> str:
    path = QUERIES_DIR / f"{query_id}.sql"
    if not path.is_file():
        raise FileNotFoundError(f"TPC-H query not found: {path}")
    sql = path.read_text(encoding="utf-8").strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql
