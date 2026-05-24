"""TPC-H integration test helpers."""

from __future__ import annotations

import subprocess
from pathlib import Path

HARNESS_ROOT = Path(__file__).resolve().parent.parent
QUERIES_DIR = HARNESS_ROOT / "queries" / "tpch"
DEFAULT_SF = 0.01

# Queries expected to pass end-to-end after Phase 1 (single-table lineitem).
PHASE1_PASSING = ("q01", "q06")

# Queries expected to pass end-to-end after Phase 2 (derived tables in FROM).
# Q08 needs Phase 4 (multi-aggregate arithmetic in SELECT).
PHASE2_PASSING = ("q07", "q09", "q13")

# Combined passing set for correctness tests.
# Phase 3: comma-joins, table aliases, qualified names.
PHASE3_PASSING = ("q05", "q10", "q12")

PASSING_QUERIES = PHASE1_PASSING + PHASE2_PASSING + PHASE3_PASSING

# All 22 queries for planning/correctness tracking.
ALL_QUERIES = tuple(f"q{i:02d}" for i in range(1, 23))


def tpch_data_dir(repo_root: Path, scale_factor: float = DEFAULT_SF) -> Path:
    label = str(scale_factor).replace(".", "_") if "." in str(scale_factor) else str(int(scale_factor))
    if scale_factor == 0.01:
        return repo_root / "benchmark" / "data" / "sf0.01"
    return repo_root / "benchmark" / "data" / f"sf{label}"


def ensure_tpch_data(repo_root: Path, scale_factor: float = DEFAULT_SF) -> Path:
    data_dir = tpch_data_dir(repo_root, scale_factor)
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

    subprocess.run(
        [
            str(repo_root / "scripts" / "benchmark-datagen.sh"),
            str(scale_factor),
            "6",
            str(data_dir),
        ],
        cwd=repo_root,
        check=True,
    )
    return data_dir


def load_tpch_sql(query_id: str) -> str:
    path = QUERIES_DIR / f"{query_id}.sql"
    if not path.is_file():
        raise FileNotFoundError(f"TPC-H query not found: {path}")
    sql = path.read_text(encoding="utf-8").strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql
