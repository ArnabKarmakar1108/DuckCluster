"""Single-node DuckDB baseline for comparing distributed query results."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


def local_baseline(demo_csv: Path, sql: str) -> dict[str, Any]:
    """Run SQL against the full demo CSV in embedded DuckDB and return HTTP-shaped results."""
    connection = duckdb.connect()
    connection.execute(
        f"CREATE TABLE events AS SELECT * FROM read_csv_auto('{demo_csv}')"
    )
    cursor = connection.execute(sql)
    columns = [column[0] for column in cursor.description]
    rows = [
        [_stringify(value) for value in row]
        for row in cursor.fetchall()
    ]
    connection.close()
    return {"columns": columns, "rows": rows}


def expected_for_demo(demo_csv: Path, sql: str) -> dict[str, Any]:
    return local_baseline(demo_csv, sql)


def tpch_baseline(baseline_db: Path, sql: str) -> dict[str, Any]:
    """Run SQL against the single-node TPC-H baseline DuckDB database."""
    connection = duckdb.connect(str(baseline_db), read_only=True)
    cursor = connection.execute(sql)
    columns = [column[0] for column in cursor.description]
    rows = [
        [_stringify(value) for value in row]
        for row in cursor.fetchall()
    ]
    connection.close()
    return {"columns": columns, "rows": rows}


def expected_for_tpch(baseline_db: Path, sql: str) -> dict[str, Any]:
    return tpch_baseline(baseline_db, sql)


def _stringify(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)
