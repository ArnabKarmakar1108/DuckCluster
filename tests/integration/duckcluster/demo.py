"""Demo dataset generation for integration tests."""

from __future__ import annotations

from pathlib import Path

import duckdb

DEFAULT_DEMO_ROW_COUNT = 1000
BUNDLED_DEMO_CSV_NAME = "demo-events.csv"


def bundled_demo_csv(harness_root: Path) -> Path:
    return harness_root / "data" / BUNDLED_DEMO_CSV_NAME


def row_count_from_csv(csv_path: Path) -> int:
    connection = duckdb.connect()
    count = connection.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{csv_path}')"
    ).fetchone()[0]
    connection.close()
    return int(count)


def write_demo_csv(path: Path, row_count: int = DEFAULT_DEMO_ROW_COUNT) -> Path:
    """Write a deterministic CSV used as the single-node correctness baseline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute(
        f"""
        COPY (
            SELECT
                id::INTEGER AS id,
                'event-' || id AS name,
                (id * 10)::INTEGER AS score,
                CASE id % 3
                    WHEN 0 THEN 'A'
                    WHEN 1 THEN 'B'
                    ELSE 'C'
                END AS category
            FROM range(1, {row_count + 1}) AS t(id)
        ) TO '{path}' (HEADER, DELIMITER ',')
        """
    )
    connection.close()
    return path
