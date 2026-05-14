"""Compare DuckCluster HTTP query results against expected .out fixtures."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def load_expected(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {"columns": [], "rows": []}

    lines = [line for line in text.splitlines() if line.strip()]
    header = lines[0]
    if not header.startswith("# columns:"):
        raise ValueError(f"Expected first line '# columns: ...' in {path}")

    columns = [column.strip() for column in header.removeprefix("# columns:").split(",")]
    rows: list[list[str]] = []
    for line in lines[1:]:
        rows.append(line.split("\t"))
    return {"columns": columns, "rows": rows}


def normalize_rows(rows: list[list[Any]]) -> list[tuple[str, ...]]:
    normalized: list[tuple[str, ...]] = []
    for row in rows:
        normalized.append(tuple(_canonicalize_cell(value) for value in row))
    return sorted(normalized)


def _canonicalize_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if text == "":
        return ""
    try:
        number = float(text)
        if number.is_integer():
            return str(int(number))
        return str(number)
    except ValueError:
        return text


def normalize_result(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "columns": list(payload.get("columns", [])),
        "rows": normalize_rows(payload.get("rows", [])),
    }


def assert_result_matches(actual: dict[str, Any], expected: dict[str, Any]) -> None:
    actual_norm = normalize_result(actual)
    expected_norm = normalize_result(expected)

    assert actual_norm["columns"] == expected_norm["columns"], (
        f"column mismatch\nactual={actual_norm['columns']}\nexpected={expected_norm['columns']}"
    )
    assert actual_norm["rows"] == expected_norm["rows"], (
        f"row mismatch\nactual={actual_norm['rows']}\nexpected={expected_norm['rows']}"
    )
