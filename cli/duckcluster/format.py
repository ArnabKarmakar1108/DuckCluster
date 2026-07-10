"""Human-readable output formatting."""

from __future__ import annotations

import json
import shutil
import sys
from typing import Any


def default_format(stdout_is_tty: bool | None = None) -> str:
    if stdout_is_tty is None:
        stdout_is_tty = sys.stdout.isatty()
    return "table" if stdout_is_tty else "json"


def format_query_result(result: dict[str, Any], fmt: str) -> str:
    if fmt == "json":
        return json.dumps(result, indent=2) + "\n"

    columns = [str(column) for column in result.get("columns") or []]
    rows = result.get("rows") or []
    stats = result.get("stats") or {}
    duration_ms = stats.get("durationMs")

    if not columns:
        footer = _footer(len(rows), duration_ms)
        return footer

    widths = [_display_width(column) for column in columns]
    rendered_rows: list[list[str]] = []

    for row in rows:
        cells = [_format_cell(value) for value in row]
        rendered_rows.append(cells)
        for index, cell in enumerate(cells):
            widths[index] = max(widths[index], _display_width(cell))

    terminal_width = shutil.get_terminal_size(fallback=(120, 20)).columns
    total_width = sum(widths) + (3 * (len(columns) - 1))
    if total_width > terminal_width and len(columns) > 1:
        overflow = total_width - terminal_width
        for index in range(len(widths) - 1, -1, -1):
            if widths[index] <= 8:
                continue
            shrink = min(widths[index] - 8, overflow)
            widths[index] -= shrink
            overflow -= shrink
            if overflow <= 0:
                break

    header = " | ".join(cell.ljust(widths[index]) for index, cell in enumerate(columns))
    divider = "-+-".join("-" * width for width in widths)
    body = "\n".join(
        " | ".join(
            _truncate_to_width(cell, widths[index]).ljust(widths[index])
            for index, cell in enumerate(cells)
        )
        for cells in rendered_rows
    )
    footer = _footer(len(rows), duration_ms)
    return "\n".join(part for part in (header, divider, body, footer) if part) + "\n"


def format_status(health: dict[str, Any], workers: dict[str, Any], base_url: str, fmt: str) -> str:
    if fmt == "json":
        payload = {
            "status": health.get("status"),
            "workerCount": health.get("workerCount"),
            "healthyWorkers": _healthy_count(workers),
            "url": base_url,
        }
        return json.dumps(payload, indent=2) + "\n"

    status = health.get("status", "UNKNOWN")
    worker_count = health.get("workerCount", 0)
    healthy = _healthy_count(workers)
    return (
        f"Cluster: {status}\n"
        f"Workers: {worker_count} registered ({healthy} healthy)\n"
        f"Coordinator: {base_url}\n"
    )


def format_workers(workers: dict[str, Any], fmt: str) -> str:
    if fmt == "json":
        return json.dumps(workers, indent=2) + "\n"

    rows = workers.get("workers") or []
    header = f"{'WORKER':<10} {'HOST':<14} {'PORT':<6} {'STATUS':<10}"
    lines = [header, "-" * len(header)]
    for worker in rows:
        lines.append(
            f"{worker.get('workerId', ''):<10} "
            f"{worker.get('host', ''):<14} "
            f"{str(worker.get('port', '')):<6} "
            f"{worker.get('status', ''):<10}"
        )
    return "\n".join(lines) + "\n"


def format_stats(result: dict[str, Any]) -> str:
    stats = result.get("stats") or {}
    merge = stats.get("mergeStrategy", "?")
    duration = stats.get("durationMs", "?")
    workers = stats.get("workersUsed", "?")
    fragments = stats.get("fragmentsExecuted", "?")
    return (
        f"merge={merge} duration={duration}ms "
        f"workers={workers} fragments={fragments}"
    )


def _healthy_count(workers: dict[str, Any]) -> int:
    return sum(
        1
        for worker in (workers.get("workers") or [])
        if str(worker.get("status", "")).upper() == "HEALTHY"
    )


def _format_cell(value: Any) -> str:
    if value is None:
        return "NULL"
    return str(value)


def _display_width(text: str) -> int:
    return min(len(text), 40)


def _truncate_to_width(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def _footer(row_count: int, duration_ms: Any) -> str:
    if duration_ms is None:
        return f"({row_count} rows)"
    return f"({row_count} rows in {duration_ms} ms)"
