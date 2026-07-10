"""Interactive SQL shell for DuckCluster."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from duckcluster.banner import print_banner
from duckcluster.client import ClusterError, ConnectionError
from duckcluster.input import read_command, remember_command, setup_history
from duckcluster.format import format_query_result, format_stats, format_status, format_workers
from duckcluster.validate import ValidationError, validate_query_prerequisites

if TYPE_CHECKING:
    from duckcluster.client import CoordinatorClient


_EXIT_WORDS = {"quit", "exit", "\\q"}


def run_shell(
    client: CoordinatorClient,
    base_url: str,
    fmt: str = "table",
    verbose: bool = False,
    skip_validate: bool = False,
) -> int:
    print_banner()
    setup_history()
    try:
        health = client.health()
        workers = client.workers()
        status = str(health.get("status", "")).upper()
        worker_count = health.get("workerCount", 0)
        print(f"Connected to {base_url} (status={status}, workers={worker_count})")
        print("Type SQL ending with ';' or a single-line query. Up/down for history. \\q to quit.")
    except (ConnectionError, ClusterError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    buffer: list[str] = []
    while True:
        try:
            line = read_command("duckcluster> " if not buffer else "    ...> ")
        except EOFError:
            print()
            return 0

        stripped = line.strip()
        if not buffer and stripped.lower() in _EXIT_WORDS:
            return 0

        if not buffer and stripped.startswith("\\"):
            if _run_meta(client, base_url, stripped, fmt):
                return 0
            continue

        if not stripped and not buffer:
            continue

        buffer.append(line)
        sql = "\n".join(buffer).strip()
        if sql.endswith(";"):
            history_entry = sql
            exec_sql = sql[:-1].strip()
            buffer = []
            _execute_query(client, exec_sql, fmt, verbose, skip_validate)
            remember_command(history_entry)
        elif len(buffer) == 1 and stripped and not stripped.endswith(";"):
            buffer = []
            _execute_query(client, stripped, fmt, verbose, skip_validate)
            remember_command(stripped)


def _execute_query(
    client: CoordinatorClient,
    sql: str,
    fmt: str,
    verbose: bool,
    skip_validate: bool,
) -> None:
    try:
        if not skip_validate:
            validate_query_prerequisites(client, sql)
        result = client.query(sql)
        sys.stdout.write(format_query_result(result, fmt))
        if verbose:
            print(format_stats(result), file=sys.stderr)
    except (ValidationError, ClusterError, ConnectionError) as exc:
        message = exc.message if isinstance(exc, ClusterError) else str(exc)
        print(f"error: {message}", file=sys.stderr)


def _run_meta(
    client: CoordinatorClient,
    base_url: str,
    command: str,
    fmt: str,
) -> bool:
    """Handle backslash meta-commands. Returns True when the shell should exit."""
    name = command.lower().split(None, 1)[0]
    if name in {"\\q", "\\quit", "\\exit"}:
        return True
    if name == "\\help":
        print(
            "Commands:\n"
            "  \\q, quit, exit     Leave the shell\n"
            "  \\status            Cluster health summary\n"
            "  \\workers           List workers\n"
            "  \\tables            Tables in shard catalog\n"
            "  \\help              Show this help\n"
            "  Up/Down arrows      Previous queries"
        )
        return False
    if name == "\\status":
        try:
            health = client.health()
            workers = client.workers()
            sys.stdout.write(format_status(health, workers, base_url, fmt))
        except (ConnectionError, ClusterError) as exc:
            message = exc.message if isinstance(exc, ClusterError) else str(exc)
            print(f"error: {message}", file=sys.stderr)
        return False
    if name == "\\workers":
        try:
            sys.stdout.write(format_workers(client.workers(), fmt))
        except (ConnectionError, ClusterError) as exc:
            message = exc.message if isinstance(exc, ClusterError) else str(exc)
            print(f"error: {message}", file=sys.stderr)
        return False
    if name == "\\tables":
        try:
            shards = client.shards()
            tables = [table["tableName"] for table in (shards.get("tables") or [])]
            if not tables:
                print("(no tables in catalog)")
            else:
                print(", ".join(sorted(tables)))
        except (ConnectionError, ClusterError) as exc:
            message = exc.message if isinstance(exc, ClusterError) else str(exc)
            print(f"error: {message}", file=sys.stderr)
        return False

    print(f"unknown command: {command}. Type \\help for options.", file=sys.stderr)
    return False
