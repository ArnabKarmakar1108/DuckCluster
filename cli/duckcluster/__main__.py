"""DuckCluster CLI entry point."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from duckcluster.client import ClientConfig, ClusterError, ConnectionError, CoordinatorClient
from duckcluster.format import (
    default_format,
    format_query_result,
    format_stats,
    format_status,
    format_workers,
)
from duckcluster.shell import run_shell
from duckcluster.validate import ValidationError, validate_query_prerequisites


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    parser = _build_parser()
    if not argv and sys.stdin.isatty():
        argv = ["shell"]
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 2

    base_url = args.url or os.environ.get("DUCKCLUSTER_URL", "http://127.0.0.1:8080")
    timeout = float(
        args.timeout
        if args.timeout is not None
        else os.environ.get("DUCKCLUSTER_TIMEOUT", "300")
    )
    fmt = args.format or default_format()

    client = CoordinatorClient(ClientConfig(base_url=base_url, timeout_sec=timeout))

    try:
        if args.command == "shell":
            return run_shell(
                client,
                base_url,
                fmt=fmt,
                verbose=args.verbose,
                skip_validate=args.skip_validate,
            )
        if args.command == "query":
            return _run_query(client, args, fmt)
        if args.command == "status":
            return _run_status(client, base_url, fmt)
        if args.command == "workers":
            return _run_workers(client, fmt)
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except ClusterError as exc:
        print(f"error: {exc.message}", file=sys.stderr)
        return 1
    except ConnectionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    parser.print_help()
    return 2


def _run_query(client: CoordinatorClient, args: argparse.Namespace, fmt: str) -> int:
    try:
        sql = _read_sql(args)
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if not sql.strip():
        print("error: SQL is required", file=sys.stderr)
        return 2

    if not args.skip_validate:
        validate_query_prerequisites(client, sql)

    result = client.query(sql)
    sys.stdout.write(format_query_result(result, fmt))
    if args.verbose:
        print(format_stats(result), file=sys.stderr)
    return 0


def _run_status(client: CoordinatorClient, base_url: str, fmt: str) -> int:
    health = client.health()
    workers = client.workers()
    sys.stdout.write(format_status(health, workers, base_url, fmt))
    status = str(health.get("status", "")).upper()
    return 0 if status == "UP" else 1


def _run_workers(client: CoordinatorClient, fmt: str) -> int:
    workers = client.workers()
    sys.stdout.write(format_workers(workers, fmt))
    return 0


def _read_sql(args: argparse.Namespace) -> str:
    if args.file:
        path = Path(args.file)
        if not path.is_file():
            raise OSError(f"SQL file not found: {path}")
        return path.read_text(encoding="utf-8")
    if args.sql:
        return args.sql
    if not sys.stdin.isatty():
        return sys.stdin.read()
    return ""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="duckcluster",
        description="DuckCluster command-line client",
    )

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--url",
        help="Coordinator base URL (default http://127.0.0.1:8080 or DUCKCLUSTER_URL)",
    )
    common.add_argument(
        "--timeout",
        type=float,
        help="HTTP timeout in seconds (default 300 or DUCKCLUSTER_TIMEOUT)",
    )
    common.add_argument(
        "--format",
        choices=("json", "table"),
        help="Output format (default table on TTY, json when piped)",
    )

    subparsers = parser.add_subparsers(dest="command")

    shell_parser = subparsers.add_parser(
        "shell",
        parents=[common],
        help="Interactive SQL shell (default when run with no command)",
    )
    shell_parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Skip table/shard pre-flight checks",
    )
    shell_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print query stats to stderr",
    )

    query_parser = subparsers.add_parser(
        "query",
        parents=[common],
        help="Run a single SQL statement",
    )
    query_parser.add_argument("sql", nargs="?", help="SQL to execute")
    query_parser.add_argument("-f", "--file", help="Read SQL from file")
    query_parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Skip table/shard pre-flight checks",
    )
    query_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print query stats to stderr",
    )

    subparsers.add_parser(
        "status",
        parents=[common],
        help="Show cluster health summary",
    )
    subparsers.add_parser(
        "workers",
        parents=[common],
        help="List registered workers",
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
