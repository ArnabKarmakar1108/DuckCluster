"""Impala-style async query: submit, poll status, fetch result chunks."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

from duckcluster.client import ClientConfig, ClusterError, ConnectionError, CoordinatorClient


def submit_query(client: CoordinatorClient, sql: str) -> dict[str, Any]:
    return client.submit_query(sql)


def wait_for_completion(
    client: CoordinatorClient,
    query_id: str,
    poll_interval_sec: float = 0.25,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    deadline = None if timeout_sec is None else time.monotonic() + timeout_sec
    while True:
        status = client.query_status(query_id)
        state = str(status.get("status", "")).upper()
        if state == "EXPIRED":
            from duckcluster.client import QueryExpiredError

            raise QueryExpiredError(410, str(status.get("error", "query result expired")))
        if state == "CANCELLED":
            raise ClusterError(499, str(status.get("error", "query cancelled")))
        if state in {"SUCCEEDED", "FAILED"}:
            if state == "FAILED":
                raise ClusterError(500, str(status.get("error", "query failed")))
            return status
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(f"query {query_id} did not finish within {timeout_sec}s")
        time.sleep(poll_interval_sec)


def fetch_all_rows(
    client: CoordinatorClient,
    query_id: str,
    chunk_size: int = 1000,
) -> dict[str, Any]:
    offset = 0
    columns: list[str] | None = None
    rows: list[list[Any]] = []
    stats: dict[str, Any] | None = None

    while True:
        chunk = client.query_results(query_id, offset=offset, limit=chunk_size)
        if columns is None:
            columns = list(chunk.get("columns", []))
        rows.extend(chunk.get("rows", []))
        stats = chunk.get("stats", stats)
        next_offset = chunk.get("nextOffset")
        if next_offset is None:
            break
        offset = int(next_offset)

    return {
        "queryId": query_id,
        "columns": columns or [],
        "rows": rows,
        "stats": stats or {},
    }


def query_async(client: CoordinatorClient, sql: str, chunk_size: int = 1000) -> dict[str, Any]:
    accepted = submit_query(client, sql)
    query_id = str(accepted["queryId"])
    try:
        wait_for_completion(client, query_id, timeout_sec=client.timeout_sec)
    except KeyboardInterrupt:
        try:
            client.cancel_query(query_id)
        except (ConnectionError, ClusterError):
            pass
        raise
    return fetch_all_rows(client, query_id, chunk_size=chunk_size)


def execute_query(
    client: CoordinatorClient,
    sql: str,
    *,
    sync: bool = False,
    chunk_size: int = 1000,
) -> dict[str, Any]:
    """Run SQL via async submit/poll/chunk (default) or blocking POST /v1/query."""
    if sync:
        return client.query(sql)
    return query_async(client, sql, chunk_size=chunk_size)
