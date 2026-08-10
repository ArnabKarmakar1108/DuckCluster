"""Async query API: submit (202), poll status, fetch chunked results."""

from __future__ import annotations

import time

import requests

from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient


def test_async_query_matches_sync_baseline(cluster_manager, demo_csv) -> None:
    """POST /v1/query/async returns 202; status poll and chunked results match sync query."""
    client = QueryClient(cluster_manager.config.http_base_url, timeout_sec=120.0)
    sql = "SELECT category, COUNT(*) AS cnt FROM events GROUP BY category ORDER BY category"

    sync = client.query(sql)

    submit = requests.post(
        f"{cluster_manager.config.http_base_url}/v1/query/async",
        json={"sql": sql},
        timeout=30,
    )
    assert submit.status_code == 202
    body = submit.json()
    query_id = body["queryId"]
    assert body["statusUrl"] == f"/v1/query/{query_id}"

    deadline = time.time() + 60
    status = None
    while time.time() < deadline:
        response = requests.get(
            f"{cluster_manager.config.http_base_url}/v1/query/{query_id}",
            timeout=10,
        )
        assert response.status_code == 200
        status = response.json()
        if status.get("status") in {"SUCCEEDED", "FAILED"}:
            break
        time.sleep(0.2)

    assert status is not None
    assert status["status"] == "SUCCEEDED"

    rows: list = []
    columns = None
    offset = 0
    while True:
        chunk = requests.get(
            f"{cluster_manager.config.http_base_url}/v1/query/{query_id}/results",
            params={"offset": offset, "limit": 2},
            timeout=30,
        )
        assert chunk.status_code == 200
        payload = chunk.json()
        if columns is None:
            columns = payload["columns"]
        rows.extend(payload["rows"])
        next_offset = payload.get("nextOffset")
        if next_offset is None:
            break
        offset = int(next_offset)

    async_result = {"columns": columns, "rows": rows}
    assert_result_matches(async_result, {"columns": sync["columns"], "rows": sync["rows"]})
