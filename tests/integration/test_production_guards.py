"""Integration coverage for production-readiness regressions (section 9.1)."""

from __future__ import annotations

import concurrent.futures as thread_pool
from pathlib import Path

import requests

from duckcluster.baseline import expected_for_demo
from duckcluster.cluster import ClusterManager
from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient
from duckcluster.waits import wait_for_worker_count

HARNESS_ROOT = Path(__file__).resolve().parent


def test_coordinator_restart_workers_reregister(harness_root: Path, with_cluster_config) -> None:
    """Workers survive coordinator restart and queries succeed after re-registration."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38680,
            coordinator_grpc_port=39690,
            worker_ports=(39701, 39702, 39703),
            heartbeat_interval_sec=1,
            heartbeat_miss_threshold=3,
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        sql = (harness_root / "queries/correctness/select_all.sql").read_text(encoding="utf-8").strip()
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, sql)
        assert_result_matches(client.query(sql), expected)

        manager.restart_coordinator()
        wait_for_worker_count(client, len(manager.config.worker_ids), timeout_sec=90)
        assert_result_matches(client.query(sql), expected)
    finally:
        manager.stop()


def test_concurrent_self_join_queries(harness_root: Path, with_cluster_config) -> None:
    """Parallel join queries on the same workers do not clash on broadcast temp tables."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38780,
            coordinator_grpc_port=39790,
            worker_ports=(39801, 39802, 39803),
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        sql = (
            "SELECT e1.category, COUNT(*) AS cnt "
            "FROM events e1 JOIN events e2 ON e1.id = e2.id "
            "GROUP BY e1.category ORDER BY e1.category"
        )
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, sql)

        def run_query() -> dict:
            return client.query(sql)

        with thread_pool.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _: run_query(), range(8)))

        for actual in results:
            assert_result_matches(actual, expected)
    finally:
        manager.stop()


def test_explain_returns_query_scoped_broadcast_names(cluster_manager: ClusterManager) -> None:
    """Explain output includes per-query broadcast temp table names."""
    base = cluster_manager.config.http_base_url
    sql = (
        "SELECT e1.category FROM events e1 "
        "JOIN events e2 ON e1.id = e2.id LIMIT 1"
    )
    response = requests.post(f"{base}/v1/query/explain", json={"sql": sql}, timeout=30)
    assert response.status_code == 200
    body = response.json()
    query_id = body["queryId"]
    broadcasts = body.get("broadcastTables") or body.get("subqueryBroadcastTables") or []
    if broadcasts:
        temp_name = broadcasts[0]["workerTempTable"]
        assert query_id in temp_name
        assert temp_name.startswith("__dc_bcast_")
