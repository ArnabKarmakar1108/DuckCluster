"""Correctness under concurrent client load."""

from __future__ import annotations

import concurrent.futures as thread_pool
from pathlib import Path

from duckcluster.baseline import expected_for_demo
from duckcluster.cluster import ClusterManager
from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient

HARNESS_ROOT = Path(__file__).resolve().parent


def test_concurrent_group_by_matches_baseline(harness_root: Path, with_cluster_config) -> None:
    """Ten parallel GROUP BY queries all match the single-node baseline."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38201,
            coordinator_grpc_port=39211,
            worker_ports=(39221, 39222, 39223),
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        sql = (harness_root / "queries/correctness/group_by_count.sql").read_text(encoding="utf-8").strip()
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, sql)

        def run_query() -> dict:
            return client.query(sql)

        with thread_pool.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(lambda _: run_query(), range(10)))

        for actual in results:
            assert_result_matches(actual, expected)
    finally:
        manager.stop()
