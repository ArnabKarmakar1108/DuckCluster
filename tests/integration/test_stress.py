"""Connection-pool limits under heavy concurrent load."""

from __future__ import annotations

import concurrent.futures as thread_pool
from pathlib import Path

import pytest

from duckcluster.baseline import expected_for_demo
from duckcluster.cluster import ClusterManager
from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient

HARNESS_ROOT = Path(__file__).resolve().parent


@pytest.mark.parametrize(
    ("pool_size", "pool_wait_ms", "http_port"),
    [
        (1, 50, 38401),
        (1, 200, 38402),
        (2, 200, 38403),
        (4, 200, 38404),
    ],
)
def test_pool_sizing_under_concurrency(
    harness_root: Path,
    with_cluster_config,
    pool_size: int,
    pool_wait_ms: int,
    http_port: int,
) -> None:
    """Sixteen concurrent GROUP BY queries succeed for several pool-size and wait-time settings."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=http_port,
            coordinator_grpc_port=http_port + 1010,
            worker_ports=(http_port + 1021, http_port + 1022, http_port + 1023),
            pool_size=pool_size,
            pool_wait_ms=pool_wait_ms,
            fragment_wait_ms=90_000,
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

        with thread_pool.ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(lambda _: run_query(), range(16)))

        for actual in results:
            assert_result_matches(actual, expected)
    finally:
        manager.stop()
