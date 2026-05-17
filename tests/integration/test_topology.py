"""Behavior when cluster topology changes."""

from __future__ import annotations

import time
from pathlib import Path

from duckcluster.baseline import expected_for_demo
from duckcluster.cluster import ClusterManager
from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient
from duckcluster.waits import wait_for_worker_count

HARNESS_ROOT = Path(__file__).resolve().parent


def test_worker_addition_queries_remain_correct(harness_root: Path, with_cluster_config) -> None:
    """Row counts stay correct while a new worker joins and the hash ring is updated."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=58080,
            coordinator_grpc_port=59090,
            worker_ports=(59101, 59102, 59103),
            watcher_interval_ms=500,
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        sql = (harness_root / "queries/topology/select_count.sql").read_text(encoding="utf-8").strip()
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, sql)

        assert_result_matches(client.query(sql), expected)

        manager.add_worker("worker-4", 59104)
        wait_for_worker_count(client, 4, timeout_sec=60)

        deadline = time.time() + 20
        while time.time() < deadline:
            assert_result_matches(client.query(sql), expected)
            time.sleep(2)

        assert_result_matches(client.query(sql), expected)
    finally:
        manager.stop()
