"""Cluster behavior when workers stop or lose shards."""

from __future__ import annotations

from pathlib import Path

from duckcluster.baseline import expected_for_demo
from duckcluster.cluster import ClusterManager
from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient
from duckcluster.waits import count_shard_replicas, wait_for_worker_absent, wait_for_worker_count

HARNESS_ROOT = Path(__file__).resolve().parent


def test_worker_failure_replica_takeover(harness_root: Path, with_cluster_config) -> None:
    """Queries keep returning correct results after a worker is killed and removed from the cluster."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38580,
            coordinator_grpc_port=39590,
            worker_ports=(39601, 39602, 39603),
            heartbeat_interval_sec=1,
            heartbeat_miss_threshold=2,
            watcher_interval_ms=500,
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        sql = (harness_root / "queries/correctness/group_by_count.sql").read_text(encoding="utf-8").strip()
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, sql)

        assert_result_matches(client.query(sql), expected)

        manager.kill_worker("worker-1")
        wait_for_worker_absent(client, "worker-1", timeout_sec=20)
        wait_for_worker_count(client, 2, timeout_sec=20)

        assert_result_matches(client.query(sql), expected)
    finally:
        manager.stop()


def test_reconciliation_after_worker_death(harness_root: Path, with_cluster_config) -> None:
    """After a permanent worker loss, shards are re-replicated and full-table reads stay correct."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38180,
            coordinator_grpc_port=39190,
            worker_ports=(39201, 39202, 39203),
            heartbeat_interval_sec=1,
            heartbeat_miss_threshold=2,
            watcher_interval_ms=500,
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        sql = (harness_root / "queries/correctness/select_all.sql").read_text(encoding="utf-8").strip()
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, sql)

        manager.kill_worker("worker-1")
        wait_for_worker_absent(client, "worker-1", timeout_sec=20)

        remaining_workers = ["worker-2", "worker-3"]

        def all_shards_replicated() -> bool:
            for shard_id in range(manager.config.shard_count):
                filename = f"events_shard{shard_id}.duckdb"
                if count_shard_replicas(manager.config.data_base, remaining_workers, filename) < 2:
                    return False
            return True

        from duckcluster.waits import wait_until

        wait_until(all_shards_replicated, timeout_sec=90, description="full replication factor")

        assert_result_matches(client.query(sql), expected)
    finally:
        manager.stop()
