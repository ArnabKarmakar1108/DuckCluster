"""Shard discovery, replication, and shard-scoped query correctness."""

from __future__ import annotations

from pathlib import Path

from duckcluster.baseline import expected_for_demo
from duckcluster.cluster import ClusterManager
from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient
from duckcluster.shards import build_shard_db, ids_for_shard, ring_assignments
from duckcluster.waits import wait_for_shard_file

HARNESS_ROOT = Path(__file__).resolve().parent


def _shard_zero_sql(demo_csv: Path, shard_count: int) -> str:
    shard_ids = ids_for_shard(demo_csv, 0, shard_count)
    id_list = ", ".join(str(row_id) for row_id in shard_ids)
    return (
        "SELECT id, name, score, category "
        f"FROM events WHERE id IN ({id_list}) ORDER BY id"
    )


def test_hot_shard_auto_replication(harness_root: Path, with_cluster_config) -> None:
    """A shard placed on only one worker is eventually copied to its hash-ring replica."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=48080,
            coordinator_grpc_port=49090,
            worker_ids=("worker-1", "worker-2"),
            worker_ports=(49101, 49102),
            shard_count=1,
            replication_factor=2,
            data_mode="primary_only_shard0",
            heartbeat_interval_sec=1,
            heartbeat_miss_threshold=2,
            watcher_interval_ms=500,
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        sql = (harness_root / "queries/replication/hot_shard_count.sql").read_text(encoding="utf-8").strip()
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, sql)

        assignments = ring_assignments(
            manager.config.root,
            ["worker-1", "worker-2"],
            "events",
            1,
            2,
        )
        replica_targets = [
            worker for worker in assignments["events_shard0"] if worker != assignments["events_shard0"][0]
        ]
        assert replica_targets, "expected a replica target worker from the hash ring"

        for replica_worker in replica_targets:
            wait_for_shard_file(
                manager.worker_data_dir(replica_worker),
                "events_shard0.duckdb",
                timeout_sec=90,
            )

        assert_result_matches(client.query(sql), expected)
    finally:
        manager.stop()


def test_shard_scoped_filter_matches_single_shard(harness_root: Path, with_cluster_config) -> None:
    """Filtering to shard-0 ids returns the same rows as querying those ids on the full dataset."""
    manager = ClusterManager(with_cluster_config(coordinator_http_port=48180, coordinator_grpc_port=49190, worker_ports=(49201, 49202, 49203)))
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        demo_csv = manager.config.data_base / "demo-events.csv"
        sql = _shard_zero_sql(demo_csv, manager.config.shard_count)
        expected = expected_for_demo(demo_csv, sql)
        assert_result_matches(client.query(sql), expected)
    finally:
        manager.stop()


def test_manual_hot_shard_placement_triggers_replication(harness_root: Path, with_cluster_config) -> None:
    """Copying a new shard file onto a running worker triggers watcher detection and replication."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=48280,
            coordinator_grpc_port=49290,
            worker_ids=("worker-1", "worker-2"),
            worker_ports=(49301, 49302),
            shard_count=1,
            replication_factor=2,
            data_mode="empty",
            heartbeat_interval_sec=1,
            heartbeat_miss_threshold=2,
            watcher_interval_ms=500,
        )
    )
    demo_csv = manager.config.data_base / "demo-events.csv"
    demo_csv.parent.mkdir(parents=True, exist_ok=True)
    manager._ensure_demo_csv()
    shard_path = build_shard_db(demo_csv, manager.config.data_base / "staging", "events", 0, 1)

    manager.start()
    try:
        manager.place_shard_file("worker-1", shard_path)

        assignments = ring_assignments(
            manager.config.root,
            ["worker-1", "worker-2"],
            "events",
            1,
            2,
        )
        replica_worker = [worker for worker in assignments["events_shard0"] if worker != "worker-1"][0]

        wait_for_shard_file(
            manager.worker_data_dir(replica_worker),
            "events_shard0.duckdb",
            timeout_sec=90,
        )

        client = QueryClient(manager.config.http_base_url)
        sql = (harness_root / "queries/replication/hot_shard_count.sql").read_text(encoding="utf-8").strip()
        expected = expected_for_demo(demo_csv, sql)
        assert_result_matches(client.query(sql), expected)
    finally:
        manager.stop()
