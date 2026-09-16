"""Integration coverage for production-readiness regressions (section 9.1)."""

from __future__ import annotations

import concurrent.futures as thread_pool
import time
from pathlib import Path

import duckdb
import requests

from duckcluster.baseline import expected_for_demo
from duckcluster.cluster import ClusterManager
from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient
from duckcluster.waits import wait_for_worker_count, wait_until

HARNESS_ROOT = Path(__file__).resolve().parent


def test_coordinator_restart_workers_reregister(harness_root: Path, with_cluster_config) -> None:
    """Workers survive coordinator restart and queries succeed after re-registration."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=39080,
            coordinator_grpc_port=40090,
            worker_ports=(40101, 40102, 40103),
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

        def query_succeeds() -> bool:
            try:
                assert_result_matches(client.query(sql), expected)
                return True
            except (RuntimeError, AssertionError):
                return False

        wait_until(query_succeeds, timeout_sec=90, description="query after coordinator restart")
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


def test_async_group_by_results_paginate_from_spool(cluster_manager: ClusterManager) -> None:
    """Async GROUP BY stores merged rows on disk for pagination (4.6)."""
    base = cluster_manager.config.http_base_url
    sql = "SELECT category, COUNT(*) AS cnt FROM events GROUP BY category ORDER BY category"
    submit = requests.post(f"{base}/v1/query/async", json={"sql": sql}, timeout=30)
    assert submit.status_code == 202
    query_id = submit.json()["queryId"]

    def finished() -> bool:
        status = requests.get(f"{base}/v1/query/{query_id}", timeout=10).json()
        return status.get("status") == "SUCCEEDED"

    wait_until(finished, timeout_sec=120, description="async group by")

    page = requests.get(f"{base}/v1/query/{query_id}/results?offset=0&limit=2", timeout=30).json()
    assert page.get("rows")
    assert page.get("rowCount", len(page["rows"])) >= len(page["rows"])


def test_unknown_table_rejected_by_coordinator(cluster_manager: ClusterManager) -> None:
    """Coordinator validates catalog before dispatch (5.1)."""
    base = cluster_manager.config.http_base_url
    response = requests.post(
        f"{base}/v1/query",
        json={"sql": "SELECT * FROM not_a_real_table"},
        timeout=30,
    )
    assert response.status_code in (400, 422, 503)
    body = response.json()
    assert "error" in body
    assert "not_a_real_table" in body["error"].lower() or "unknown" in body["error"].lower()


def test_wire_types_count_is_integer(cluster_manager: ClusterManager) -> None:
    """COUNT(*) is returned as a JSON number, not a string (wire type 1.3)."""
    client = QueryClient(cluster_manager.config.http_base_url)
    result = client.query("SELECT COUNT(*) AS cnt FROM events")
    value = result["rows"][0][0]
    assert isinstance(value, int)
    assert value > 0


def test_http_staged_shard_ingest_and_activate(harness_root: Path, with_cluster_config) -> None:
    """PUT staged shard bytes to coordinator, then activate version (5.2)."""
    import duckdb

    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38980,
            coordinator_grpc_port=39990,
            worker_ids=("worker-1", "worker-2"),
            worker_ports=(40001, 40002),
            shard_count=1,
            replication_factor=1,
            data_mode="empty",
        )
    )
    manager.start()
    try:
        base = manager.config.http_base_url
        requests.post(
            f"{base}/v1/catalog/tables",
            json={"name": "events", "shardCount": 1, "shardKey": "id"},
            timeout=30,
        )
        staging = manager.config.data_base / "ingest-staging"
        staging.mkdir(parents=True, exist_ok=True)
        shard_path = staging / "events_shard0.duckdb"
        if shard_path.exists():
            shard_path.unlink()
        connection = duckdb.connect(str(shard_path))
        connection.execute("CREATE TABLE events (id INTEGER, zip VARCHAR)")
        connection.execute("INSERT INTO events VALUES (1, '02134')")
        connection.close()
        payload = shard_path.read_bytes()

        put = requests.put(
            f"{base}/v1/catalog/tables/events/versions/1/shards/0",
            data=payload,
            headers={"Content-Type": "application/octet-stream"},
            timeout=120,
        )
        assert put.status_code == 201

        activate = requests.post(f"{base}/v1/catalog/tables/events/versions/1/activate", timeout=60)
        assert activate.status_code == 200

        def shard_zero_has_owner() -> bool:
            try:
                shards = requests.get(f"{base}/v1/monitor/shards", timeout=10).json()
                for table in shards.get("tables") or []:
                    if table.get("tableName") != "events":
                        continue
                    for shard in table.get("shards") or []:
                        if shard.get("shardId") == 0 and shard.get("owners"):
                            return True
                return False
            except Exception:
                return False

        wait_until(shard_zero_has_owner, timeout_sec=90, description="events_shard0 ownership")

        client = QueryClient(base)
        row = client.query("SELECT zip FROM events WHERE id = 1")["rows"][0][0]
        assert row == "02134"
    finally:
        manager.stop()


def test_staged_table_version_swap(harness_root: Path, with_cluster_config) -> None:
    """Activating a staged shard set replaces live data without restarting workers."""
    import duckdb

    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38880,
            coordinator_grpc_port=39890,
            worker_ports=(39901, 39902, 39903),
            shard_count=1,
            replication_factor=1,
            data_mode="primary_only_shard0",
        )
    )
    manager.start()
    try:
        base = manager.config.http_base_url
        client = QueryClient(base)
        requests.post(
            f"{base}/v1/catalog/tables",
            json={"name": "events", "shardCount": 1, "shardKey": "id"},
            timeout=30,
        )
        before = client.query("SELECT COUNT(*) AS cnt FROM events")["rows"][0][0]
        assert int(before) > 1

        owner_dir = None
        for worker_id in manager.config.worker_ids:
            shard_path = manager.worker_data_dir(worker_id) / "events_shard0.duckdb"
            if shard_path.is_file():
                owner_dir = manager.worker_data_dir(worker_id)
                break
        assert owner_dir is not None

        staging = owner_dir / ".staging" / "v1"
        staging.mkdir(parents=True, exist_ok=True)
        staged = staging / "events_shard0.duckdb"
        connection = duckdb.connect(str(staged))
        connection.execute("CREATE TABLE events (id INTEGER, category VARCHAR, score DOUBLE)")
        connection.execute("INSERT INTO events VALUES (1, 'solo', 1.0)")
        connection.close()

        response = requests.post(
            f"{base}/v1/catalog/tables/events/versions/1/activate",
            timeout=60,
        )
        assert response.status_code == 200
        body = response.json()
        assert body["version"] == 1
        assert body["shardsActivated"] >= 1

        after = client.query("SELECT COUNT(*) AS cnt FROM events")["rows"][0][0]
        assert int(after) == 1
    finally:
        manager.stop()


def test_pin_cleanup_failure_reaper_releases_leased_pins(with_cluster_config) -> None:
    """Failed EndShardPin during query cleanup does not leak pins past the lease (1.2 / 9.1)."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38680,
            coordinator_grpc_port=39690,
            worker_ports=(39711, 39712, 39713),
            cache_max_shards=1,
            shard_pin_lease_sec=2,
            worker_extra_env={
                "DUCKCLUSTER_TEST_FAIL_END_SHARD_PIN": "1",
                "DUCKCLUSTER_PIN_REAPER_INTERVAL_MS": "500",
            },
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        join_sql = (
            "SELECT e1.category, COUNT(*) AS cnt "
            "FROM events e1 JOIN events e2 ON e1.id = e2.id "
            "GROUP BY e1.category ORDER BY e1.category"
        )
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, join_sql)
        assert_result_matches(client.query(join_sql), expected)
        time.sleep(3.5)
        assert_result_matches(client.query(join_sql), expected)
    finally:
        manager.stop()


def test_group_by_distinguishes_null_and_empty_string(with_cluster_config) -> None:
    """Merged GROUP BY keeps NULL separate from empty string on VARCHAR (1.3 / 9.1)."""
    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=38580,
            coordinator_grpc_port=39590,
            worker_ids=("worker-1", "worker-2"),
            worker_ports=(39611, 39612),
            shard_count=2,
            replication_factor=1,
            data_mode="empty",
        )
    )
    manager.start()
    try:
        base = manager.config.http_base_url
        requests.post(
            f"{base}/v1/catalog/tables",
            json={"name": "events", "shardCount": 2, "shardKey": "id"},
            timeout=30,
        )
        staging = manager.config.data_base / "null-label-shards"
        staging.mkdir(parents=True, exist_ok=True)
        for shard_id, label in ((0, None), (1, "")):
            shard_path = staging / f"events_shard{shard_id}.duckdb"
            if shard_path.exists():
                shard_path.unlink()
            connection = duckdb.connect(str(shard_path))
            connection.execute("CREATE TABLE events (id INTEGER, label VARCHAR)")
            if label is None:
                connection.execute("INSERT INTO events VALUES (1, NULL)")
            else:
                connection.execute("INSERT INTO events VALUES (2, ?)", [label])
            connection.close()
            worker_dir = manager.worker_data_dir(f"worker-{shard_id + 1}")
            worker_dir.mkdir(parents=True, exist_ok=True)
            dest = worker_dir / f"events_shard{shard_id}.duckdb"
            if dest.exists():
                dest.unlink()
            dest.write_bytes(shard_path.read_bytes())

        reconcile = requests.post(f"{base}/v1/cluster/reconcile", timeout=120)
        assert reconcile.status_code in (200, 202)

        client = QueryClient(base)

        def two_groups() -> bool:
            try:
                rows = client.query("SELECT label, COUNT(*) AS cnt FROM events GROUP BY label")["rows"]
                return len(rows) == 2
            except Exception:
                return False

        wait_until(two_groups, timeout_sec=90, description="NULL and empty string groups")

        rows = client.query("SELECT label, COUNT(*) AS cnt FROM events GROUP BY label")["rows"]
        labels = {row[0] for row in rows}
        assert labels == {None, ""}
    finally:
        manager.stop()
