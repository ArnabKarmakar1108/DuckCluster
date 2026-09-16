"""TPC-H correctness when the fact tables are NOT co-partitioned (4.3 shuffle joins).

``orders`` is split 4 ways by ``o_custkey`` while ``lineitem`` keeps 6 shards by ``l_orderkey``.
Joins on the order key must re-bucket ``orders`` (shuffle) instead of slicing it by shard, and
correlated EXISTS over ``lineitem`` must not assume shard-local co-location.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from duckcluster.baseline import expected_for_tpch
from duckcluster.cluster import ClusterConfig, ClusterManager
from duckcluster.compare import assert_tpch_result_matches
from duckcluster.query import QueryClient
from duckcluster.tpch import ALL_QUERIES, declare_tpch_tables, ensure_tpch_data, explain, load_tpch_sql

SHUFFLE_HTTP_PORT = 37080
SHUFFLE_GRPC_PORT = 37090
SHUFFLE_WORKER_PORTS = (37101, 37102, 37103)
ORDERS_BY_CUSTOMER = "orders=o_custkey:4"


@pytest.fixture(scope="module")
def shuffle_data_dir(repo_root: Path) -> Path:
    return ensure_tpch_data(
        repo_root,
        scale_factor=0.01,
        data_dir=repo_root / "benchmark" / "data" / "sf0.01-orders-by-custkey",
        partitions=(ORDERS_BY_CUSTOMER,),
    )


@pytest.fixture(scope="module")
def shuffle_cluster(repo_root: Path, shuffle_data_dir: Path) -> ClusterManager:
    manager = ClusterManager(
        ClusterConfig(
            root=repo_root,
            coordinator_http_port=SHUFFLE_HTTP_PORT,
            coordinator_grpc_port=SHUFFLE_GRPC_PORT,
            worker_ports=SHUFFLE_WORKER_PORTS,
            data_mode="tpch",
            tpch_data_dir=shuffle_data_dir,
        )
    )
    manager.start()
    declare_tpch_tables(manager.config.http_base_url, shuffle_data_dir)
    yield manager
    manager.stop()


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", ALL_QUERIES)
def test_tpch_correct_with_differently_partitioned_orders(
    query_id: str, shuffle_cluster: ClusterManager, shuffle_data_dir: Path
) -> None:
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(shuffle_data_dir / "baseline.duckdb", sql)
    actual = QueryClient(shuffle_cluster.config.http_base_url, timeout_sec=180.0).query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", ("q03", "q05", "q10", "q12", "q18"))
def test_order_key_joins_shuffle_orders(query_id: str, shuffle_cluster: ClusterManager) -> None:
    plan = explain(shuffle_cluster.config.http_base_url, load_tpch_sql(query_id))
    assert plan["table"] == "lineitem", plan
    shuffled = {view["table"]: view for view in plan["shuffleJoinTables"]}
    assert shuffled["orders"]["joinColumn"] == "o_orderkey", plan
    assert shuffled["orders"]["bucketCount"] == 6, plan
    assert "orders" not in plan["coPartitionJoinTables"], plan


@pytest.mark.tpch
def test_correlated_exists_is_not_co_located_across_layouts(shuffle_cluster: ClusterManager) -> None:
    plan = explain(shuffle_cluster.config.http_base_url, load_tpch_sql("q04"))
    assert plan["table"] == "orders", plan
    assert plan["correlatedCoPartitionTables"] == [], plan
