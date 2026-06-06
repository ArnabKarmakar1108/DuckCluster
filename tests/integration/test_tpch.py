"""TPC-H correctness tests on small-scale benchmark data (SF0.01)."""

from __future__ import annotations

from pathlib import Path

import pytest

from duckcluster.baseline import expected_for_tpch
from duckcluster.cluster import ClusterConfig, ClusterManager
from duckcluster.compare import assert_tpch_result_matches
from duckcluster.query import QueryClient
from duckcluster.tpch import (
    ALL_QUERIES,
    COLOCATED_EXISTS_PASSING,
    COMMA_JOIN_ALIAS_PASSING,
    CORRELATED_SCALAR_SUBQUERY_PASSING,
    COUNT_DISTINCT_PASSING,
    DERIVED_TABLE_FROM_PASSING,
    EXPRESSION_AGGREGATE_PASSING,
    FULL_CORRECTNESS_PASSING,
    NESTED_AGGREGATE_ARITHMETIC_PASSING,
    PASSING_QUERIES,
    RESERVED_ALIAS_PASSING,
    TOPK_AGGREGATE_ALIAS_PASSING,
    UNCORRELATED_SUBQUERY_PASSING,
    WITH_CTE_MERGE_PASSING,
    ensure_tpch_data,
    load_tpch_sql,
)

# Separate ports from the default demo cluster session fixture.
TPCH_HTTP_PORT = 38080
TPCH_GRPC_PORT = 39090
TPCH_WORKER_PORTS = (39101, 39102, 39103)


@pytest.fixture(scope="session")
def tpch_data_dir(repo_root: Path) -> Path:
    return ensure_tpch_data(repo_root, scale_factor=0.01)


@pytest.fixture(scope="session")
def tpch_baseline_db(tpch_data_dir: Path) -> Path:
    baseline = tpch_data_dir / "baseline.duckdb"
    if not baseline.is_file():
        raise FileNotFoundError(f"TPC-H baseline database not found: {baseline}")
    return baseline


@pytest.fixture(scope="session")
def tpch_cluster(repo_root: Path, tpch_data_dir: Path) -> ClusterManager:
    manager = ClusterManager(
        ClusterConfig(
            root=repo_root,
            coordinator_http_port=TPCH_HTTP_PORT,
            coordinator_grpc_port=TPCH_GRPC_PORT,
            worker_ports=TPCH_WORKER_PORTS,
            data_mode="tpch",
            tpch_data_dir=tpch_data_dir,
            skip_build=True,
        )
    )
    manager.start()
    yield manager
    manager.stop()


@pytest.fixture(scope="session")
def tpch_query_client(tpch_cluster: ClusterManager) -> QueryClient:
    return QueryClient(tpch_cluster.config.http_base_url, timeout_sec=180.0)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", PASSING_QUERIES)
def test_tpch_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """All TPC-H queries must match single-node baseline on SF0.01."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", EXPRESSION_AGGREGATE_PASSING)
def test_tpch_expression_aggregate_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with expression aggregates on lineitem."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", DERIVED_TABLE_FROM_PASSING)
def test_tpch_derived_table_from_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with derived tables in FROM."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", COMMA_JOIN_ALIAS_PASSING)
def test_tpch_comma_join_alias_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with comma-joins and table aliases."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", NESTED_AGGREGATE_ARITHMETIC_PASSING)
def test_tpch_nested_aggregate_arithmetic_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with arithmetic over nested aggregates."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", RESERVED_ALIAS_PASSING)
def test_tpch_reserved_alias_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with reserved-word aliases."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", COUNT_DISTINCT_PASSING)
def test_tpch_count_distinct_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with COUNT(DISTINCT ...) global merge."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", UNCORRELATED_SUBQUERY_PASSING)
def test_tpch_uncorrelated_subquery_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with uncorrelated predicate subqueries."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", COLOCATED_EXISTS_PASSING)
def test_tpch_colocated_exists_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with correlated EXISTS/NOT EXISTS and co-located shards."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", CORRELATED_SCALAR_SUBQUERY_PASSING)
def test_tpch_correlated_scalar_subquery_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with correlated scalar subqueries and derived-table predicates."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", WITH_CTE_MERGE_PASSING)
def test_tpch_with_cte_merge_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with WITH/CTE coordinator two-step merge."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", TOPK_AGGREGATE_ALIAS_PASSING)
def test_tpch_topk_aggregate_alias_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Queries with TOP_K ORDER BY on aggregate aliases."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", FULL_CORRECTNESS_PASSING)
def test_tpch_full_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Full 22-query gate: planning, fragment execution, and merge succeed."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", ALL_QUERIES)
def test_tpch_query_files_load(query_id: str) -> None:
    """All 22 TPC-H query files are present and non-empty."""
    sql = load_tpch_sql(query_id)
    stripped = sql.lstrip().upper()
    assert stripped.startswith("SELECT") or stripped.startswith("WITH"), (
        f"{query_id} must be a SELECT or WITH query"
    )
