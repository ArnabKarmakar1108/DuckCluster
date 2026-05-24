"""TPC-H correctness tests on small-scale benchmark data (SF0.01)."""

from __future__ import annotations

from pathlib import Path

import pytest

from duckcluster.baseline import expected_for_tpch
from duckcluster.cluster import ClusterConfig, ClusterManager
from duckcluster.compare import assert_result_matches, assert_tpch_result_matches
from duckcluster.query import QueryClient
from duckcluster.tpch import (
    ALL_QUERIES,
    PASSING_QUERIES,
    PHASE1_PASSING,
    PHASE2_PASSING,
    PHASE3_PASSING,
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
    """Passing TPC-H queries must match single-node baseline on SF0.01."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", PHASE1_PASSING)
def test_tpch_phase1_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Backward-compatible alias for Phase 1 passing queries."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", PHASE2_PASSING)
def test_tpch_phase2_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Phase 2 queries with derived tables in FROM."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)


@pytest.mark.tpch
@pytest.mark.parametrize("query_id", PHASE3_PASSING)
def test_tpch_phase3_correctness(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Phase 3 queries with comma-joins and table aliases."""
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


@pytest.mark.tpch
@pytest.mark.xfail(reason="Pending later planner phases", strict=False)
@pytest.mark.parametrize(
    "query_id",
    [q for q in ALL_QUERIES if q not in PASSING_QUERIES],
)
def test_tpch_full_correctness_pending(
    query_id: str,
    tpch_query_client: QueryClient,
    tpch_baseline_db: Path,
) -> None:
    """Track remaining queries; remove xfail as planner phases land."""
    sql = load_tpch_sql(query_id)
    expected = expected_for_tpch(tpch_baseline_db, sql)
    actual = tpch_query_client.query(sql)
    assert_tpch_result_matches(actual, expected)
