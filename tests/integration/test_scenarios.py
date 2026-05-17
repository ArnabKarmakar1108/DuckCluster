"""Manifest-driven distributed correctness checks."""

from __future__ import annotations

import concurrent.futures as thread_pool
from pathlib import Path

import pytest
import yaml

from duckcluster.baseline import expected_for_demo
from duckcluster.cluster import ClusterManager
from duckcluster.compare import assert_result_matches
from duckcluster.query import QueryClient

HARNESS_ROOT = Path(__file__).resolve().parent


def _load_manifest() -> list[dict]:
    with (HARNESS_ROOT / "manifest.yaml").open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return data["scenarios"]


def _scenario_ids() -> list[str]:
    return [scenario["id"] for scenario in _load_manifest() if "pool_size" not in scenario]


def _scenario_by_id(scenario_id: str) -> dict:
    for scenario in _load_manifest():
        if scenario["id"] == scenario_id:
            return scenario
    raise KeyError(scenario_id)


@pytest.mark.parametrize("scenario_id", _scenario_ids())
def test_manifest_scenario(
    scenario_id: str,
    query_client: QueryClient,
    harness_root: Path,
    demo_csv: Path,
) -> None:
    """Each manifest SQL must match a single-node DuckDB baseline on the demo dataset."""
    scenario = _scenario_by_id(scenario_id)
    sql = (harness_root / scenario["query"]).read_text(encoding="utf-8").strip()
    expected = expected_for_demo(demo_csv, sql)

    actual = query_client.query(sql)
    assert_result_matches(actual, expected)


def test_pool_exhaustion_retry(
    harness_root: Path,
    with_cluster_config,
) -> None:
    """Many concurrent queries still return correct GROUP BY results when worker pools are saturated."""
    scenario = _scenario_by_id("pool_exhaustion_retry")
    sql = (harness_root / scenario["query"]).read_text(encoding="utf-8").strip()

    manager = ClusterManager(
        with_cluster_config(
            coordinator_http_port=28080,
            coordinator_grpc_port=29090,
            worker_ports=(29101, 29102, 29103),
            pool_size=scenario["pool_size"],
            pool_wait_ms=scenario["pool_wait_ms"],
            fragment_wait_ms=90_000,
        )
    )
    manager.start()
    try:
        client = QueryClient(manager.config.http_base_url)
        demo_csv = manager.config.data_base / "demo-events.csv"
        expected = expected_for_demo(demo_csv, sql)
        concurrent = int(scenario.get("concurrent_requests", 8))

        def run_query() -> dict:
            return client.query(sql)

        with thread_pool.ThreadPoolExecutor(max_workers=concurrent) as executor:
            tasks = [executor.submit(run_query) for _ in range(concurrent)]
            results = [future.result() for future in tasks]

        for actual in results:
            assert_result_matches(actual, expected)
    finally:
        manager.stop()
