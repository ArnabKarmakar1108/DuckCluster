"""Shared pytest fixtures for integration tests."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from duckcluster.cluster import ClusterConfig, ClusterManager
from duckcluster.query import QueryClient
from helpers import BUNDLED_DEMO_CSV, resolve_demo_config, with_cluster

HARNESS_ROOT = Path(__file__).resolve().parent
REPO_ROOT = HARNESS_ROOT.parent.parent


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--demo-csv",
        action="store",
        default=None,
        help=f"Path to source events CSV (default: {BUNDLED_DEMO_CSV})",
    )
    parser.addoption(
        "--demo-rows",
        action="store",
        default=None,
        type=int,
        help="Generate a synthetic CSV with N rows instead of using --demo-csv",
    )


@pytest.fixture(scope="session", autouse=True)
def _build_jars_once(repo_root: Path) -> None:
    subprocess.run(
        ["mvn", "-q", "package", "-DskipTests"],
        cwd=repo_root,
        check=True,
    )
    ClusterManager._jars_built = True


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def harness_root() -> Path:
    return HARNESS_ROOT


@pytest.fixture(scope="session")
def cluster_config(pytestconfig: pytest.Config) -> ClusterConfig:
    demo_csv = pytestconfig.getoption("demo_csv")
    demo_rows = pytestconfig.getoption("demo_rows")
    csv_path = Path(demo_csv) if demo_csv else None
    kwargs = resolve_demo_config(REPO_ROOT, csv_path, demo_rows)
    return ClusterConfig(**kwargs, skip_build=True)


@pytest.fixture(scope="session")
def cluster_manager(cluster_config: ClusterConfig) -> ClusterManager:
    manager = ClusterManager(cluster_config)
    manager.start()
    yield manager
    manager.stop()


@pytest.fixture(scope="session")
def query_client(cluster_manager: ClusterManager) -> QueryClient:
    return QueryClient(cluster_manager.config.http_base_url)


@pytest.fixture(scope="session")
def demo_csv(cluster_manager: ClusterManager) -> Path:
    return cluster_manager.config.data_base / "demo-events.csv"


@pytest.fixture(scope="session")
def with_cluster_config(cluster_config: ClusterConfig):
    return lambda **overrides: with_cluster(cluster_config, **overrides)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "tpch: TPC-H benchmark query integration tests")
