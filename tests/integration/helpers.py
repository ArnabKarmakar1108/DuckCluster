"""Shared helpers for integration test modules."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from duckcluster.cluster import ClusterConfig

HARNESS_ROOT = Path(__file__).resolve().parent
BUNDLED_DEMO_CSV = HARNESS_ROOT / "data" / "demo-events.csv"


def resolve_demo_config(repo_root: Path, demo_csv: Path | None, demo_rows: int | None) -> dict:
    if demo_rows is not None:
        return {"root": repo_root, "demo_row_count": demo_rows}
    source = demo_csv.resolve() if demo_csv is not None else BUNDLED_DEMO_CSV
    return {"root": repo_root, "demo_csv": source}


def with_cluster(cluster_config: ClusterConfig, **overrides: object) -> ClusterConfig:
    return replace(cluster_config, **overrides)
