"""Consistent-hash ring assignment properties."""

from __future__ import annotations

import json
import subprocess
from collections import Counter
from pathlib import Path


def test_hash_ring_distribution_and_determinism(repo_root: Path) -> None:
    """Ring assignment is deterministic and spreads primary shard ownership evenly across workers."""
    subprocess.run(
        ["mvn", "-q", "-pl", "common", "compile"],
        cwd=repo_root,
        check=True,
    )

    command = [
        "java",
        "-cp",
        str(repo_root / "common/target/classes"),
        "io.duckcluster.common.routing.RingAssignerCli",
        "--workers",
        "worker-1,worker-2,worker-3,worker-4,worker-5",
        "--table",
        "events",
        "--shards",
        "100",
        "--rf",
        "2",
        "--vnodes",
        "100",
    ]

    first = _run_assigner(command)
    second = _run_assigner(command)
    assert first == second, "Ring assignment must be deterministic"

    primary_counts = Counter(owner[0] for owner in first.values())
    assert len(primary_counts) == 5
    for count in primary_counts.values():
        assert count <= 30, f"worker received {count}% of primary assignments"


def _run_assigner(command: list[str]) -> dict[str, list[str]]:
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)
