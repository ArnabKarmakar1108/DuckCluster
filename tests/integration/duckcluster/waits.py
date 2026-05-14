"""Polling helpers for integration scenarios."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

from duckcluster.query import QueryClient


def wait_until(
    predicate: Callable[[], bool],
    timeout_sec: float = 60.0,
    interval_sec: float = 0.5,
    description: str = "condition",
) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval_sec)
    raise TimeoutError(f"Timed out waiting for {description}")


def wait_for_worker_count(client: QueryClient, expected: int, timeout_sec: float = 60.0) -> None:
    def ready() -> bool:
        try:
            workers = client.workers().get("workers", [])
            return len(workers) >= expected
        except Exception:
            return False

    wait_until(ready, timeout_sec=timeout_sec, description=f"{expected} registered workers")


def wait_for_worker_absent(client: QueryClient, worker_id: str, timeout_sec: float = 60.0) -> None:
    def gone() -> bool:
        try:
            workers = client.workers().get("workers", [])
            return all(worker["workerId"] != worker_id for worker in workers)
        except Exception:
            return False

    wait_until(gone, timeout_sec=timeout_sec, description=f"worker {worker_id} removal")


def wait_for_shard_file(
    data_dir: Path,
    shard_filename: str,
    timeout_sec: float = 90.0,
) -> None:
    target = data_dir / shard_filename

    def exists() -> bool:
        return target.is_file() and target.stat().st_size > 0

    wait_until(exists, timeout_sec=timeout_sec, description=f"{shard_filename} on {data_dir}")


def count_shard_replicas(data_base: Path, worker_ids: list[str], shard_filename: str) -> int:
    return sum(1 for worker_id in worker_ids if (data_base / worker_id / shard_filename).is_file())
