"""Process lifecycle helpers for integration tests."""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import requests

from duckcluster.demo import DEFAULT_DEMO_ROW_COUNT, write_demo_csv
from duckcluster.shards import build_shard_db, ring_assignments


@dataclass
class ClusterConfig:
    root: Path
    coordinator_http_port: int = 18080
    coordinator_grpc_port: int = 19090
    coordinator_host: str = "127.0.0.1"
    shard_count: int = 6
    replication_factor: int = 2
    worker_ids: tuple[str, ...] = ("worker-1", "worker-2", "worker-3")
    worker_ports: tuple[int, ...] = (19101, 19102, 19103)
    pool_size: int | None = None
    pool_wait_ms: int | None = None
    fragment_wait_ms: int | None = None
    heartbeat_interval_sec: int = 2
    heartbeat_miss_threshold: int = 3
    watcher_interval_ms: int = 1000
    data_mode: Literal["distributed", "primary_only_shard0", "empty", "tpch"] = "distributed"
    demo_csv: Path | None = None
    demo_row_count: int | None = None
    tpch_data_dir: Path | None = None
    skip_build: bool = False

    @property
    def http_base_url(self) -> str:
        return f"http://{self.coordinator_host}:{self.coordinator_http_port}"

    @property
    def data_base(self) -> Path:
        return self.root / "data" / "integration" / str(self.coordinator_http_port)


class ClusterManager:
    _jars_built = False

    def __init__(self, config: ClusterConfig) -> None:
        self.config = config
        self._coordinator: subprocess.Popen | None = None
        self._workers: dict[str, subprocess.Popen] = {}

    def start(self) -> None:
        self._build_jars()
        self._prepare_data()
        env = self._base_env()
        coordinator_jar = (
            self.config.root / "coordinator/target/duckcluster-coordinator-0.1.0-SNAPSHOT.jar"
        )
        worker_jar = self.config.root / "worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar"

        self._coordinator = self._spawn(["java", "-jar", str(coordinator_jar)], env)
        self._wait_for_coordinator()

        for worker_id, port in zip(self.config.worker_ids, self.config.worker_ports):
            self._start_worker_process(worker_id, port, worker_jar, env)

        self._wait_for_workers(len(self.config.worker_ids))

    def stop(self) -> None:
        for process in reversed(list(self._workers.values())):
            self._terminate(process)
        if self._coordinator is not None:
            self._terminate(self._coordinator)
        self._workers.clear()
        self._coordinator = None

    def kill_worker(self, worker_id: str) -> None:
        process = self._workers.get(worker_id)
        if process is None or process.poll() is not None:
            raise RuntimeError(f"Worker {worker_id} is not running")
        process.kill()
        process.wait(timeout=5)
        del self._workers[worker_id]

    def add_worker(self, worker_id: str, port: int) -> None:
        if worker_id in self._workers:
            raise RuntimeError(f"Worker {worker_id} already running")
        worker_jar = self.config.root / "worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar"
        worker_dir = self.config.data_base / worker_id
        worker_dir.mkdir(parents=True, exist_ok=True)
        env = self._base_env()
        self._start_worker_process(worker_id, port, worker_jar, env)
        self._wait_for_workers(len(self._workers), timeout_sec=60)

    def place_shard_file(self, worker_id: str, shard_path: Path) -> Path:
        target_dir = self.config.data_base / worker_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / shard_path.name
        target.write_bytes(shard_path.read_bytes())
        return target

    def worker_data_dir(self, worker_id: str) -> Path:
        return self.config.data_base / worker_id

    def _start_worker_process(
        self,
        worker_id: str,
        port: int,
        worker_jar: Path,
        env: dict[str, str],
    ) -> None:
        worker_env = env.copy()
        worker_env["DUCKCLUSTER_DATA_DIR"] = str(self.config.data_base / worker_id)
        worker_env["DUCKCLUSTER_WATCHER_INTERVAL_MS"] = str(self.config.watcher_interval_ms)
        if self.config.pool_size is not None:
            worker_env["DUCKCLUSTER_POOL_SIZE"] = str(self.config.pool_size)
        if self.config.pool_wait_ms is not None:
            worker_env["DUCKCLUSTER_POOL_WAIT_MS"] = str(self.config.pool_wait_ms)
        self._workers[worker_id] = self._spawn(
            ["java", "-jar", str(worker_jar), worker_id, self.config.coordinator_host, str(port)],
            worker_env,
        )

    def _build_jars(self) -> None:
        if self.config.skip_build and ClusterManager._jars_built:
            return
        subprocess.run(
            ["mvn", "-q", "package", "-DskipTests"],
            cwd=self.config.root,
            check=True,
        )
        ClusterManager._jars_built = True

    def _prepare_data(self) -> None:
        worker_dirs = [str(self.config.data_base / worker_id) for worker_id in self.config.worker_ids]
        for worker_dir in worker_dirs:
            path = Path(worker_dir)
            path.mkdir(parents=True, exist_ok=True)
            for shard_file in path.glob("*.duckdb"):
                shard_file.unlink()

        if self.config.data_mode == "empty":
            return

        if self.config.data_mode == "tpch":
            self._prepare_tpch_data(worker_dirs)
            return

        demo_source = self._ensure_demo_csv()

        if self.config.data_mode == "primary_only_shard0":
            self._prepare_primary_only_shard0(demo_source, worker_dirs)
            return

        split_script = self.config.root / "scripts/split-and-distribute.sh"
        if shutil.which("duckdb") is not None and split_script.exists():
            subprocess.run(
                [
                    str(split_script),
                    "--source",
                    str(demo_source),
                    "--table",
                    "events",
                    "--key",
                    "id",
                    "--shards",
                    str(self.config.shard_count),
                    "--workers",
                    ",".join(self.config.worker_ids),
                    "--dirs",
                    ",".join(worker_dirs),
                    "--rf",
                    str(self.config.replication_factor),
                ],
                cwd=self.config.root,
                check=True,
            )
            return

        self._prepare_data_with_python(demo_source, worker_dirs)

    def _ensure_demo_csv(self) -> Path:
        demo_source = self.config.data_base / "demo-events.csv"
        demo_source.parent.mkdir(parents=True, exist_ok=True)

        if self.config.demo_csv is not None:
            shutil.copy2(self.config.demo_csv, demo_source)
            return demo_source

        if self.config.demo_row_count is not None:
            write_demo_csv(demo_source, self.config.demo_row_count)
            return demo_source

        bundled = self.config.root / "tests" / "integration" / "data" / "demo-events.csv"
        if bundled.is_file():
            shutil.copy2(bundled, demo_source)
            return demo_source

        write_demo_csv(demo_source, DEFAULT_DEMO_ROW_COUNT)
        return demo_source

    def _prepare_primary_only_shard0(self, demo_source: Path, worker_dirs: list[str]) -> None:
        assignments = ring_assignments(
            self.config.root,
            list(self.config.worker_ids),
            "events",
            1,
            self.config.replication_factor,
        )
        shard_path = build_shard_db(
            demo_source,
            self.config.data_base / "staging",
            "events",
            0,
            1,
        )
        primary_worker = assignments["events_shard0"][0]
        primary_index = self.config.worker_ids.index(primary_worker)
        target = Path(worker_dirs[primary_index]) / shard_path.name
        target.write_bytes(shard_path.read_bytes())

    def _prepare_data_with_python(self, demo_source: Path, worker_dirs: list[str]) -> None:
        import duckdb

        assignments = ring_assignments(
            self.config.root,
            list(self.config.worker_ids),
            "events",
            self.config.shard_count,
            self.config.replication_factor,
        )

        staging = self.config.data_base / "staging"
        staging.mkdir(parents=True, exist_ok=True)
        for shard_id in range(self.config.shard_count):
            shard_key = f"events_shard{shard_id}"
            shard_path = staging / f"{shard_key}.duckdb"
            if shard_path.exists():
                shard_path.unlink()
            connection = duckdb.connect(str(shard_path))
            connection.execute(
                f"""
                CREATE TABLE events AS
                SELECT * FROM read_csv_auto('{demo_source}')
                WHERE abs(hash(CAST(id AS VARCHAR))) % {self.config.shard_count} = {shard_id}
                """
            )
            connection.close()

            for worker_id in assignments[shard_key]:
                worker_index = self.config.worker_ids.index(worker_id)
                target = Path(worker_dirs[worker_index]) / f"{shard_key}.duckdb"
                target.write_bytes(shard_path.read_bytes())

    def _prepare_tpch_data(self, worker_dirs: list[str]) -> None:
        if self.config.tpch_data_dir is None:
            raise ValueError("tpch_data_dir is required when data_mode='tpch'")
        source_workers = self.config.tpch_data_dir / "workers"
        if not source_workers.is_dir():
            raise FileNotFoundError(f"TPC-H worker data not found: {source_workers}")

        for worker_id, worker_dir in zip(self.config.worker_ids, worker_dirs, strict=True):
            source_dir = source_workers / worker_id
            if not source_dir.is_dir():
                raise FileNotFoundError(f"TPC-H data missing for {worker_id}: {source_dir}")
            target_dir = Path(worker_dir)
            for shard_file in source_dir.glob("*.duckdb"):
                shutil.copy2(shard_file, target_dir / shard_file.name)

    def _base_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["DUCKCLUSTER_COORDINATOR_HOST"] = self.config.coordinator_host
        env["DUCKCLUSTER_COORDINATOR_HTTP_PORT"] = str(self.config.coordinator_http_port)
        env["DUCKCLUSTER_COORDINATOR_GRPC_PORT"] = str(self.config.coordinator_grpc_port)
        env["DUCKCLUSTER_SHARD_COUNT"] = str(self.config.shard_count)
        env["DUCKCLUSTER_REPLICATION_FACTOR"] = str(self.config.replication_factor)
        env["DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC"] = str(self.config.heartbeat_interval_sec)
        env["DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD"] = str(self.config.heartbeat_miss_threshold)
        if self.config.fragment_wait_ms is not None:
            env["DUCKCLUSTER_FRAGMENT_WAIT_MS"] = str(self.config.fragment_wait_ms)
        return env

    def _spawn(self, command: list[str], env: dict[str, str]) -> subprocess.Popen:
        log_dir = self.config.data_base / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        label = Path(command[-1]).stem if command else "process"
        log_file = open(log_dir / f"{label}.log", "w", encoding="utf-8")
        return subprocess.Popen(
            command,
            cwd=self.config.root,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

    def _terminate(self, process: subprocess.Popen) -> None:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def _wait_for_coordinator(self) -> None:
        deadline = time.time() + 45
        while time.time() < deadline:
            try:
                response = requests.get(f"{self.config.http_base_url}/v1/cluster/health", timeout=1)
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise RuntimeError(
            f"Coordinator did not become healthy in time ({self.config.http_base_url})"
        )

    def _wait_for_workers(self, expected: int, timeout_sec: float = 45) -> None:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            try:
                response = requests.get(f"{self.config.http_base_url}/v1/cluster/workers", timeout=2)
                response.raise_for_status()
                workers = response.json().get("workers", [])
                if len(workers) >= expected:
                    time.sleep(2)
                    return
            except requests.RequestException:
                pass
            time.sleep(1)
        raise RuntimeError(f"Expected {expected} workers, cluster did not become ready")
