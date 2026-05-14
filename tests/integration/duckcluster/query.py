"""HTTP client for DuckCluster coordinator queries."""

from __future__ import annotations

import requests


class QueryClient:
    def __init__(self, base_url: str, timeout_sec: float = 120.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_sec

    def health(self) -> dict:
        response = requests.get(f"{self._base_url}/v1/cluster/health", timeout=self._timeout)
        response.raise_for_status()
        return response.json()

    def workers(self) -> dict:
        response = requests.get(f"{self._base_url}/v1/cluster/workers", timeout=self._timeout)
        response.raise_for_status()
        return response.json()

    def query(self, sql: str) -> dict:
        response = requests.post(
            f"{self._base_url}/v1/query",
            json={"sql": sql},
            timeout=self._timeout,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"query failed ({response.status_code}): {response.text}")
        return response.json()

    def query_expect_error(self, sql: str) -> int:
        response = requests.post(
            f"{self._base_url}/v1/query",
            json={"sql": sql},
            timeout=self._timeout,
        )
        return response.status_code
