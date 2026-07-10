"""HTTP client for the DuckCluster coordinator REST API."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


class ClusterError(RuntimeError):
    """Coordinator returned an HTTP error."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class ConnectionError(RuntimeError):
    """Could not reach the coordinator."""


@dataclass(frozen=True)
class ClientConfig:
    base_url: str
    timeout_sec: float = 300.0


class CoordinatorClient:
    def __init__(self, config: ClientConfig) -> None:
        self._base_url = config.base_url.rstrip("/")
        self._timeout_sec = config.timeout_sec

    def health(self) -> dict[str, Any]:
        return self._get("/v1/cluster/health")

    def workers(self) -> dict[str, Any]:
        return self._get("/v1/cluster/workers")

    def shards(self) -> dict[str, Any]:
        return self._get("/v1/monitor/shards")

    def query(self, sql: str) -> dict[str, Any]:
        return self._post("/v1/query", {"sql": sql})

    def _get(self, path: str) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self._base_url}{path}",
            method="GET",
            headers={"Accept": "application/json"},
        )
        return self._send(request)

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        payload = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base_url}{path}",
            data=payload,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        return self._send(request)

    def _send(self, request: urllib.request.Request) -> dict[str, Any]:
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_sec) as response:
                raw = response.read().decode("utf-8")
                if not raw:
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(message)
                if isinstance(parsed, dict) and "error" in parsed:
                    message = str(parsed["error"])
            except json.JSONDecodeError:
                pass
            raise ClusterError(exc.code, message) from exc
        except urllib.error.URLError as exc:
            raise ConnectionError(f"cannot reach coordinator at {self._base_url}: {exc.reason}") from exc
