"""Pre-flight checks before running distributed queries."""

from __future__ import annotations

import re
from typing import Iterable

from duckcluster.client import CoordinatorClient

_TABLE_PATTERN = re.compile(
    r"(?:\bFROM|\bJOIN)\s+(?:ONLY\s+)?([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)


class ValidationError(RuntimeError):
    """Cluster state or SQL prerequisites are not satisfied."""


def extract_table_names(sql: str) -> list[str]:
    seen: set[str] = set()
    tables: list[str] = []
    for match in _TABLE_PATTERN.finditer(sql):
        name = match.group(1).lower()
        if name not in seen:
            seen.add(name)
            tables.append(name)
    return tables


def validate_cluster_ready(health: dict, workers: dict) -> None:
    status = str(health.get("status", "UNKNOWN")).upper()
    if status != "UP":
        raise ValidationError(f"cluster status is {status}; expected UP")

    worker_list = workers.get("workers") or []
    if not worker_list:
        raise ValidationError("no workers registered")

    unhealthy = [
        worker["workerId"]
        for worker in worker_list
        if str(worker.get("status", "")).upper() != "HEALTHY"
    ]
    if unhealthy:
        raise ValidationError(
            "unhealthy workers: " + ", ".join(unhealthy)
        )


def validate_tables_and_shards(sql: str, shards: dict) -> None:
    required_tables = extract_table_names(sql)
    if not required_tables:
        raise ValidationError("could not find any tables in SQL FROM/JOIN clauses")

    catalog = {
        str(table["tableName"]).lower(): table
        for table in (shards.get("tables") or [])
    }

    if not catalog:
        raise ValidationError(
            "cluster catalog is empty; load shard data before querying"
        )

    missing_tables = [table for table in required_tables if table not in catalog]
    if missing_tables:
        known = ", ".join(sorted(catalog)) or "(none)"
        raise ValidationError(
            "unknown table(s): "
            + ", ".join(missing_tables)
            + f". Known tables: {known}"
        )

    for table_name in required_tables:
        table = catalog[table_name]
        shard_rows = table.get("shards") or []
        if not shard_rows:
            raise ValidationError(f"table '{table_name}' has no shards")

        unassigned = [
            shard["shardId"]
            for shard in shard_rows
            if not _non_empty(shard.get("owners"))
        ]
        if unassigned:
            raise ValidationError(
                f"table '{table_name}' has shards without owners: "
                + ", ".join(str(shard_id) for shard_id in unassigned)
            )


def validate_query_prerequisites(client: CoordinatorClient, sql: str) -> None:
    health = client.health()
    workers = client.workers()
    validate_cluster_ready(health, workers)
    shards = client.shards()
    validate_tables_and_shards(sql, shards)


def _non_empty(values: Iterable[object] | None) -> bool:
    return bool(values)
