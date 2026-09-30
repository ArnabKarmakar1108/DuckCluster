# Docker deployment

Run DuckCluster as a multi-container stack: one coordinator, three workers, and a one-shot job that prepares demo shard data.

Related files at the repo root:

| File | Role |
|------|------|
| `Dockerfile` | Builds coordinator and worker JARs; runtime image includes Python + DuckDB for shard prep |
| `docker-compose.yml` | Service definitions, networking, and named volumes |
| `docker/prepare-data.sh` | Entrypoint for the `datagen` service |

## Prerequisites

- Docker Engine
- **Docker Compose v2** — the `docker compose` subcommand (not legacy `docker-compose` alone)

On Ubuntu/Debian, if `docker compose version` fails or `docker compose up --build` prints `unknown flag: --build`, install the plugin:

```bash
sudo apt install docker-compose-v2
docker compose version
```

Without the plugin, Docker does not recognize `compose` and mis-parses `--build` as a global `docker` flag.

Enough disk for three named volumes (demo data is small; default CSV is 10 rows).

## Quick start

From the repository root:

```bash
docker compose up --build
```

When startup finishes:

- **Dashboard:** http://localhost:8080/dashboard/
- **Health:** `curl -s http://localhost:8080/v1/cluster/health`
- **CLI** (from the host, cluster must be up):

```bash
./scripts/duckcluster status
./scripts/duckcluster query "SELECT COUNT(*) FROM events"
```

Stop containers:

```bash
docker compose down
```

Stop and delete shard volumes (forces shard prep on the next `up`):

```bash
docker compose down -v
```

## Services

```
datagen  →  writes shard DuckDB files into worker volumes
coordinator  →  HTTP :8080, gRPC :9090 (published to host)
worker-1/2/3  →  gRPC :9101 inside the compose network
```

| Service | Purpose |
|---------|---------|
| `datagen` | Runs `prepare-data.sh`, splits `demo-events.csv` into shard files, copies them into per-worker volumes, then exits |
| `coordinator` | Query planner, REST API, dashboard, worker registry |
| `catalog-init` | One-shot: declares `events` with shard key `id` (`POST /v1/catalog/tables`) once the coordinator is healthy, so self-joins and co-partitioned joins on `id` are planned shard-locally |
| `worker-1`, `worker-2`, `worker-3` | Execute fragments on local shard files and register with the coordinator |

Startup order:

1. `datagen` completes successfully.
2. `coordinator` starts and passes its healthcheck (HTTP API reachable).
3. Workers start, register over gRPC, and attach shards from `/data`.

Workers advertise themselves using Docker service hostnames (`worker-1`, `worker-2`, `worker-3`) so the coordinator can reach them on the internal network. Worker gRPC ports are not published to the host; only the coordinator HTTP/gRPC ports are.

## Data layout

`datagen` writes into named volumes mounted at:

| Volume | `datagen` path | Worker path |
|--------|----------------|-------------|
| `worker-1-data` | `/shards/worker-1` | `/data` (`worker-1`) |
| `worker-2-data` | `/shards/worker-2` | `/data` (`worker-2`) |
| `worker-3-data` | `/shards/worker-3` | `/data` (`worker-3`) |

Default source file: `tests/integration/data/demo-events.csv` (table `events`, partition key `id`, 6 shards, replication factor 2).

`prepare-data.sh` invokes `scripts/prepare-demo-shards.py` with ring assignments from `RingAssignerCli` (classes copied into the image at build time).

## Environment variables

Set these in the shell or in a `.env` file beside `docker-compose.yml`.

| Variable | Default | Used by | Description |
|----------|---------|---------|-------------|
| `DUCKCLUSTER_HTTP_PORT` | `8080` | `coordinator` | Host port for REST API and dashboard |
| `DUCKCLUSTER_GRPC_PORT` | `9090` | `coordinator` | Host port for coordinator gRPC |
| `DUCKCLUSTER_SHARD_COUNT` | `6` | all | Logical shard count |
| `DUCKCLUSTER_REPLICATION_FACTOR` | `2` | all | Shard replicas per ring assignment |
| `DUCKCLUSTER_DEMO_CSV` | bundled demo CSV | `datagen` | Path **inside the image** to a CSV or Parquet file |

Example — custom CSV baked into the image at build time:

```bash
# Add COPY in Dockerfile, or mount a file and point datagen at it
DUCKCLUSTER_DEMO_CSV=/app/my-events.csv docker compose up --build
```

Example — different host ports:

```bash
DUCKCLUSTER_HTTP_PORT=18080 DUCKCLUSTER_GRPC_PORT=19090 docker compose up
```

Workers always use `DUCKCLUSTER_COORDINATOR_HOST=coordinator` and `DUCKCLUSTER_DATA_DIR=/data` as set in `docker-compose.yml`.

## Image build

The `Dockerfile` is a two-stage build:

1. **build** — `mvn -pl coordinator,worker -am package -DskipTests`
2. **runtime** — JRE 17, `curl`, Python 3, `duckdb` pip package, JARs, demo CSV, and this directory’s scripts

Build only (no start):

```bash
docker compose build
```

## Operations

**Rebuild after code changes:**

```bash
docker compose up --build
```

**Re-run shard preparation** (e.g. after changing `DUCKCLUSTER_DEMO_CSV` or shard settings):

```bash
docker compose down -v
docker compose up --build
```

**Logs:**

```bash
docker compose logs -f coordinator
docker compose logs worker-1
```

**Scale note:** The compose file is fixed at three workers. Adding workers requires new services in `docker-compose.yml`, extra volumes, and updating the `--workers` / `--dirs` lists in `prepare-data.sh`.

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| Workers never start | Coordinator healthcheck failing; check `docker compose logs coordinator` |
| `unknown flag: --build` | Docker Compose plugin not installed — `sudo apt install docker-compose-v2` (Ubuntu) |
| `datagen` exits with error | Invalid `DUCKCLUSTER_DEMO_CSV`, missing source file, or shard script failure — `docker compose logs datagen` |
| CLI reports cluster down | Stack not ready yet, or wrong URL — default is `http://127.0.0.1:8080` |
| Empty query results | Stale volumes from an old shard layout — `docker compose down -v` and start again |
| Cluster health `DEGRADED` | Normal briefly at startup; should become `UP` once all three workers register |

## Local cluster without Docker

For development on the host without containers, use:

```bash
./scripts/start-cluster.sh
```

See the main [README](../README.md) and [CLI README](../cli/README.md) for query and shell usage.
