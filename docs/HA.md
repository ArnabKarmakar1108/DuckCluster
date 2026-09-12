# Coordinator high availability (cold active–passive)

DuckCluster uses a **single active coordinator** that holds query state, the worker registry, and merge execution. High availability is **cold active–passive**: only one coordinator process serves traffic at a time; failover means **starting a new coordinator** that reloads metadata and accepts worker re-registration.

## What survives failover

| State | After coordinator loss |
|-------|-------------------------|
| Shard files on workers | Intact |
| Worker processes | Keep running; reconnect via heartbeat |
| Catalog / ownership intent | Restored from `coordinator-metadata.json` snapshot when `DUCKCLUSTER_DATA_DIR` is on persistent storage |
| In-flight queries | Lost — clients must retry |
| Async query results | Lost unless already fetched |

## Recommended deployment (Kubernetes)

1. **Coordinator Deployment with `replicas: 1`** — intentional single writer.
2. **PersistentVolumeClaim** for `DUCKCLUSTER_DATA_DIR` (metadata snapshot, merge temp, async spool).
3. **Service** `duckcluster-coordinator` — stable DNS for workers and clients.
4. **Readiness** `GET /v1/cluster/ready` — not ready until workers and catalog are usable.
5. **Liveness** `GET /v1/cluster/health` — process up; may be `DEGRADED` during partial worker loss.
6. **PodDisruptionBudget** — allow voluntary disruption so node drains can replace the pod.

Manifest sketches live under `deploy/kubernetes/` (`coordinator.yaml`, `coordinator-pvc.yaml`, `coordinator-pdb.yaml`).

## Failover procedure

1. Kubernetes or an operator detects the coordinator pod is dead.
2. A new pod starts with the **same PVC** (or restored snapshot volume).
3. Coordinator loads `coordinator-metadata.json` if present, then starts HTTP/gRPC.
4. Workers detect coordinator restart (boot epoch), **re-register**, and report shard ownership.
5. `ShardReplicator` reconciles under-replicated shards.
6. Clients retry failed queries.

Typical **RTO**: pod schedule + JVM start + worker re-register (often 30–90 seconds). **RPO** for catalog intent: last metadata snapshot interval (default 300s unless tuned).

## Graceful shutdown

On SIGTERM the coordinator:

1. Sets admission **draining** — new HTTP queries receive `503` with `Retry-After`.
2. Waits up to 30s for in-flight queries to finish.
3. Writes a final metadata snapshot.
4. Stops gRPC/HTTP.

Use `preStop` with a short sleep in Kubernetes so the Service endpoints update before the process exits.

## Worker configuration

Point workers at the **Service hostname**, not a pod IP:

- `DUCKCLUSTER_COORDINATOR_HOST=duckcluster-coordinator`

Workers compare the coordinator **boot epoch** on heartbeat; when it changes they re-register automatically so the new coordinator rebuilds the registry without manual steps.
