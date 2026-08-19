# Kubernetes deployment (8.2)

Minimal manifests for a single-coordinator, multi-worker DuckCluster. Workers need persistent volumes
for shard `.duckdb` files under `DUCKCLUSTER_DATA_DIR`.

## Apply order

1. `kubectl apply -f coordinator.yaml`
2. `kubectl apply -f worker.yaml`

Probes use `/v1/cluster/health` (coordinator) and worker gRPC is not probed here; extend with a gRPC health
service when available.

## Upgrade order (8.3)

Roll workers first, then the coordinator, so new workers speak `protocol_version=1` before the coordinator
enforces it.
