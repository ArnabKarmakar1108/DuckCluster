# Kubernetes deployment (8.2)

Minimal manifests for a single-coordinator, multi-worker DuckCluster. Workers need persistent volumes
for shard `.duckdb` files under `DUCKCLUSTER_DATA_DIR`.

## Apply order

1. `kubectl apply -f coordinator-pvc.yaml`
2. `kubectl apply -f coordinator.yaml`
3. `kubectl apply -f coordinator-pdb.yaml`
4. `kubectl apply -f worker.yaml`

Coordinator HA is **cold active–passive** (`replicas: 1`): failover is pod restart plus metadata snapshot restore. See [docs/HA.md](../../docs/HA.md).

Probes use `/v1/cluster/health` (coordinator) and worker gRPC is not probed here; extend with a gRPC health
service when available.

## Upgrade order (8.3)

Roll workers first, then the coordinator, so new workers speak `protocol_version=1` before the coordinator
enforces it.
