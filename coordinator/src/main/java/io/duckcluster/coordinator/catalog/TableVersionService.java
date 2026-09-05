package io.duckcluster.coordinator.catalog;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import io.duckcluster.proto.v1.ActivateTableVersionResponse;

import java.util.LinkedHashMap;
import java.util.Map;

/** Promotes staged shard sets on workers and records the active catalog version (5.2). */
public final class TableVersionService {
    private final ClusterCatalog catalog;
    private final WorkerRegistry registry;
    private final WorkerNodeClient workerClient;

    public TableVersionService(ClusterCatalog catalog, WorkerRegistry registry, WorkerNodeClient workerClient) {
        this.catalog = catalog;
        this.registry = registry;
        this.workerClient = workerClient;
    }

    public Map<String, Object> activateVersion(String tableName, long version) {
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
        catalog.table(tableName);
        int shardsActivated = 0;
        for (WorkerRegistry.WorkerRecord worker : registry.listWorkers()) {
            if (!registry.isSchedulable(worker.workerId())) {
                continue;
            }
            ActivateTableVersionResponse response = workerClient.activateTableVersion(worker, tableName, version);
            shardsActivated += response.getShardsActivated();
        }
        if (shardsActivated == 0) {
            throw new IllegalStateException(
                    "no staged shard files found for table " + tableName + " version " + version);
        }
        catalog.setActiveDataVersion(tableName, version);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("table", tableName);
        body.put("version", version);
        body.put("shardsActivated", shardsActivated);
        body.put("activeDataVersion", version);
        return body;
    }
}
