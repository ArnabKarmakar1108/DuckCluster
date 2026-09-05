package io.duckcluster.coordinator.catalog;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.coordinator.http.ApiErrorCode;
import io.duckcluster.coordinator.http.ApiException;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Pre-dispatch checks: declared catalog tables exist and shards have at least one owner (5.1). */
public final class CatalogDispatchValidator {
    private final ClusterCatalog clusterCatalog;
    private final ShardCatalog shardCatalog;

    public CatalogDispatchValidator(ClusterCatalog clusterCatalog, ShardCatalog shardCatalog) {
        this.clusterCatalog = clusterCatalog;
        this.shardCatalog = shardCatalog;
    }

    public void validate(PlannedQuery plan) {
        Set<String> tables = new LinkedHashSet<>();
        if (plan.tableName() != null && !plan.tableName().isBlank()) {
            tables.add(plan.tableName());
        }
        tables.addAll(plan.shardedTables());
        tables.addAll(plan.coPartitionJoinTables());
        plan.shuffleJoinTables().forEach(table -> tables.add(table.tableName()));

        for (String tableName : tables) {
            if (!clusterCatalog.hasTable(tableName)) {
                throw new ApiException(ApiErrorCode.PLANNING_ERROR, "unknown table: " + tableName);
            }
            TableShardConfig table = clusterCatalog.table(tableName);
            for (int shardId = 0; shardId < table.shardCount(); shardId++) {
                List<String> actualOwners = shardCatalog.getActualOwners(tableName, shardId);
                if (actualOwners.isEmpty()) {
                    throw new ApiException(
                            ApiErrorCode.NO_WORKERS,
                            "no worker owns shard " + tableName + "_shard" + shardId);
                }
            }
        }
    }
}
