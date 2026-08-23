package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;

import java.util.Set;

/**
 * Per-query naming and partitioning context for fragment SQL generation: which tables are
 * materialized broadcasts, shuffled into per-shard buckets, or co-partitioned with the driving
 * table, plus the catalog used to decide whether correlated subqueries can run shard-locally.
 */
public record BroadcastScope(
        Set<String> materializedTableNames,
        String queryId,
        ClusterCatalog catalog,
        Set<String> shuffledTableNames,
        Set<String> coPartitionedTableNames) {

    public static BroadcastScope of(Set<String> materializedTableNames, String queryId) {
        return new BroadcastScope(materializedTableNames, queryId, null, Set.of(), Set.of());
    }

    public static BroadcastScope of(
            Set<String> materializedTableNames,
            String queryId,
            ClusterCatalog catalog,
            Set<String> shuffled,
            Set<String> coPartitioned) {
        return new BroadcastScope(
                materializedTableNames, queryId, catalog, Set.copyOf(shuffled), Set.copyOf(coPartitioned));
    }

    public boolean isMaterialized(String tableNameLower) {
        return materializedTableNames.contains(tableNameLower);
    }

    public boolean isShuffled(String tableNameLower) {
        return shuffledTableNames.contains(tableNameLower);
    }

    public String tempTableName(String tableNameLower) {
        long version = catalog != null && catalog.hasTable(tableNameLower)
                ? catalog.activeDataVersion(tableNameLower)
                : 0L;
        return BroadcastSqlNames.broadcastTable(queryId, tableNameLower, version);
    }

    public String shuffleTableName(String tableNameLower, int bucket) {
        return BroadcastSqlNames.shuffleBucketTable(queryId, tableNameLower, bucket);
    }
}
