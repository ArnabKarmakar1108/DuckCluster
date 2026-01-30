package io.duckcluster.common.model;

import java.util.Map;

public final class ClusterCatalog {
    public static ClusterCatalog demo(int shardCount) {
        return new ClusterCatalog(Map.of(
                "events", new TableShardConfig("events", shardCount, "id")));
    }

    private final Map<String, TableShardConfig> tables;

    public ClusterCatalog(Map<String, TableShardConfig> tables) {
        this.tables = Map.copyOf(tables);
    }

    public boolean hasTable(String tableName) {
        return tables.containsKey(tableName.toLowerCase());
    }

    public TableShardConfig table(String tableName) {
        TableShardConfig config = tables.get(tableName.toLowerCase());
        if (config == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        return config;
    }
}
