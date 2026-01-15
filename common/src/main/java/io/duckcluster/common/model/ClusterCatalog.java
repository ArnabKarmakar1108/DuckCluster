package io.duckcluster.common.model;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public final class ClusterCatalog {
    private final Map<String, TableShardConfig> tables;

    public ClusterCatalog(Map<String, TableShardConfig> tables) {
        this.tables = Map.copyOf(tables);
    }

    public static ClusterCatalog empty() {
        return new ClusterCatalog(Collections.emptyMap());
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

    public Set<String> tableNames() {
        return tables.keySet();
    }
}
