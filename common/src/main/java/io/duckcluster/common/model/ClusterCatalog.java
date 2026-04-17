package io.duckcluster.common.model;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ClusterCatalog {

    private final ConcurrentHashMap<String, TableShardConfig> tables = new ConcurrentHashMap<>();

    public ClusterCatalog() {}

    public synchronized void registerTable(String tableName, int shardCount) {
        String key = tableName.toLowerCase();
        TableShardConfig existing = tables.get(key);
        if (existing == null || existing.shardCount() < shardCount) {
            tables.put(key, new TableShardConfig(tableName, shardCount, "rowid"));
        }
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

    public Set<String> getShardedTableNames() {
        return Set.copyOf(tables.keySet());
    }

    public static ClusterCatalog withTables(Map<String, TableShardConfig> tables) {
        ClusterCatalog catalog = new ClusterCatalog();
        for (Map.Entry<String, TableShardConfig> entry : tables.entrySet()) {
            catalog.tables.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        return catalog;
    }

    public static ClusterCatalog demo(int shardCount) {
        return withTables(Map.of("events", new TableShardConfig("events", shardCount, "id")));
    }
}
