package io.duckcluster.common.model;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ClusterCatalog {

    private final ConcurrentHashMap<String, TableShardConfig> tables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> estimatedRowCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<TableColumn>> tableColumns = new ConcurrentHashMap<>();

    public ClusterCatalog() {}

    public synchronized void declareTable(TableShardConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("table config is required");
        }
        tables.put(config.tableName().toLowerCase(), config);
    }

    public synchronized boolean removeTable(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return false;
        }
        String key = tableName.toLowerCase();
        boolean removed = tables.remove(key) != null;
        estimatedRowCounts.remove(key);
        tableColumns.remove(key);
        return removed;
    }

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

    public java.util.List<TableShardConfig> listTables() {
        return tables.values().stream()
                .sorted(java.util.Comparator.comparing(TableShardConfig::tableName))
                .toList();
    }

    public void restoreTable(TableShardConfig config) {
        if (config == null) {
            return;
        }
        String key = config.tableName().toLowerCase();
        TableShardConfig existing = tables.get(key);
        if (existing == null || existing.shardCount() < config.shardCount()) {
            tables.put(key, config);
        }
    }

    public void replaceEstimatedRowCounts(Map<String, Long> totals) {
        estimatedRowCounts.clear();
        if (totals == null) {
            return;
        }
        totals.forEach((table, rows) -> {
            if (table != null && !table.isBlank() && rows != null && rows > 0) {
                estimatedRowCounts.put(table.toLowerCase(), rows);
            }
        });
    }

    public long estimatedRowCount(String tableName) {
        return estimatedRowCounts.getOrDefault(tableName.toLowerCase(), 0L);
    }

    public void updateTableSchema(String tableName, List<TableColumn> columns) {
        if (tableName == null || tableName.isBlank() || columns == null || columns.isEmpty()) {
            return;
        }
        tableColumns.put(tableName.toLowerCase(), List.copyOf(columns));
    }

    public List<TableColumn> tableColumns(String tableName) {
        return tableColumns.getOrDefault(tableName.toLowerCase(), List.of());
    }

    public boolean updateTableSchemaIfCompatible(String tableName, List<TableColumn> incoming) {
        if (incoming == null || incoming.isEmpty()) {
            return true;
        }
        String key = tableName.toLowerCase();
        List<TableColumn> existing = tableColumns.get(key);
        if (existing == null || existing.isEmpty()) {
            tableColumns.put(key, List.copyOf(incoming));
            return true;
        }
        if (existing.equals(incoming)) {
            return true;
        }
        return false;
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
