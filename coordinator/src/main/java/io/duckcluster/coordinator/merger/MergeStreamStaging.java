package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.PlannedQuery;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Streams fragment row batches into per-fragment DuckDB tables as they arrive from workers,
 * so merge does not need all batches in the Java heap first.
 */
final class MergeStreamStaging implements AutoCloseable {
    private final String queryId;
    private final PlannedQuery plan;
    private final CoordinatorDuckDbPool.Lease lease;
    private final Connection connection;
    private final List<String> tempColumns;
    private final List<String> plannedColumnTypes;
    private final Map<String, String> wireColumnTypes = new ConcurrentHashMap<>();
    private final Map<Integer, String> fragmentTables = new ConcurrentHashMap<>();
    private final Object connectionLock = new Object();

    private MergeStreamStaging(
            String queryId, PlannedQuery plan, CoordinatorDuckDbPool.Lease lease, Connection connection) {
        this.queryId = queryId;
        this.plan = plan;
        this.lease = lease;
        this.connection = connection;
        this.tempColumns = MergeSqlColumns.fromPlannedQuery(plan);
        this.plannedColumnTypes = DuckDbMergeSupport.columnTypesForPlannedQuery(plan, tempColumns);
    }

    private List<String> effectiveColumnTypes() {
        List<String> types = new ArrayList<>(tempColumns.size());
        for (int i = 0; i < tempColumns.size(); i++) {
            String column = tempColumns.get(i);
            String wire = wireColumnTypes.get(column);
            types.add(wire != null && !wire.isBlank() ? wire : plannedColumnTypes.get(i));
        }
        return types;
    }

    Connection connection() {
        return connection;
    }

    static MergeStreamStaging open(String queryId, PlannedQuery plan) {
        try {
            CoordinatorDuckDbPool.Lease lease = CoordinatorDuckDbPool.get().lease();
            return new MergeStreamStaging(queryId, plan, lease, lease.connection());
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open merge stream staging: " + e.getMessage(), e);
        }
    }

    void appendBatch(int fragmentId, RowBatchData batch) {
        recordWireTypes(batch);
        synchronized (connectionLock) {
            try {
                String tableName = ensureFragmentTable(fragmentId);
                DuckDbBulkInserter.appendSingleBatch(
                        connection, tableName, tempColumns, effectiveColumnTypes(), batch, plan.analysis());
            } catch (SQLException e) {
                throw new IllegalStateException(
                        "Failed to stage fragment " + fragmentId + " batch for query " + queryId + ": "
                                + e.getMessage(),
                        e);
            }
        }
    }

    boolean hasData() {
        return !fragmentTables.isEmpty();
    }

    Map<String, String> wireColumnTypes() {
        return Map.copyOf(wireColumnTypes);
    }

    void assembleInto(Connection targetConnection, String tempTable, MergeContext context) throws SQLException {
        List<String> columns = MergeSqlColumns.fromContext(context);
        DuckDbMergeSupport.createEmptyTable(targetConnection, tempTable, columns, effectiveColumnTypes());
        List<FragmentResult> fragments = context.fragmentResults();
        synchronized (connectionLock) {
            for (FragmentResult fragment : fragments) {
                String sourceTable = fragmentTables.get(fragment.fragmentId());
                if (sourceTable == null) {
                    continue;
                }
                String columnList = String.join(", ", columns.stream().map(DuckDbBulkInserter::quoteIdentifier).toList());
                targetConnection
                        .createStatement()
                        .execute("INSERT INTO " + tempTable + " (" + columnList + ") SELECT " + columnList + " FROM "
                                + sourceTable);
            }
        }
    }

    @Override
    public void close() {
        synchronized (connectionLock) {
            for (String table : fragmentTables.values()) {
                try {
                    DuckDbMergeSupport.dropTable(connection, table);
                } catch (SQLException ignored) {
                    // best-effort cleanup
                }
            }
            fragmentTables.clear();
        }
        lease.close();
    }

    private void recordWireTypes(RowBatchData batch) {
        if (batch.columnSqlTypes().isEmpty()) {
            return;
        }
        for (int i = 0; i < batch.columnNames().size(); i++) {
            wireColumnTypes.putIfAbsent(batch.columnNames().get(i), batch.columnSqlTypes().get(i));
        }
    }

    private String ensureFragmentTable(int fragmentId) throws SQLException {
        return fragmentTables.computeIfAbsent(fragmentId, id -> {
            try {
                String tableName = "__stg_" + sanitizeQueryId(queryId) + "_f" + id;
                DuckDbMergeSupport.createEmptyTable(connection, tableName, tempColumns, effectiveColumnTypes());
                return tableName;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static String sanitizeQueryId(String queryId) {
        return queryId.replaceAll("[^a-zA-Z0-9_]", "_");
    }

}
