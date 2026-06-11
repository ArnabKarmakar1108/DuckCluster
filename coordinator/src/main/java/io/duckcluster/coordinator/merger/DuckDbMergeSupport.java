package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.NestedDerivedTableSpec;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.model.TopKSpec;
import io.duckcluster.common.planner.MergePushdownPlanner;
import io.duckcluster.common.planner.MergeSqlBuilder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class DuckDbMergeSupport {
    private static final String TEMP_TABLE = "__merge_temp";
    private static final String PHASE1_TABLE = "__merge_phase1";

    private DuckDbMergeSupport() {}

    static QueryResult mergeWithSql(MergeContext context, QueryResult.QueryStats stats, String mergeSql) {
        if (MergePushdownPlanner.useHierarchicalGroupByMerge(context)) {
            String intermediateMergeSql =
                    MergeSqlBuilder.buildGroupByIntermediateMerge(context.plan().analysis());
            return mergeGroupByHierarchical(context, stats, mergeSql, intermediateMergeSql);
        }
        try (CoordinatorDuckDbPool.Lease lease = CoordinatorDuckDbPool.get().lease()) {
            Connection connection = lease.connection();
            createTempTableFromFragments(connection, context);
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(mergeSql)) {
                return toQueryResult(context.queryId(), resultSet, stats);
            } finally {
                dropTable(connection, TEMP_TABLE);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to merge partial results in coordinator DuckDB: " + e.getMessage(), e);
        }
    }

    static QueryResult mergeGroupByHierarchical(
            MergeContext context,
            QueryResult.QueryStats stats,
            String finalMergeSql,
            String intermediateMergeSql) {
        try (CoordinatorDuckDbPool.Lease lease = CoordinatorDuckDbPool.get().lease()) {
            Connection connection = lease.connection();
            HierarchicalCollapse collapse = collapseWorkerFragments(connection, context, intermediateMergeSql);
            try {
                createTempTableFromFragments(connection, collapse.singleFragmentContext());
                for (String workerTable : collapse.workerTables()) {
                    connection.createStatement().execute(
                            "INSERT INTO " + TEMP_TABLE + " SELECT * FROM " + workerTable);
                }
                try (Statement statement = connection.createStatement();
                        ResultSet resultSet = statement.executeQuery(finalMergeSql)) {
                    return toQueryResult(context.queryId(), resultSet, stats);
                }
            } finally {
                dropTable(connection, TEMP_TABLE);
                for (String workerTable : collapse.workerTables()) {
                    dropTable(connection, workerTable);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed hierarchical GROUP BY merge in coordinator DuckDB: " + e.getMessage(), e);
        }
    }

    private record HierarchicalCollapse(MergeContext singleFragmentContext, List<String> workerTables) {}

    private static HierarchicalCollapse collapseWorkerFragments(
            Connection connection, MergeContext context, String intermediateMergeSql) throws SQLException {
        Map<String, List<FragmentResult>> fragmentsByWorker = new LinkedHashMap<>();
        for (FragmentResult fragment : context.fragmentResults()) {
            fragmentsByWorker
                    .computeIfAbsent(fragment.workerId(), ignored -> new ArrayList<>())
                    .add(fragment);
        }

        List<FragmentResult> singles = new ArrayList<>();
        List<String> workerTables = new ArrayList<>();
        int syntheticWorkerId = 0;
        for (Map.Entry<String, List<FragmentResult>> entry : fragmentsByWorker.entrySet()) {
            List<FragmentResult> workerFragments = entry.getValue();
            if (workerFragments.size() == 1) {
                singles.add(workerFragments.get(0));
                continue;
            }

            String workerTable = "__merge_worker_" + syntheticWorkerId++;
            MergeContext workerContext = new MergeContext(
                    context.queryId(), context.plan(), workerFragments, context.durationMs(), Map.of());
            dropTable(connection, TEMP_TABLE);
            createTempTableFromFragments(connection, workerContext);
            dropTable(connection, workerTable);
            connection.createStatement().execute(
                    "CREATE TABLE " + workerTable + " AS (" + intermediateMergeSql + ")");
            dropTable(connection, TEMP_TABLE);
            workerTables.add(workerTable);
        }
        MergeContext singleFragmentContext = new MergeContext(
                context.queryId(), context.plan(), singles, context.durationMs(), Map.of());
        return new HierarchicalCollapse(singleFragmentContext, workerTables);
    }

    static QueryResult mergeWithCte(
            MergeContext context,
            QueryResult.QueryStats stats,
            String phase1Sql,
            String phase2Sql,
            Map<String, RowBatchData> coordinatorTables) {
        try (CoordinatorDuckDbPool.Lease lease = CoordinatorDuckDbPool.get().lease()) {
            Connection connection = lease.connection();
            createTempTableFromFragments(connection, context);
            materializePhase1InDb(connection, phase1Sql);

            for (Map.Entry<String, RowBatchData> entry : coordinatorTables.entrySet()) {
                RowBatchData batch = entry.getValue();
                createNamedTempTable(
                        connection,
                        entry.getKey(),
                        batch.columnNames(),
                        batchRows(batch),
                        varcharColumnTypes(batch.columnNames().size()));
            }

            try (Statement statement = connection.createStatement();
                    ResultSet phase2Result = statement.executeQuery(phase2Sql)) {
                return toQueryResult(context.queryId(), phase2Result, stats);
            } finally {
                dropTable(connection, TEMP_TABLE);
                for (String tableName : coordinatorTables.keySet()) {
                    dropTable(connection, tableName);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to merge WITH CTE results in coordinator DuckDB: " + e.getMessage(), e);
        }
    }

    private static List<List<Object>> batchRows(RowBatchData batch) {
        List<List<Object>> rows = new ArrayList<>();
        for (List<String> row : batch.rows()) {
            rows.add(new ArrayList<>(row));
        }
        return rows;
    }

    private static List<String> varcharColumnTypes(int columnCount) {
        List<String> types = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            types.add("VARCHAR");
        }
        return types;
    }

    static QueryResult mergeNestedGroupBy(
            MergeContext context,
            QueryResult.QueryStats stats,
            String phase1Sql,
            String phase2Sql,
            NestedDerivedTableSpec nested) {
        try (CoordinatorDuckDbPool.Lease lease = CoordinatorDuckDbPool.get().lease()) {
            Connection connection = lease.connection();
            createTempTableFromFragments(connection, context);
            materializePhase1InDb(connection, phase1Sql);

            try (Statement statement = connection.createStatement();
                    ResultSet phase2Result = statement.executeQuery(phase2Sql)) {
                return toQueryResult(context.queryId(), phase2Result, stats);
            } finally {
                dropTable(connection, TEMP_TABLE);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to merge nested derived table results in coordinator DuckDB: " + e.getMessage(), e);
        }
    }

    private static void createTempTableFromFragments(Connection connection, MergeContext context)
            throws SQLException {
        QueryAnalysis analysis = context.plan().analysis();
        List<String> columns = MergeSqlBuilder.tempTableColumns(analysis);
        List<String> columnTypes = columnTypesForAnalysis(analysis, columns);
        createEmptyTable(connection, TEMP_TABLE, columns, columnTypes);
        DuckDbBulkInserter.appendFragmentBatches(
                connection, TEMP_TABLE, columns, columnTypes, context.fragmentResults(), analysis);
    }

    private static void materializePhase1InDb(Connection connection, String phase1Sql) throws SQLException {
        dropTable(connection, PHASE1_TABLE);
        connection.createStatement().execute("CREATE TABLE " + PHASE1_TABLE + " AS (" + phase1Sql + ")");
        dropTable(connection, TEMP_TABLE);
        connection.createStatement().execute("ALTER TABLE " + PHASE1_TABLE + " RENAME TO " + TEMP_TABLE);
    }

    private static void createEmptyTable(
            Connection connection, String tableName, List<String> columns, List<String> columnTypes)
            throws SQLException {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                ddl.append(", ");
            }
            ddl.append(quote(columns.get(i))).append(" ").append(columnTypes.get(i));
        }
        ddl.append(")");
        connection.createStatement().execute(ddl.toString());
    }

    private static void createNamedTempTable(
            Connection connection,
            String tableName,
            List<String> columns,
            List<List<Object>> rows,
            List<String> columnTypes)
            throws SQLException {
        createEmptyTable(connection, tableName, columns, columnTypes);
        DuckDbBulkInserter.insertRows(connection, tableName, columns, rows, columnTypes);
    }

    private static List<String> columnTypesForAnalysis(QueryAnalysis analysis, List<String> columns) {
        List<String> types = new ArrayList<>(columns.size());
        for (String column : columns) {
            types.add(columnType(analysis, column));
        }
        return types;
    }

    private static QueryResult toQueryResult(String queryId, ResultSet resultSet, QueryResult.QueryStats stats)
            throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();
        List<String> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(metadata.getColumnLabel(i));
        }

        List<List<Object>> rows = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                row.add(resultSet.getObject(i));
            }
            rows.add(row);
        }
        return new QueryResult(queryId, columns, rows, stats);
    }

    private static String columnType(QueryAnalysis analysis, String column) {
        if (analysis.groupByColumns().contains(column)) {
            return "VARCHAR";
        }
        for (var aggregate : analysis.aggregates()) {
            if (aggregate.mergeColumnName().equals(column)) {
                if (aggregate.part() == io.duckcluster.common.model.AggregateSpec.AggregatePart.DISTINCT_COUNT) {
                    return "BIGINT";
                }
                return "DOUBLE";
            }
        }
        return "VARCHAR";
    }

    private static void dropTable(Connection connection, String tableName) throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
