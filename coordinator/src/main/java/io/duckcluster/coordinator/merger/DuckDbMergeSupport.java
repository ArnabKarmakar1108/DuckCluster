package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.NestedDerivedTableSpec;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.planner.MergeSqlBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
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

    private DuckDbMergeSupport() {}

    static QueryResult mergeWithSql(MergeContext context, QueryResult.QueryStats stats, String mergeSql) {
        List<String> tempColumns = MergeSqlColumns.fromContext(context);
        List<List<Object>> rows = collectRows(context, tempColumns);
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            createTempTable(connection, context.plan().analysis(), rows);
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(mergeSql)) {
                return toQueryResult(context.queryId(), resultSet, stats);
            } finally {
                connection.createStatement().execute("DROP TABLE IF EXISTS " + TEMP_TABLE);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to merge partial results in coordinator DuckDB: " + e.getMessage(), e);
        }
    }

    static QueryResult mergeNestedGroupBy(
            MergeContext context,
            QueryResult.QueryStats stats,
            String phase1Sql,
            String phase2Sql,
            NestedDerivedTableSpec nested) {
        List<String> tempColumns = MergeSqlColumns.fromContext(context);
        List<List<Object>> rows = collectRows(context, tempColumns);
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            createTempTable(connection, context.plan().analysis(), rows);
            List<String> phase1Columns;
            List<List<Object>> phase1Rows;
            try (Statement statement = connection.createStatement();
                    ResultSet phase1Result = statement.executeQuery(phase1Sql)) {
                phase1Columns = readColumnLabels(phase1Result);
                phase1Rows = readRows(phase1Result);
            } finally {
                connection.createStatement().execute("DROP TABLE IF EXISTS " + TEMP_TABLE);
            }

            createNamedTempTable(
                    connection,
                    TEMP_TABLE,
                    phase1Columns,
                    phase1Rows,
                    nestedDerivedColumnTypes(context.plan().analysis(), phase1Columns));

            try (Statement statement = connection.createStatement();
                    ResultSet phase2Result = statement.executeQuery(phase2Sql)) {
                return toQueryResult(context.queryId(), phase2Result, stats);
            } finally {
                connection.createStatement().execute("DROP TABLE IF EXISTS " + TEMP_TABLE);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to merge nested derived table results in coordinator DuckDB: " + e.getMessage(), e);
        }
    }

    private static List<List<Object>> collectRows(MergeContext context, List<String> tempColumns) {
        List<List<Object>> rows = new ArrayList<>();
        QueryAnalysis analysis = context.plan().analysis();
        for (FragmentResult fragment : context.fragmentResults()) {
            for (RowBatchData batch : fragment.batches()) {
                Map<String, Integer> columnIndex = indexColumns(batch.columnNames());
                for (List<String> batchRow : batch.rows()) {
                    rows.add(projectRow(batchRow, columnIndex, tempColumns, analysis));
                }
            }
        }
        return rows;
    }

    private static List<Object> projectRow(
            List<String> batchRow, Map<String, Integer> columnIndex, List<String> tempColumns, QueryAnalysis analysis) {
        List<Object> row = new ArrayList<>(tempColumns.size());
        for (String column : tempColumns) {
            if (analysis.groupByColumns().contains(column)) {
                row.add(batchRow.get(columnIndex.get(column)));
            } else {
                row.add(batchRow.get(columnIndex.get(column)));
            }
        }
        return row;
    }

    private static Map<String, Integer> indexColumns(List<String> columnNames) {
        Map<String, Integer> index = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            index.put(columnNames.get(i), i);
        }
        return index;
    }

    private static void createTempTable(Connection connection, QueryAnalysis analysis, List<List<Object>> rows)
            throws SQLException {
        createNamedTempTable(connection, TEMP_TABLE, MergeSqlBuilder.tempTableColumns(analysis), rows, analysis);
    }

    private static void createNamedTempTable(
            Connection connection,
            String tableName,
            List<String> columns,
            List<List<Object>> rows,
            QueryAnalysis analysis)
            throws SQLException {
        createNamedTempTable(connection, tableName, columns, rows, columnTypesForAnalysis(analysis, columns));
    }

    private static void createNamedTempTable(
            Connection connection,
            String tableName,
            List<String> columns,
            List<List<Object>> rows,
            List<String> columnTypes)
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

        if (rows.isEmpty()) {
            return;
        }

        String columnList = String.join(", ", columns.stream().map(DuckDbMergeSupport::quote).toList());
        try (Statement statement = connection.createStatement()) {
            for (List<Object> row : rows) {
                StringBuilder insert = new StringBuilder("INSERT INTO ")
                        .append(tableName)
                        .append(" (")
                        .append(columnList)
                        .append(") VALUES (");
                for (int i = 0; i < row.size(); i++) {
                    if (i > 0) {
                        insert.append(", ");
                    }
                    insert.append(toSqlLiteral(normalizeValue(row.get(i), columnTypes.get(i))));
                }
                insert.append(")");
                statement.execute(insert.toString());
            }
        }
    }

    private static List<String> columnTypesForAnalysis(QueryAnalysis analysis, List<String> columns) {
        List<String> types = new ArrayList<>(columns.size());
        for (String column : columns) {
            types.add(columnType(analysis, column));
        }
        return types;
    }

    private static List<String> nestedDerivedColumnTypes(QueryAnalysis innerAnalysis, List<String> columns) {
        List<String> types = new ArrayList<>(columns.size());
        for (String column : columns) {
            if (innerAnalysis.groupByColumns().contains(column)) {
                types.add("VARCHAR");
            } else {
                types.add("DOUBLE");
            }
        }
        return types;
    }

    private static List<String> readColumnLabels(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();
        List<String> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(metadata.getColumnLabel(i));
        }
        return columns;
    }

    private static List<List<Object>> readRows(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();
        List<List<Object>> rows = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                row.add(resultSet.getObject(i));
            }
            rows.add(row);
        }
        return rows;
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
                return "DOUBLE";
            }
        }
        return "VARCHAR";
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static Object normalizeValue(Object value, String sqlType) {
        if (value instanceof String text && text.isEmpty()) {
            if ("BIGINT".equals(sqlType) || "DOUBLE".equals(sqlType)) {
                return null;
            }
        }
        return value;
    }

    private static String toSqlLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        String text = value.toString();
        if (text.matches("-?\\d+(\\.\\d+)?")) {
            return text;
        }
        return "'" + text.replace("'", "''") + "'";
    }
}
