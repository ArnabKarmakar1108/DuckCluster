package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.QueryAnalysis;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class DuckDbBulkInserter {
    static final int BATCH_SIZE = 512;

    private DuckDbBulkInserter() {}

    static void insertRows(
            Connection connection,
            String tableName,
            List<String> columns,
            List<List<Object>> rows,
            List<String> columnTypes)
            throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        if (connection instanceof DuckDBConnection duckConnection) {
            insertViaAppender(duckConnection, tableName, columns, rows, columnTypes);
        } else {
            insertViaBatchedSql(connection, tableName, columns, rows, columnTypes);
        }
    }

    static void appendFragmentBatches(
            Connection connection,
            String tableName,
            List<String> tempColumns,
            List<String> columnTypes,
            List<FragmentResult> fragmentResults,
            QueryAnalysis analysis)
            throws SQLException {
        if (!(connection instanceof DuckDBConnection duckConnection)) {
            List<List<Object>> rows = materializeFragmentRows(tempColumns, fragmentResults, analysis);
            insertViaBatchedSql(connection, tableName, tempColumns, rows, columnTypes);
            return;
        }
        try (DuckDBAppender appender =
                new DuckDBAppender(duckConnection, DuckDBConnection.DEFAULT_SCHEMA, tableName)) {
            for (FragmentResult fragment : fragmentResults) {
                for (RowBatchData batch : fragment.batches()) {
                    Map<String, Integer> columnIndex = indexColumns(batch.columnNames());
                    for (List<String> batchRow : batch.rows()) {
                        appender.beginRow();
                        for (int columnIndexPos = 0; columnIndexPos < tempColumns.size(); columnIndexPos++) {
                            String column = tempColumns.get(columnIndexPos);
                            Object value = batchRow.get(columnIndex.get(column));
                            appendValue(
                                    appender,
                                    normalizeValue(value, columnTypes.get(columnIndexPos)),
                                    columnTypes.get(columnIndexPos));
                        }
                        appender.endRow();
                    }
                }
            }
            appender.flush();
        }
    }

    private static List<List<Object>> materializeFragmentRows(
            List<String> tempColumns, List<FragmentResult> fragmentResults, QueryAnalysis analysis) {
        List<List<Object>> rows = new ArrayList<>();
        for (FragmentResult fragment : fragmentResults) {
            for (RowBatchData batch : fragment.batches()) {
                Map<String, Integer> columnIndex = indexColumns(batch.columnNames());
                for (List<String> batchRow : batch.rows()) {
                    List<Object> row = new ArrayList<>(tempColumns.size());
                    for (String column : tempColumns) {
                        row.add(batchRow.get(columnIndex.get(column)));
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private static Map<String, Integer> indexColumns(List<String> columnNames) {
        Map<String, Integer> index = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            index.put(columnNames.get(i), i);
        }
        return index;
    }

    static void insertViaBatchedSql(
            Connection connection,
            String tableName,
            List<String> columns,
            List<List<Object>> rows,
            List<String> columnTypes)
            throws SQLException {
        String columnList = String.join(", ", columns.stream().map(DuckDbBulkInserter::quote).toList());
        try (Statement statement = connection.createStatement()) {
            for (int start = 0; start < rows.size(); start += BATCH_SIZE) {
                int end = Math.min(start + BATCH_SIZE, rows.size());
                StringBuilder insert = new StringBuilder("INSERT INTO ")
                        .append(tableName)
                        .append(" (")
                        .append(columnList)
                        .append(") VALUES ");
                for (int rowIndex = start; rowIndex < end; rowIndex++) {
                    if (rowIndex > start) {
                        insert.append(", ");
                    }
                    List<Object> row = rows.get(rowIndex);
                    insert.append("(");
                    for (int columnIndex = 0; columnIndex < row.size(); columnIndex++) {
                        if (columnIndex > 0) {
                            insert.append(", ");
                        }
                        Object value = normalizeValue(row.get(columnIndex), columnTypes.get(columnIndex));
                        insert.append(toSqlLiteral(value));
                    }
                    insert.append(")");
                }
                statement.execute(insert.toString());
            }
        }
    }

    private static void insertViaAppender(
            DuckDBConnection connection,
            String tableName,
            List<String> columns,
            List<List<Object>> rows,
            List<String> columnTypes)
            throws SQLException {
        try (DuckDBAppender appender = new DuckDBAppender(connection, DuckDBConnection.DEFAULT_SCHEMA, tableName)) {
            for (List<Object> row : rows) {
                appender.beginRow();
                for (int columnIndex = 0; columnIndex < row.size(); columnIndex++) {
                    appendValue(appender, normalizeValue(row.get(columnIndex), columnTypes.get(columnIndex)), columnTypes.get(columnIndex));
                }
                appender.endRow();
            }
            appender.flush();
        }
    }

    private static void appendValue(DuckDBAppender appender, Object value, String sqlType) throws SQLException {
        if (value == null) {
            appender.append((String) null);
            return;
        }
        switch (sqlType) {
            case "DOUBLE" -> appendDouble(appender, value);
            case "BIGINT" -> appendLong(appender, value);
            default -> appender.append(value.toString());
        }
    }

    private static void appendDouble(DuckDBAppender appender, Object value) throws SQLException {
        if (value instanceof Number number) {
            appender.append(number.doubleValue());
            return;
        }
        appender.append(Double.parseDouble(value.toString()));
    }

    private static void appendLong(DuckDBAppender appender, Object value) throws SQLException {
        if (value instanceof Number number) {
            appender.append(number.longValue());
            return;
        }
        appender.append(Long.parseLong(value.toString()));
    }

    static Object normalizeValue(Object value, String sqlType) {
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

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
