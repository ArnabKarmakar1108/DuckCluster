package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.model.TopKSpec;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuckDbBulkInserterTest {

    private final GroupByMergeStrategy strategy = new GroupByMergeStrategy();

    @Test
    void batchedSqlLoadsFiveThousandRows() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            List<String> columns = List.of("category", "value");
            List<String> types = List.of("VARCHAR", "DOUBLE");
            List<List<Object>> rows = buildRows(5_000);

            connection.createStatement().execute("CREATE TABLE bulk_test (category VARCHAR, value DOUBLE)");
            DuckDbBulkInserter.insertViaBatchedSql(connection, "bulk_test", columns, rows, types);

            try (ResultSet resultSet =
                    connection.createStatement().executeQuery("SELECT COUNT(*) FROM bulk_test")) {
                resultSet.next();
                assertEquals(5_000, resultSet.getLong(1));
            }
        }
    }

    @Test
    void appenderLoadsFiveThousandRows() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            List<String> columns = List.of("category", "value");
            List<String> types = List.of("VARCHAR", "DOUBLE");
            List<List<Object>> rows = buildRows(5_000);

            connection.createStatement().execute("CREATE TABLE bulk_test (category VARCHAR, value DOUBLE)");
            DuckDbBulkInserter.insertRows(connection, "bulk_test", columns, rows, types);

            try (ResultSet resultSet =
                    connection.createStatement().executeQuery("SELECT COUNT(*) FROM bulk_test")) {
                resultSet.next();
                assertEquals(5_000, resultSet.getLong(1));
            }
        }
    }

    @Test
    void appenderMatchesBatchedSqlForMerge() throws SQLException {
        List<String> columns = List.of("category", "value");
        List<String> types = List.of("VARCHAR", "DOUBLE");
        List<List<Object>> rows = buildRows(1_000);

        double batchedSum = sumValueColumn(loadWithBatchedSql(columns, types, rows));
        double appenderSum = sumValueColumn(loadWithAppender(columns, types, rows));
        assertEquals(batchedSum, appenderSum, 0.001);
    }

    @Test
    void mergeCorrectnessWithFiveThousandFragmentRows() {
        List<List<String>> fragmentRows = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            fragmentRows.add(List.of("bucket-" + (i % 50), "1"));
        }

        QueryAnalysis analysis = new QueryAnalysis(
                List.of("category"),
                List.of(new AggregateSpec("cnt", "__dc_agg_0", AggregateFunction.COUNT, null)),
                List.of("category", "cnt"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT category, COUNT(*) AS cnt FROM events GROUP BY category",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.GROUP_BY_MERGE,
                analysis,
                List.of(),
                TopKSpec.none());
        MergeContext context = new MergeContext(
                "query-bulk",
                plan,
                List.of(new FragmentResult(
                        0,
                        0,
                        "worker-1",
                        5_000L,
                        List.of(new RowBatchData(List.of("category", "__dc_agg_0"), fragmentRows)))),
                5_000L);

        QueryResult result = strategy.merge(context);

        assertEquals(50, result.rows().size());
        assertEquals(5_000.0, result.rows().stream()
                .mapToDouble(row -> ((Number) row.get(1)).doubleValue())
                .sum());
    }

    @Test
    void appenderIsFasterThanBatchedSqlAtTenThousandRows() throws SQLException {
        List<String> columns = List.of("category", "value");
        List<String> types = List.of("VARCHAR", "DOUBLE");
        List<List<Object>> rows = buildRows(10_000);

        long batchedMs = timeInsert(() -> loadWithBatchedSql(columns, types, rows));
        long appenderMs = timeInsert(() -> loadWithAppender(columns, types, rows));

        System.out.printf("bulk load 10k rows: batched=%dms appender=%dms%n", batchedMs, appenderMs);
        assertTrue(appenderMs <= batchedMs, "appender should not be slower than batched SQL");
    }

    private static long timeInsert(ThrowingRunnable runnable) throws SQLException {
        long start = System.nanoTime();
        runnable.run();
        return (System.nanoTime() - start) / 1_000_000L;
    }

    private static double sumValueColumn(List<List<Object>> rows) {
        return rows.stream()
                .mapToDouble(row -> ((Number) row.get(1)).doubleValue())
                .sum();
    }

    private static List<List<Object>> loadWithBatchedSql(
            List<String> columns, List<String> types, List<List<Object>> rows) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            connection.createStatement().execute("CREATE TABLE bulk_test (category VARCHAR, value DOUBLE)");
            DuckDbBulkInserter.insertViaBatchedSql(connection, "bulk_test", columns, rows, types);
            return readAllRows(connection);
        }
    }

    private static List<List<Object>> loadWithAppender(
            List<String> columns, List<String> types, List<List<Object>> rows) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            connection.createStatement().execute("CREATE TABLE bulk_test (category VARCHAR, value DOUBLE)");
            DuckDbBulkInserter.insertRows(connection, "bulk_test", columns, rows, types);
            return readAllRows(connection);
        }
    }

    private static List<List<Object>> readAllRows(Connection connection) throws SQLException {
        List<List<Object>> loaded = new ArrayList<>();
        try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM bulk_test")) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.add(resultSet.getObject(i));
                }
                loaded.add(row);
            }
        }
        return loaded;
    }

    private static List<List<Object>> buildRows(int rowCount) {
        List<List<Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(List.of("bucket-" + (i % 100), (double) (i % 7)));
        }
        return rows;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws SQLException;
    }
}
