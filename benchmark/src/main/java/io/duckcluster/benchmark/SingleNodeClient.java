package io.duckcluster.benchmark;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class SingleNodeClient implements AutoCloseable {

    private final Connection connection;

    public SingleNodeClient(Path duckdbPath) throws Exception {
        this.connection = DriverManager.getConnection("jdbc:duckdb:" + duckdbPath.toAbsolutePath());
        connection.setAutoCommit(true);
    }

    public DuckClusterClient.QueryExecution execute(String sql) throws Exception {
        long start = System.nanoTime();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData meta = resultSet.getMetaData();
            int columnCount = meta.getColumnCount();
            List<String> columns = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columns.add(meta.getColumnLabel(i));
            }

            List<List<Object>> rows = new ArrayList<>();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.add(resultSet.getObject(i));
                }
                rows.add(row);
            }

            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            return new DuckClusterClient.QueryExecution(elapsedMs, elapsedMs, columns, rows, Map.of());
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
