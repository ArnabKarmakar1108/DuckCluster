package io.duckcluster.worker.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class WorkerDemoDataLoader {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerDemoDataLoader.class);

    private WorkerDemoDataLoader() {}

    public static void initialize(Connection connection, int shardIndex, int shardCount) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE events (id INTEGER, name VARCHAR, value INTEGER, category VARCHAR)");
            StringBuilder insert = new StringBuilder("INSERT INTO events VALUES ");
            boolean first = true;
            for (int id = 1; id <= 9; id++) {
                if (id % shardCount != shardIndex) {
                    continue;
                }
                if (!first) {
                    insert.append(", ");
                }
                String category = categoryFor(id);
                insert.append(String.format("(%d, 'event-%d', %d, '%s')", id, id, id * 10, category));
                first = false;
            }
            if (!first) {
                statement.execute(insert.toString());
            }
            LOG.info("Loaded demo shard {}/{} into events table", shardIndex, shardCount);
        }
    }

    static String categoryFor(int id) {
        return switch (id % 3) {
            case 0 -> "A";
            case 1 -> "B";
            default -> "C";
        };
    }

    public static Connection openInMemoryConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:duckdb:");
    }
}
