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
            statement.execute("CREATE TABLE events (id INTEGER, name VARCHAR, value INTEGER)");
            StringBuilder insert = new StringBuilder("INSERT INTO events VALUES ");
            boolean first = true;
            for (int id = 1; id <= 9; id++) {
                if (id % shardCount != shardIndex) {
                    continue;
                }
                if (!first) {
                    insert.append(", ");
                }
                insert.append(String.format("(%d, 'event-%d', %d)", id, id, id * 10));
                first = false;
            }
            if (!first) {
                statement.execute(insert.toString());
            }
            LOG.info("Loaded demo shard {}/{} into events table", shardIndex, shardCount);
        }
    }

    public static Connection openInMemoryConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:duckdb:");
    }
}
