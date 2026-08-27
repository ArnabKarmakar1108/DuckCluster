package io.duckcluster.worker.duckdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BroadcastMaterializerTest {
    @TempDir
    Path tempDir;

    private DuckDBConnectionPool pool;
    private BroadcastMaterializer materializer;

    @BeforeEach
    void setUp() throws SQLException, InterruptedException {
        pool = new DuckDBConnectionPool(tempDir.resolve("worker.duckdb").toString(), 2, 5000);
        materializer = new BroadcastMaterializer(pool);
        seedShards();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void versionedBroadcastIsSharedAcrossQueries() throws Exception {
        String tableA = materializer.materialize("query-a", "events", 2, 2L);
        String tableB = materializer.materialize("query-b", "events", 2, 2L);
        assertEquals(tableA, tableB);
        assertTrue(tableA.startsWith("__dc_bcast_v2_"));

        materializer.clearQuery("query-a");
        Connection connection = pool.checkout();
        try (Statement statement = connection.createStatement()) {
            assertTableRowCount(statement, tableA, 2);
        } finally {
            pool.checkin(connection);
        }

        materializer.clearQuery("query-b");
        Connection connectionAfter = pool.checkout();
        try (Statement statement = connectionAfter.createStatement()) {
            assertTableMissing(statement, tableA);
        } finally {
            pool.checkin(connectionAfter);
        }
    }

    @Test
    void clearQueryDropsOnlyThatQueriesTables() throws Exception {
        String tableA = materializer.materialize("query-a", "events", 2);
        String tableB = materializer.materialize("query-b", "events", 2);

        materializer.clearQuery("query-a");

        Connection connection = pool.checkout();
        try (Statement statement = connection.createStatement()) {
            assertTableMissing(statement, tableA);
            assertTableRowCount(statement, tableB, 2);
        } finally {
            pool.checkin(connection);
        }

        materializer.clearQuery("query-b");
    }

    private void seedShards() throws SQLException, InterruptedException {
        Connection connection = pool.checkout();
        try (Statement statement = connection.createStatement()) {
            statement.execute("ATTACH ':memory:' AS events_shard0");
            statement.execute("CREATE TABLE events_shard0.events (id INT)");
            statement.execute("INSERT INTO events_shard0.events VALUES (1)");
            statement.execute("ATTACH ':memory:' AS events_shard1");
            statement.execute("CREATE TABLE events_shard1.events (id INT)");
            statement.execute("INSERT INTO events_shard1.events VALUES (2)");
        } finally {
            pool.checkin(connection);
        }
    }

    private static void assertTableMissing(Statement statement, String tableName) throws Exception {
        try (ResultSet rs = statement.executeQuery(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + tableName + "'")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    private static void assertTableRowCount(Statement statement, String tableName, int expectedRows) throws Exception {
        try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM \"" + tableName + "\"")) {
            assertTrue(rs.next());
            assertEquals(expectedRows, rs.getInt(1));
        }
    }
}
