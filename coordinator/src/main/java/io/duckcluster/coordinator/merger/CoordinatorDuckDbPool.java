package io.duckcluster.coordinator.merger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Bounded pool of in-memory coordinator DuckDB connections used for merge.
 * Tables are dropped on check-in so connections can be reused across queries.
 */
final class CoordinatorDuckDbPool implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorDuckDbPool.class);
    private static final String JDBC_URL = "jdbc:duckdb:";
    private static final int DEFAULT_POOL_SIZE = 4;
    private static final long CHECKOUT_WAIT_MS = 30_000L;

    private static final CoordinatorDuckDbPool INSTANCE = new CoordinatorDuckDbPool(DEFAULT_POOL_SIZE);

    static CoordinatorDuckDbPool get() {
        return INSTANCE;
    }

    private final ArrayBlockingQueue<Connection> pool;
    private volatile boolean closed;

    private CoordinatorDuckDbPool(int poolSize) {
        this.pool = new ArrayBlockingQueue<>(poolSize);
        try {
            for (int i = 0; i < poolSize; i++) {
                pool.add(createConnection());
            }
            LOG.info("Coordinator merge DuckDB pool ready (size={})", poolSize);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize coordinator DuckDB pool", e);
        }
    }

    Lease lease() throws SQLException {
        if (closed) {
            throw new SQLException("Coordinator DuckDB pool is closed");
        }
        try {
            Connection connection = pool.poll(CHECKOUT_WAIT_MS, TimeUnit.MILLISECONDS);
            if (connection == null) {
                throw new SQLException("Timed out waiting for coordinator DuckDB connection");
            }
            if (!connection.isValid(1)) {
                closeQuietly(connection);
                connection = createConnection();
            }
            return new Lease(connection);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted waiting for coordinator DuckDB connection", e);
        }
    }

    void checkin(Connection connection) {
        if (connection == null) {
            return;
        }
        if (closed) {
            closeQuietly(connection);
            return;
        }
        try {
            resetCatalog(connection);
        } catch (SQLException e) {
            LOG.warn("Failed to reset coordinator DuckDB catalog; discarding connection", e);
            closeQuietly(connection);
            try {
                if (!pool.offer(createConnection())) {
                    closeQuietly(connection);
                }
            } catch (SQLException ex) {
                LOG.error("Failed to replace coordinator DuckDB connection", ex);
            }
            return;
        }
        if (!pool.offer(connection)) {
            closeQuietly(connection);
        }
    }

    @Override
    public void close() {
        closed = true;
        Connection connection;
        while ((connection = pool.poll()) != null) {
            closeQuietly(connection);
        }
        LOG.info("Coordinator merge DuckDB pool closed");
    }

    private static void resetCatalog(Connection connection) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement query = connection.createStatement();
                ResultSet tables =
                        query.executeQuery(
                                "SELECT table_name FROM information_schema.tables "
                                        + "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'")) {
            while (tables.next()) {
                tableNames.add(tables.getString(1));
            }
        }
        if (tableNames.isEmpty()) {
            return;
        }
        try (Statement drop = connection.createStatement()) {
            for (String tableName : tableNames) {
                drop.execute("DROP TABLE IF EXISTS " + quoteIdentifier(tableName));
            }
        }
    }

    private static Connection createConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_URL);
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("Failed to close coordinator DuckDB connection", e);
        }
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    final class Lease implements AutoCloseable {
        private final Connection connection;

        private Lease(Connection connection) {
            this.connection = connection;
        }

        Connection connection() {
            return connection;
        }

        @Override
        public void close() {
            checkin(connection);
        }
    }
}
