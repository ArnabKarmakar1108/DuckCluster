package io.duckcluster.coordinator.merger;

import io.duckcluster.common.duckdb.DuckDbResourceLimits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
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
    private static final long CHECKOUT_WAIT_MS = 30_000L;

    private static volatile CoordinatorDuckDbPool instance;

    static CoordinatorDuckDbPool get() {
        CoordinatorDuckDbPool pool = instance;
        if (pool == null) {
            synchronized (CoordinatorDuckDbPool.class) {
                pool = instance;
                if (pool == null) {
                    pool = new CoordinatorDuckDbPool(4, 2048, null);
                    instance = pool;
                }
            }
        }
        return pool;
    }

    static void initialize(int poolSize, long memoryLimitMb, Path tempDirectory) {
        synchronized (CoordinatorDuckDbPool.class) {
            if (instance != null) {
                instance.close();
            }
            instance = new CoordinatorDuckDbPool(poolSize, memoryLimitMb, tempDirectory);
        }
    }

    private final ArrayBlockingQueue<Connection> pool;
    private final long memoryLimitMb;
    private final Path tempDirectory;
    private volatile boolean closed;

    private CoordinatorDuckDbPool(int poolSize, long memoryLimitMb, Path tempDirectory) {
        this.memoryLimitMb = memoryLimitMb;
        this.tempDirectory = tempDirectory;
        this.pool = new ArrayBlockingQueue<>(poolSize);
        try {
            if (tempDirectory != null) {
                Files.createDirectories(tempDirectory);
            }
            for (int i = 0; i < poolSize; i++) {
                pool.add(createConnection());
            }
            LOG.info(
                    "Coordinator merge DuckDB pool ready (size={}, memoryLimitMb={}, tempDir={})",
                    poolSize,
                    memoryLimitMb,
                    tempDirectory);
        } catch (Exception e) {
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

    private Connection createConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_URL);
        DuckDbResourceLimits.apply(connection, memoryLimitMb, 0, tempDirectory);
        return connection;
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
