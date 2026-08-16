package io.duckcluster.worker.duckdb;

import io.duckcluster.common.duckdb.DuckDbResourceLimits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class DuckDBConnectionPool implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DuckDBConnectionPool.class);

    private final String jdbcUrl;
    private final ArrayBlockingQueue<Connection> pool;
    private final long waitMs;
    private final long memoryLimitMb;
    private final Path tempDirectory;
    private volatile boolean closed;

    public DuckDBConnectionPool(String dbPath, int poolSize, long waitMs) throws SQLException {
        this(dbPath, poolSize, waitMs, 0, null);
    }

    public DuckDBConnectionPool(String dbPath, int poolSize, long waitMs, long memoryLimitMb, Path tempDirectory)
            throws SQLException {
        this.jdbcUrl = "jdbc:duckdb:" + dbPath;
        this.pool = new ArrayBlockingQueue<>(poolSize);
        this.waitMs = waitMs;
        this.memoryLimitMb = memoryLimitMb;
        this.tempDirectory = tempDirectory;
        for (int i = 0; i < poolSize; i++) {
            pool.add(createConnection());
        }
        LOG.info(
                "Connection pool created: size={}, db={}, waitMs={}, memoryLimitMb={}",
                poolSize,
                dbPath,
                waitMs,
                memoryLimitMb);
    }

    public Connection checkout() throws InterruptedException {
        if (closed) {
            return null;
        }
        Connection conn = pool.poll(waitMs, TimeUnit.MILLISECONDS);
        if (conn == null) {
            return null;
        }
        try {
            if (!conn.isValid(1)) {
                LOG.warn("Replacing invalid connection from pool");
                conn.close();
                conn = createConnection();
            }
        } catch (SQLException e) {
            LOG.warn("Connection validation failed, creating replacement", e);
            try { conn.close(); } catch (SQLException ignored) {}
            try {
                conn = createConnection();
            } catch (SQLException ex) {
                LOG.error("Failed to create replacement connection", ex);
                return null;
            }
        }
        return conn;
    }

    public void checkin(Connection conn) {
        if (conn == null) {
            return;
        }
        if (closed) {
            closeQuietly(conn);
            return;
        }
        if (!pool.offer(conn)) {
            closeQuietly(conn);
        }
    }

    public boolean isExhausted() {
        return pool.isEmpty();
    }

    @Override
    public void close() {
        Connection conn;
        while ((conn = pool.poll()) != null) {
            closeQuietly(conn);
        }
        closed = true;
        LOG.info("Connection pool closed");
    }

    private Connection createConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl);
        DuckDbResourceLimits.apply(connection, memoryLimitMb, 0, tempDirectory);
        return connection;
    }

    private static void closeQuietly(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            LOG.warn("Failed to close connection", e);
        }
    }
}
