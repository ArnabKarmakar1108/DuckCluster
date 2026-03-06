package io.duckcluster.worker.duckdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DuckDBConnectionPoolTest {

    @TempDir
    Path tempDir;

    private DuckDBConnectionPool pool;
    private String dbPath;

    @BeforeEach
    void setUp() throws Exception {
        dbPath = tempDir.resolve("test.db").toString();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test (id INTEGER, val VARCHAR)");
            stmt.execute("INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void checkout_returnsConnection() throws Exception {
        pool = new DuckDBConnectionPool(dbPath, 2, 200);

        Connection conn = pool.checkout();
        assertNotNull(conn);
        assertFalse(conn.isClosed());

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
        pool.checkin(conn);
    }

    @Test
    void checkout_returnsNull_whenExhausted() throws Exception {
        pool = new DuckDBConnectionPool(dbPath, 1, 50);

        Connection conn1 = pool.checkout();
        assertNotNull(conn1);

        long start = System.currentTimeMillis();
        Connection conn2 = pool.checkout();
        long elapsed = System.currentTimeMillis() - start;

        assertNull(conn2);
        assertTrue(elapsed >= 50, "Should have waited at least 50ms, waited " + elapsed + "ms");

        pool.checkin(conn1);
    }

    @Test
    void checkin_connectionCanBeReused() throws Exception {
        pool = new DuckDBConnectionPool(dbPath, 1, 200);

        Connection conn = pool.checkout();
        assertNotNull(conn);
        pool.checkin(conn);

        Connection conn2 = pool.checkout();
        assertNotNull(conn2);
        pool.checkin(conn2);
    }

    @Test
    void concurrentExecution_allSucceed() throws Exception {
        pool = new DuckDBConnectionPool(dbPath, 4, 500);
        int threadCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger();

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    Connection conn = pool.checkout();
                    assertNotNull(conn);
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test")) {
                        assertTrue(rs.next());
                        assertEquals(3, rs.getInt(1));
                        successCount.incrementAndGet();
                    } finally {
                        pool.checkin(conn);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();

        assertEquals(threadCount, successCount.get());
    }

    @Test
    void close_closesAllConnections() throws Exception {
        pool = new DuckDBConnectionPool(dbPath, 3, 200);

        Connection conn = pool.checkout();
        pool.checkin(conn);

        pool.close();
        assertTrue(pool.isExhausted());

        Connection after = pool.checkout();
        assertNull(after);
        pool = null;
    }

    @Test
    void isExhausted_reflectsPoolState() throws Exception {
        pool = new DuckDBConnectionPool(dbPath, 2, 50);

        assertFalse(pool.isExhausted());

        Connection c1 = pool.checkout();
        assertFalse(pool.isExhausted());

        Connection c2 = pool.checkout();
        assertTrue(pool.isExhausted());

        pool.checkin(c1);
        assertFalse(pool.isExhausted());

        pool.checkin(c2);
    }
}
