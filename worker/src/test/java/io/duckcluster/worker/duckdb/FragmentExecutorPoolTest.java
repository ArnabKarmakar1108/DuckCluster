package io.duckcluster.worker.duckdb;

import io.duckcluster.proto.v1.RowBatch;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class FragmentExecutorPoolTest {

    @TempDir
    Path tempDir;

    private DuckDBConnectionPool pool;
    private FragmentExecutor executor;
    private String dbPath;

    @BeforeEach
    void setUp() throws Exception {
        dbPath = tempDir.resolve("test.db").toString();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE events (id INTEGER, name VARCHAR, value INTEGER, category VARCHAR)");
            stmt.execute("INSERT INTO events VALUES (1,'e1',10,'A'), (2,'e2',20,'B'), (3,'e3',30,'A')");
        }
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void execute_returnsResourceExhausted_whenPoolExhausted() throws Exception {
        pool = new DuckDBConnectionPool(dbPath, 1, 50);
        executor = new FragmentExecutor(pool);

        Connection held = pool.checkout();
        assertNotNull(held);

        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        executor.execute("SELECT * FROM events", new StreamObserver<>() {
            @Override public void onNext(RowBatch value) { fail("Should not receive data"); }
            @Override public void onError(Throwable t) { error.set(t); done.countDown(); }
            @Override public void onCompleted() { fail("Should not complete"); }
        });

        done.await();
        assertNotNull(error.get());
        assertInstanceOf(StatusRuntimeException.class, error.get());
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(),
                ((StatusRuntimeException) error.get()).getStatus().getCode());

        pool.checkin(held);
    }

    @Test
    void execute_concurrentFragments_correctResults() throws Exception {
        pool = new DuckDBConnectionPool(dbPath, 2, 500);
        executor = new FragmentExecutor(pool);

        int threadCount = 4;
        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        AtomicInteger completedCount = new AtomicInteger();
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(threadPool.submit(() -> {
                CountDownLatch latch = new CountDownLatch(1);
                List<RowBatch> batches = Collections.synchronizedList(new ArrayList<>());

                executor.execute("SELECT category, COUNT(*) as cnt FROM events GROUP BY category ORDER BY category",
                        new StreamObserver<>() {
                            @Override public void onNext(RowBatch value) { batches.add(value); }
                            @Override public void onError(Throwable t) { errors.add(t); latch.countDown(); }
                            @Override public void onCompleted() { completedCount.incrementAndGet(); latch.countDown(); }
                        });

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                if (!batches.isEmpty()) {
                    assertEquals(2, batches.get(0).getRowCount());
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        threadPool.shutdown();

        assertTrue(errors.isEmpty(), "Errors: " + errors);
        assertEquals(threadCount, completedCount.get());
    }
}
