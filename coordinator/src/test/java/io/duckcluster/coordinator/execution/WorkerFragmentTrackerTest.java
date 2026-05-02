package io.duckcluster.coordinator.execution;

import io.duckcluster.common.registry.WorkerRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerFragmentTrackerTest {

    @Test
    void blocksUntilWorkerHasCapacity() {
        WorkerRegistry registry = new WorkerRegistry();
        registry.register("worker-1", "127.0.0.1", 9101, 1);
        WorkerFragmentTracker tracker = new WorkerFragmentTracker(registry);

        tracker.acquireBlocking("worker-1", 1_000);
        AtomicBoolean waiterAcquired = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            tracker.acquireBlocking("worker-1", 2_000);
            waiterAcquired.set(true);
            tracker.release("worker-1");
        });
        waiter.start();

        sleep(200);
        assertEquals(1, tracker.inFlightCount("worker-1"));

        tracker.release("worker-1");
        joinQuietly(waiter);

        assertTrue(waiterAcquired.get());
        assertEquals(0, tracker.inFlightCount("worker-1"));
    }

    @Test
    void timesOutWhenWorkerNeverFreesCapacity() {
        WorkerRegistry registry = new WorkerRegistry();
        registry.register("worker-1", "127.0.0.1", 9101, 1);
        WorkerFragmentTracker tracker = new WorkerFragmentTracker(registry);

        tracker.acquireBlocking("worker-1", 1_000);

        assertThrows(IllegalStateException.class, () -> tracker.acquireBlocking("worker-1", 200));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void joinQuietly(Thread thread) {
        try {
            thread.join(3_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
