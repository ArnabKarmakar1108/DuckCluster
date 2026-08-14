package io.duckcluster.coordinator.execution;

import java.util.concurrent.Semaphore;

/** Limits how many queries may run concurrently on the coordinator. */
public final class QueryAdmissionControl {
    private final Semaphore slots;
    private volatile boolean draining;

    public QueryAdmissionControl(int maxConcurrentQueries) {
        this.slots = new Semaphore(Math.max(1, maxConcurrentQueries), true);
    }

    public void beginDrain() {
        draining = true;
    }

    public boolean isDraining() {
        return draining;
    }

    public boolean tryAcquire() {
        if (draining) {
            return false;
        }
        return slots.tryAcquire();
    }

    public void acquire() throws InterruptedException {
        slots.acquire();
    }

    public void release() {
        slots.release();
    }

    public int availablePermits() {
        return slots.availablePermits();
    }

    public int maxPermits() {
        return slots.availablePermits() + slots.getQueueLength();
    }
}
