package io.duckcluster.coordinator.worker;

import io.duckcluster.common.registry.WorkerRegistry;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotSame;

class WorkerChannelPoolTest {

    @Test
    void removeChannelForcesNewChannelOnNextLookup() {
        WorkerChannelPool pool = new WorkerChannelPool();
        WorkerRegistry.WorkerRecord worker = new WorkerRegistry.WorkerRecord(
                "worker-1",
                "127.0.0.1",
                9101,
                2,
                WorkerRegistry.WorkerStatus.HEALTHY,
                java.time.Instant.now(),
                java.time.Instant.now(),
                0.0);

        ManagedChannel first = pool.getChannel(worker);
        pool.removeChannel(worker.workerId());
        ManagedChannel second = pool.getChannel(worker);

        assertNotSame(first, second);
        pool.close();
    }
}
