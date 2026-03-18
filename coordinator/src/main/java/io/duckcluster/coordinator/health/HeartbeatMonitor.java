package io.duckcluster.coordinator.health;

import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.replication.ShardReplicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class HeartbeatMonitor implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatMonitor.class);

    private final WorkerRegistry registry;
    private final ShardCatalog shardCatalog;
    private final ShardReplicator shardReplicator;
    private final Duration heartbeatTimeout;
    private final ScheduledExecutorService executor;

    public HeartbeatMonitor(WorkerRegistry registry, ShardCatalog shardCatalog,
                            ShardReplicator shardReplicator, Duration heartbeatInterval,
                            int missThreshold) {
        this.registry = registry;
        this.shardCatalog = shardCatalog;
        this.shardReplicator = shardReplicator;
        this.heartbeatTimeout = heartbeatInterval.multipliedBy(missThreshold);
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-monitor");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        long checkIntervalMs = heartbeatTimeout.toMillis();
        executor.scheduleWithFixedDelay(this::checkWorkers, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("HeartbeatMonitor started (timeout={}s)", heartbeatTimeout.toSeconds());
    }

    private void checkWorkers() {
        try {
            Instant deadline = Instant.now().minus(heartbeatTimeout);
            List<String> toRemove = new ArrayList<>();

            for (WorkerRegistry.WorkerRecord worker : registry.listWorkers()) {
                if (worker.lastHeartbeatAt().isBefore(deadline)) {
                    toRemove.add(worker.workerId());
                }
            }

            for (String workerId : toRemove) {
                LOG.warn("Worker {} missed heartbeat deadline, removing from cluster", workerId);
                registry.removeWorker(workerId);
                shardCatalog.onWorkerRemoved(workerId);
            }

            if (!toRemove.isEmpty()) {
                shardReplicator.triggerReconcile();
            }
        } catch (Exception e) {
            LOG.warn("HeartbeatMonitor check error: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
        LOG.info("HeartbeatMonitor stopped");
    }
}
