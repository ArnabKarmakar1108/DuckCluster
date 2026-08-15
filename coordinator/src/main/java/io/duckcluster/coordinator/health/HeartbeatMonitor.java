package io.duckcluster.coordinator.health;

import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.replication.ShardReplicator;
import io.duckcluster.coordinator.worker.WorkerChannelPool;
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
    private final WorkerChannelPool channelPool;
    private final Duration heartbeatInterval;
    private final Duration suspectTimeout;
    private final Duration deadTimeout;
    private final ScheduledExecutorService executor;

    public HeartbeatMonitor(
            WorkerRegistry registry,
            ShardCatalog shardCatalog,
            ShardReplicator shardReplicator,
            WorkerChannelPool channelPool,
            Duration heartbeatInterval,
            int suspectMissThreshold,
            int deadMissThreshold) {
        this.registry = registry;
        this.shardCatalog = shardCatalog;
        this.shardReplicator = shardReplicator;
        this.channelPool = channelPool;
        this.heartbeatInterval = heartbeatInterval;
        this.suspectTimeout = heartbeatInterval.multipliedBy(suspectMissThreshold);
        this.deadTimeout = heartbeatInterval.multipliedBy(deadMissThreshold);
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-monitor");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        long checkIntervalMs = heartbeatInterval.toMillis();
        executor.scheduleWithFixedDelay(this::checkWorkers, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "HeartbeatMonitor started (suspect={}s, dead={}s)",
                suspectTimeout.toSeconds(),
                deadTimeout.toSeconds());
    }

    private void checkWorkers() {
        try {
            Instant now = Instant.now();
            Instant suspectDeadline = now.minus(suspectTimeout);
            Instant deadDeadline = now.minus(deadTimeout);
            List<String> toRemove = new ArrayList<>();

            for (WorkerRegistry.WorkerRecord worker : registry.listWorkers()) {
                Instant lastHeartbeat = worker.lastHeartbeatAt();
                if (lastHeartbeat.isBefore(deadDeadline)) {
                    toRemove.add(worker.workerId());
                } else if (lastHeartbeat.isBefore(suspectDeadline)
                        && worker.status() == WorkerRegistry.WorkerStatus.HEALTHY) {
                    registry.markSuspect(worker.workerId());
                    LOG.warn(
                            "Worker {} marked SUSPECT (last heartbeat {}s ago)",
                            worker.workerId(),
                            Duration.between(lastHeartbeat, now).toSeconds());
                }
            }

            for (String workerId : toRemove) {
                LOG.warn("Worker {} missed dead heartbeat deadline, removing from cluster", workerId);
                registry.removeWorker(workerId);
                shardCatalog.onWorkerRemoved(workerId);
                channelPool.removeChannel(workerId);
            }

            if (!toRemove.isEmpty()) {
                LOG.info("Removed {} dead workers, triggered reconciliation", toRemove.size());
                shardReplicator.triggerReconcile();
            } else {
                LOG.debug("Heartbeat check: {} workers tracked", registry.workerCount());
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
