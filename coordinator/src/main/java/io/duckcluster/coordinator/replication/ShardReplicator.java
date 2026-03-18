package io.duckcluster.coordinator.replication;

import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import io.duckcluster.proto.v1.ShardDataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class ShardReplicator implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ShardReplicator.class);
    private static final int MAX_CONCURRENT_REPLICATIONS = 2;
    private static final long RECONCILE_INTERVAL_SECONDS = 30;

    private final ShardCatalog shardCatalog;
    private final WorkerRegistry registry;
    private final WorkerNodeClient workerClient;
    private final ScheduledExecutorService executor;
    private final Semaphore replicationSlots = new Semaphore(MAX_CONCURRENT_REPLICATIONS);

    public ShardReplicator(ShardCatalog shardCatalog, WorkerRegistry registry,
                           WorkerNodeClient workerClient) {
        this.shardCatalog = shardCatalog;
        this.registry = registry;
        this.workerClient = workerClient;
        this.executor = Executors.newScheduledThreadPool(MAX_CONCURRENT_REPLICATIONS + 1, r -> {
            Thread thread = new Thread(r, "shard-replicator");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        executor.scheduleWithFixedDelay(
                this::reconcile, RECONCILE_INTERVAL_SECONDS, RECONCILE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        LOG.info("ShardReplicator started (interval={}s, maxConcurrent={})",
                RECONCILE_INTERVAL_SECONDS, MAX_CONCURRENT_REPLICATIONS);
    }

    public void triggerReconcile() {
        executor.submit(this::reconcile);
        LOG.info("Immediate reconciliation triggered (topology change)");
    }

    private void reconcile() {
        try {
            List<ShardCatalog.UnderReplicatedShard> underReplicated = shardCatalog.getUnderReplicatedShards();
            if (underReplicated.isEmpty()) {
                return;
            }
            LOG.info("Reconciliation found {} under-replicated shards", underReplicated.size());
            for (ShardCatalog.UnderReplicatedShard shard : underReplicated) {
                for (String targetWorkerId : shard.targetWorkers()) {
                    executor.submit(() -> replicateShard(shard, targetWorkerId));
                }
            }
        } catch (Exception e) {
            LOG.warn("Reconciliation error: {}", e.getMessage());
        }
    }

    private void replicateShard(ShardCatalog.UnderReplicatedShard shard, String targetWorkerId) {
        if (!replicationSlots.tryAcquire()) {
            LOG.debug("Replication slots full, deferring {}_shard{} to {}",
                    shard.tableName(), shard.shardId(), targetWorkerId);
            return;
        }
        try {
            Optional<WorkerRegistry.WorkerRecord> sourceOpt = registry.getWorker(shard.sourceWorker());
            Optional<WorkerRegistry.WorkerRecord> targetOpt = registry.getWorker(targetWorkerId);
            if (sourceOpt.isEmpty() || targetOpt.isEmpty()) {
                LOG.warn("Cannot replicate {}_shard{}: source or target worker not in registry",
                        shard.tableName(), shard.shardId());
                return;
            }

            LOG.info("Replicating {}_shard{} from {} to {}",
                    shard.tableName(), shard.shardId(), shard.sourceWorker(), targetWorkerId);

            Iterator<ShardDataChunk> chunks = workerClient.streamShardFrom(
                    sourceOpt.get(), shard.tableName(), shard.shardId());
            boolean success = workerClient.pushShardTo(targetOpt.get(), chunks);

            if (success) {
                LOG.info("Replication complete: {}_shard{} → {}",
                        shard.tableName(), shard.shardId(), targetWorkerId);
            } else {
                LOG.warn("Replication failed: {}_shard{} → {}",
                        shard.tableName(), shard.shardId(), targetWorkerId);
            }
        } catch (Exception e) {
            LOG.error("Replication error for {}_shard{}: {}",
                    shard.tableName(), shard.shardId(), e.getMessage());
        } finally {
            replicationSlots.release();
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
        LOG.info("ShardReplicator stopped");
    }
}
