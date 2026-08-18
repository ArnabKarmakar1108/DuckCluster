package io.duckcluster.coordinator.config;

import io.duckcluster.common.config.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logs the effective coordinator configuration at startup (8.1). */
public final class EffectiveClusterConfigLogger {
    private static final Logger LOG = LoggerFactory.getLogger(EffectiveClusterConfigLogger.class);

    private EffectiveClusterConfigLogger() {}

    public static void log(ClusterConfig config) {
        LOG.info(
                "Effective config: http={}:{} grpc={} workers(maxQueries={}, maxFragments={}, mergePool={}) "
                        + "replication(factor={}, maxConcurrent={}, reconcileMs={}) "
                        + "rpc(deadlineMs={}, grpcInboundMb={}, compression={}) "
                        + "fragments(batchRows={}, transferChunkBytes={})",
                config.coordinatorHttpBindHost(),
                config.coordinatorHttpPort(),
                config.coordinatorGrpcPort(),
                config.maxConcurrentQueries(),
                config.maxConcurrentFragments(),
                config.mergePoolSize(),
                config.replicationFactor(),
                config.replicationMaxConcurrent(),
                config.replicationReconcileIntervalMs(),
                config.workerRpcDeadlineMs(),
                config.grpcMaxInboundMessageBytes() / (1024 * 1024),
                config.grpcUseCompression(),
                config.fragmentBatchRows(),
                config.shardTransferChunkBytes());
    }
}
