package io.duckcluster.coordinator;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.planner.CalciteQueryPlanner;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.common.routing.ConsistentHashRing;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.config.EffectiveClusterConfigLogger;
import io.duckcluster.coordinator.execution.AsyncQueryService;
import io.duckcluster.coordinator.execution.QueryAdmissionControl;
import io.duckcluster.coordinator.execution.QueryExecutionService;
import io.duckcluster.coordinator.grpc.CoordinatorGrpcServer;
import io.duckcluster.coordinator.health.HeartbeatMonitor;
import io.duckcluster.coordinator.http.CoordinatorHttpServer;
import io.duckcluster.coordinator.merger.CoordinatorMergeRuntime;
import io.duckcluster.coordinator.merger.MergeExecutionService;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.metadata.MetadataSnapshotService;
import io.duckcluster.coordinator.monitor.CoordinatorMetrics;
import io.duckcluster.coordinator.monitor.MonitorService;
import io.duckcluster.coordinator.monitor.QueryActivityTracker;
import io.duckcluster.coordinator.replication.ShardReplicator;
import io.duckcluster.coordinator.worker.WorkerChannelPool;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public final class CoordinatorMain {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorMain.class);

    public static void main(String[] args) throws Exception {
        ClusterConfig config = ClusterConfig.fromEnvironment();
        EffectiveClusterConfigLogger.log(config);
        WorkerRegistry registry = new WorkerRegistry();
        ClusterCatalog catalog = new ClusterCatalog();

        Path mergeTempDir = Path.of(config.dataDir()).resolve(".merge-tmp");
        CoordinatorMergeRuntime.initialize(config.mergePoolSize(), config.mergeMemoryLimitMb(), mergeTempDir);

        ConsistentHashRing ring = new ConsistentHashRing(config.vnodesPerWorker());
        ShardCatalog shardCatalog = new ShardCatalog(ring, config.replicationFactor(), catalog);
        WorkerChannelPool channelPool = new WorkerChannelPool(config.grpcMaxInboundMessageBytes());
        WorkerNodeClient workerClient =
                new WorkerNodeClient(channelPool, config.workerRpcDeadlineMs(), config.grpcUseCompression());

        QueryActivityTracker activityTracker = new QueryActivityTracker();
        QueryAdmissionControl admission = new QueryAdmissionControl(config.maxConcurrentQueries());
        MergeExecutionService mergeExecutionService = new MergeExecutionService(config.mergePoolSize());
        QueryExecutionService queryExecutionService = new QueryExecutionService(
                new CalciteQueryPlanner(),
                catalog,
                registry,
                workerClient,
                new MergeStrategyRegistry(),
                shardCatalog,
                config,
                activityTracker,
                admission,
                mergeExecutionService);

        MonitorService monitorService = new MonitorService(
                config, registry, shardCatalog, catalog, activityTracker, queryExecutionService);

        AsyncQueryService asyncQueryService =
                new AsyncQueryService(queryExecutionService, activityTracker, config, admission);

        ShardReplicator shardReplicator = new ShardReplicator(config, shardCatalog, registry, workerClient);
        HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(
                registry, shardCatalog, shardReplicator, channelPool,
                config.heartbeatInterval(),
                config.heartbeatSuspectThreshold(),
                config.heartbeatMissThreshold());

        MetadataSnapshotService metadataSnapshotService =
                new MetadataSnapshotService(config, shardCatalog, catalog);

        CoordinatorMetrics coordinatorMetrics = new CoordinatorMetrics(
                registry,
                shardCatalog,
                activityTracker,
                admission,
                config.maxConcurrentQueries(),
                config.heartbeatInterval().multipliedBy(config.heartbeatMissThreshold()));

        CoordinatorGrpcServer grpcServer = new CoordinatorGrpcServer(
                config, registry, shardCatalog, shardReplicator, channelPool);
        CoordinatorHttpServer httpServer = new CoordinatorHttpServer(
                config, registry, queryExecutionService, asyncQueryService, monitorService, coordinatorMetrics);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down coordinator");
            admission.beginDrain();
            try {
                queryExecutionService.awaitDrain(30_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            metadataSnapshotService.writeSnapshot();
            metadataSnapshotService.close();
            heartbeatMonitor.close();
            shardReplicator.close();
            httpServer.stop();
            grpcServer.stop();
            channelPool.close();
            mergeExecutionService.close();
            CoordinatorMergeRuntime.shutdown();
        }));

        grpcServer.start();
        httpServer.start();
        shardReplicator.start();
        heartbeatMonitor.start();
        metadataSnapshotService.writeSnapshot();

        LOG.info(
                "Coordinator ready (HTTP {}:{}, gRPC {}:{})",
                config.coordinatorHost(),
                config.coordinatorHttpPort(),
                config.coordinatorHost(),
                config.coordinatorGrpcPort());

        Thread.currentThread().join();
    }
}
