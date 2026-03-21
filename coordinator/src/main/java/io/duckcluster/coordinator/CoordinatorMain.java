package io.duckcluster.coordinator;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.planner.CalciteQueryPlanner;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.common.routing.ConsistentHashRing;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.execution.QueryExecutionService;
import io.duckcluster.coordinator.grpc.CoordinatorGrpcServer;
import io.duckcluster.coordinator.health.HeartbeatMonitor;
import io.duckcluster.coordinator.http.CoordinatorHttpServer;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.replication.ShardReplicator;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CoordinatorMain {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorMain.class);

    public static void main(String[] args) throws Exception {
        ClusterConfig config = ClusterConfig.fromEnvironment();
        WorkerRegistry registry = new WorkerRegistry();
        ClusterCatalog catalog = ClusterCatalog.demo(config.shardCount());

        ConsistentHashRing ring = new ConsistentHashRing(config.vnodesPerWorker());
        ShardCatalog shardCatalog = new ShardCatalog(ring, config.replicationFactor());
        WorkerNodeClient workerClient = new WorkerNodeClient();

        QueryExecutionService queryExecutionService = new QueryExecutionService(
                new CalciteQueryPlanner(),
                catalog,
                registry,
                workerClient,
                new MergeStrategyRegistry(),
                shardCatalog);

        ShardReplicator shardReplicator = new ShardReplicator(shardCatalog, registry, workerClient);
        HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(
                registry, shardCatalog, shardReplicator,
                config.heartbeatInterval(), config.heartbeatMissThreshold());

        CoordinatorGrpcServer grpcServer = new CoordinatorGrpcServer(config, registry, shardCatalog, shardReplicator);
        CoordinatorHttpServer httpServer = new CoordinatorHttpServer(config, registry, queryExecutionService);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down coordinator");
            heartbeatMonitor.close();
            shardReplicator.close();
            httpServer.stop();
            grpcServer.stop();
        }));

        grpcServer.start();
        httpServer.start();
        shardReplicator.start();
        heartbeatMonitor.start();

        LOG.info(
                "Coordinator ready (HTTP {}:{}, gRPC {}:{})",
                config.coordinatorHost(),
                config.coordinatorHttpPort(),
                config.coordinatorHost(),
                config.coordinatorGrpcPort());

        Thread.currentThread().join();
    }
}
