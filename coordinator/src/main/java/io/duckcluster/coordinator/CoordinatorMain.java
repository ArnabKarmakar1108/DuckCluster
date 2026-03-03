package io.duckcluster.coordinator;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.planner.CalciteQueryPlanner;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.execution.QueryExecutionService;
import io.duckcluster.coordinator.grpc.CoordinatorGrpcServer;
import io.duckcluster.coordinator.http.CoordinatorHttpServer;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CoordinatorMain {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorMain.class);

    public static void main(String[] args) throws Exception {
        ClusterConfig config = ClusterConfig.fromEnvironment();
        WorkerRegistry registry = new WorkerRegistry();
        ClusterCatalog catalog = ClusterCatalog.demo(config.shardCount());
        QueryExecutionService queryExecutionService = new QueryExecutionService(
                new CalciteQueryPlanner(),
                catalog,
                registry,
                new WorkerNodeClient(),
                new MergeStrategyRegistry());

        CoordinatorGrpcServer grpcServer = new CoordinatorGrpcServer(config, registry);
        CoordinatorHttpServer httpServer = new CoordinatorHttpServer(config, registry, queryExecutionService);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down coordinator");
            httpServer.stop();
            grpcServer.stop();
        }));

        grpcServer.start();
        httpServer.start();

        LOG.info(
                "Coordinator ready (HTTP {}:{}, gRPC {}:{})",
                config.coordinatorHost(),
                config.coordinatorHttpPort(),
                config.coordinatorHost(),
                config.coordinatorGrpcPort());

        Thread.currentThread().join();
    }
}
