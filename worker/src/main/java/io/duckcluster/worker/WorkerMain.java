package io.duckcluster.worker;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.worker.client.CoordinatorClient;
import io.duckcluster.worker.duckdb.FragmentExecutor;
import io.duckcluster.worker.duckdb.WorkerDemoDataLoader;
import io.duckcluster.worker.grpc.WorkerGrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public final class WorkerMain {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerMain.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WorkerMain <worker-id> <host> <port>");
            System.exit(1);
        }

        String workerId = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        int numThreads = args.length > 3 ? Integer.parseInt(args[3]) : 1;

        ClusterConfig config = ClusterConfig.fromEnvironment();
        int shardIndex = parseShardIndex(workerId);
        Connection connection = WorkerDemoDataLoader.openInMemoryConnection();
        WorkerDemoDataLoader.initialize(connection, shardIndex, config.shardCount());

        FragmentExecutor fragmentExecutor = new FragmentExecutor(connection);
        WorkerGrpcServer workerServer = new WorkerGrpcServer(workerId, host, port, fragmentExecutor);
        CoordinatorClient coordinatorClient = new CoordinatorClient(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down worker {}", workerId);
            workerServer.stop();
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("Failed to close DuckDB connection", e);
            }
        }));

        workerServer.start();
        coordinatorClient.registerWorker(workerId, host, port, numThreads);
        coordinatorClient.startHeartbeat(workerId, config.heartbeatInterval());

        LOG.info("Worker {} ready at {}:{} (coordinator gRPC {}:{})",
                workerId, host, port, config.coordinatorHost(), config.coordinatorGrpcPort());

        Thread.currentThread().join();
    }

    static int parseShardIndex(String workerId) {
        int dash = workerId.lastIndexOf('-');
        if (dash < 0 || dash == workerId.length() - 1) {
            return 0;
        }
        return Integer.parseInt(workerId.substring(dash + 1)) - 1;
    }
}
