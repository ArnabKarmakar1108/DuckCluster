package io.duckcluster.worker;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.worker.client.CoordinatorClient;
import io.duckcluster.worker.grpc.WorkerGrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        WorkerGrpcServer workerServer = new WorkerGrpcServer(workerId, host, port);
        CoordinatorClient coordinatorClient = new CoordinatorClient(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down worker {}", workerId);
            workerServer.stop();
        }));

        workerServer.start();
        coordinatorClient.registerWorker(workerId, host, port, numThreads);
        coordinatorClient.startHeartbeat(workerId, config.heartbeatInterval());

        LOG.info("Worker {} ready at {}:{} (coordinator gRPC {}:{})",
                workerId, host, port, config.coordinatorHost(), config.coordinatorGrpcPort());

        Thread.currentThread().join();
    }
}
