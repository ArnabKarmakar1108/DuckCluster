package io.duckcluster.worker;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.proto.v1.ShardOwnership;
import io.duckcluster.worker.client.CoordinatorClient;
import io.duckcluster.worker.duckdb.DuckDBConnectionPool;
import io.duckcluster.worker.duckdb.FragmentExecutor;
import io.duckcluster.worker.duckdb.ShardFileWatcher;
import io.duckcluster.worker.duckdb.ShardManager;
import io.duckcluster.worker.grpc.WorkerGrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
        Path dataDir = Path.of(config.dataDir());
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        String dbPath = config.dataDir() + "/worker-" + workerId + ".db";

        ShardManager shardManager = new ShardManager(dbPath, dataDir);
        shardManager.scanAndAttachAll();

        DuckDBConnectionPool pool = new DuckDBConnectionPool(dbPath, config.poolSize(), config.poolWaitMs());
        FragmentExecutor fragmentExecutor = new FragmentExecutor(pool);
        WorkerGrpcServer workerServer = new WorkerGrpcServer(workerId, host, port, fragmentExecutor, shardManager);
        CoordinatorClient coordinatorClient = new CoordinatorClient(config);

        ShardFileWatcher watcher = new ShardFileWatcher(
                dataDir, shardManager, coordinatorClient, workerId, config.watcherIntervalMs());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down worker {}", workerId);
            watcher.close();
            workerServer.stop();
            pool.close();
            shardManager.close();
        }));

        workerServer.start();

        List<ShardOwnership> ownedShards = shardManager.getAttachedShards().stream()
                .map(meta -> ShardOwnership.newBuilder()
                        .setTableName(meta.tableName())
                        .setShardId(meta.shardId())
                        .setRowCount(0)
                        .build())
                .toList();
        coordinatorClient.registerWorker(workerId, host, port, numThreads, ownedShards);
        coordinatorClient.startHeartbeat(workerId, config.heartbeatInterval());

        LOG.info("Worker {} ready at {}:{} (pool size={}, shards={}, coordinator gRPC {}:{})",
                workerId, host, port, config.poolSize(), ownedShards.size(),
                config.coordinatorHost(), config.coordinatorGrpcPort());

        Thread.currentThread().join();
    }

}
