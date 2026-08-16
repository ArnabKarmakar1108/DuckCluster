package io.duckcluster.worker;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.worker.client.CoordinatorClient;
import io.duckcluster.worker.duckdb.BroadcastMaterializer;
import io.duckcluster.worker.duckdb.DuckDBConnectionPool;
import io.duckcluster.worker.duckdb.FragmentExecutor;
import io.duckcluster.worker.duckdb.RunningFragmentRegistry;
import io.duckcluster.worker.duckdb.ShardFileWatcher;
import io.duckcluster.worker.duckdb.ShardManager;
import io.duckcluster.worker.duckdb.ShardOwnershipReporter;
import io.duckcluster.worker.duckdb.TempShardCache;
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

        Path dimensionsDir = dataDir.resolve("dimensions");
        if (Files.exists(dimensionsDir)) {
            shardManager.scanAndAttachDimensions(dimensionsDir);
        }

        Path duckdbTempDir = dataDir.resolve(".duckdb-tmp");
        Files.createDirectories(duckdbTempDir);
        DuckDBConnectionPool pool = new DuckDBConnectionPool(
                dbPath, config.poolSize(), config.poolWaitMs(), config.workerMemoryLimitMb(), duckdbTempDir);
        RunningFragmentRegistry runningFragments = new RunningFragmentRegistry();
        FragmentExecutor fragmentExecutor = new FragmentExecutor(pool, runningFragments);
        BroadcastMaterializer broadcastMaterializer = new BroadcastMaterializer(pool);
        CoordinatorClient coordinatorClient = new CoordinatorClient(config);

        Path cacheDir = dataDir.resolve(".cache");
        Path transferTempDir = dataDir.resolve(".transfer-tmp");
        Files.createDirectories(transferTempDir);
        TempShardCache tempShardCache = new TempShardCache(
                shardManager, coordinatorClient, workerId, cacheDir, config.cacheMaxShards(), config.shardPinLeaseMs());

        WorkerGrpcServer workerServer = new WorkerGrpcServer(
                workerId, host, port, fragmentExecutor, runningFragments, shardManager, tempShardCache,
                broadcastMaterializer, transferTempDir, config.grpcMaxInboundMessageBytes());

        ShardFileWatcher watcher = new ShardFileWatcher(
                dataDir, shardManager, coordinatorClient, workerId, config.watcherIntervalMs());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down worker {}", workerId);
            long drainDeadline = System.currentTimeMillis() + 30_000;
            while (fragmentExecutor.inFlightCount() > 0 && System.currentTimeMillis() < drainDeadline) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            watcher.close();
            tempShardCache.close();
            workerServer.stop();
            pool.close();
            shardManager.close();
        }));

        workerServer.start();

        coordinatorClient.setReconnectHandler(() -> registerWithCoordinator(
                coordinatorClient, shardManager, workerId, host, port, numThreads));
        registerWithCoordinator(coordinatorClient, shardManager, workerId, host, port, numThreads);
        coordinatorClient.startHeartbeat(
                workerId, config.heartbeatInterval(), () -> fragmentExecutor.loadFactor(numThreads));

        LOG.info("Worker {} ready at {}:{} (pool size={}, shards={}, coordinator gRPC {}:{})",
                workerId, host, port, config.poolSize(), shardManager.getAttachedShards().size(),
                config.coordinatorHost(), config.coordinatorGrpcPort());

        Thread.currentThread().join();
    }

    private static void registerWithCoordinator(
            CoordinatorClient coordinatorClient,
            ShardManager shardManager,
            String workerId,
            String host,
            int port,
            int numThreads) {
        var ownedShards = ShardOwnershipReporter.toOwnership(shardManager, shardManager.getAttachedShards());
        coordinatorClient.registerWorker(workerId, host, port, numThreads, ownedShards);
    }
}
