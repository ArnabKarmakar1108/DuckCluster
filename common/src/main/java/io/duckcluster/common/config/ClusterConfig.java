package io.duckcluster.common.config;

import java.time.Duration;
import java.util.Objects;

public final class ClusterConfig {
    private final String coordinatorHost;
    private final int coordinatorGrpcPort;
    private final int coordinatorHttpPort;
    private final Duration heartbeatInterval;
    private final int heartbeatMissThreshold;
    private final int shardCount;
    private final String dataDir;
    private final int poolSize;
    private final long poolWaitMs;
    private final int replicationFactor;
    private final int vnodesPerWorker;
    private final long watcherIntervalMs;

    public ClusterConfig(
            String coordinatorHost,
            int coordinatorGrpcPort,
            int coordinatorHttpPort,
            Duration heartbeatInterval,
            int heartbeatMissThreshold,
            int shardCount,
            String dataDir,
            int poolSize,
            long poolWaitMs,
            int replicationFactor,
            int vnodesPerWorker,
            long watcherIntervalMs) {
        this.coordinatorHost = Objects.requireNonNull(coordinatorHost, "coordinatorHost");
        this.coordinatorGrpcPort = coordinatorGrpcPort;
        this.coordinatorHttpPort = coordinatorHttpPort;
        this.heartbeatInterval = Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        this.heartbeatMissThreshold = heartbeatMissThreshold;
        this.shardCount = shardCount;
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir");
        this.poolSize = poolSize;
        this.poolWaitMs = poolWaitMs;
        this.replicationFactor = replicationFactor;
        this.vnodesPerWorker = vnodesPerWorker;
        this.watcherIntervalMs = watcherIntervalMs;
    }

    public static ClusterConfig fromEnvironment() {
        int defaultPoolSize = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() - 1));
        return new ClusterConfig(
                getenv("DUCKCLUSTER_COORDINATOR_HOST", "127.0.0.1"),
                parseInt("DUCKCLUSTER_COORDINATOR_GRPC_PORT", 9090),
                parseInt("DUCKCLUSTER_COORDINATOR_HTTP_PORT", 8080),
                Duration.ofSeconds(parseInt("DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC", 5)),
                parseInt("DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD", 3),
                parseInt("DUCKCLUSTER_SHARD_COUNT", 3),
                getenv("DUCKCLUSTER_DATA_DIR", "./data"),
                parseInt("DUCKCLUSTER_POOL_SIZE", defaultPoolSize),
                parseLong("DUCKCLUSTER_POOL_WAIT_MS", 200),
                parseInt("DUCKCLUSTER_REPLICATION_FACTOR", 2),
                parseInt("DUCKCLUSTER_VNODES_PER_WORKER", 100),
                parseLong("DUCKCLUSTER_WATCHER_INTERVAL_MS", 2000));
    }

    public String coordinatorHost() {
        return coordinatorHost;
    }

    public int coordinatorGrpcPort() {
        return coordinatorGrpcPort;
    }

    public int coordinatorHttpPort() {
        return coordinatorHttpPort;
    }

    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    public int heartbeatMissThreshold() {
        return heartbeatMissThreshold;
    }

    public int shardCount() {
        return shardCount;
    }

    public String dataDir() {
        return dataDir;
    }

    public int poolSize() {
        return poolSize;
    }

    public long poolWaitMs() {
        return poolWaitMs;
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    public int vnodesPerWorker() {
        return vnodesPerWorker;
    }

    public long watcherIntervalMs() {
        return watcherIntervalMs;
    }

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int parseInt(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static long parseLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
}
