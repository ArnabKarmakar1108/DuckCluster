package io.duckcluster.common.config;

import java.time.Duration;
import java.util.Objects;

public final class ClusterConfig {
    private final String coordinatorHost;
    private final int coordinatorGrpcPort;
    private final int coordinatorHttpPort;
    private final String coordinatorHttpBindHost;
    private final Duration heartbeatInterval;
    private final int heartbeatMissThreshold;
    private final int shardCount;
    private final String dataDir;
    private final int poolSize;
    private final long poolWaitMs;
    private final int replicationFactor;
    private final int vnodesPerWorker;
    private final long watcherIntervalMs;
    private final int cacheMaxShards;
    private final long fragmentWaitMs;
    private final boolean logFragmentSql;
    private final long asyncResultTtlMs;
    private final int asyncMaxStoredJobs;
    private final long asyncMaxStoredBytes;
    private final long shardPinLeaseMs;
    private final long workerRpcDeadlineMs;
    private final int maxConcurrentFragments;
    private final int maxAsyncQueryThreads;
    private final int heartbeatSuspectThreshold;
    private final int maxConcurrentQueries;
    private final long queryTimeoutMs;
    private final int mergePoolSize;
    private final long mergeMemoryLimitMb;
    private final long workerMemoryLimitMb;
    private final long metadataSnapshotIntervalMs;
    private final int grpcMaxInboundMessageBytes;
    private final boolean grpcUseCompression;

    public ClusterConfig(
            String coordinatorHost,
            int coordinatorGrpcPort,
            int coordinatorHttpPort,
            String coordinatorHttpBindHost,
            Duration heartbeatInterval,
            int heartbeatMissThreshold,
            int shardCount,
            String dataDir,
            int poolSize,
            long poolWaitMs,
            int replicationFactor,
            int vnodesPerWorker,
            long watcherIntervalMs,
            int cacheMaxShards,
            long fragmentWaitMs,
            boolean logFragmentSql,
            long asyncResultTtlMs,
            int asyncMaxStoredJobs,
            long asyncMaxStoredBytes,
            long shardPinLeaseMs,
            long workerRpcDeadlineMs,
            int maxConcurrentFragments,
            int maxAsyncQueryThreads,
            int heartbeatSuspectThreshold,
            int maxConcurrentQueries,
            long queryTimeoutMs,
            int mergePoolSize,
            long mergeMemoryLimitMb,
            long workerMemoryLimitMb,
            long metadataSnapshotIntervalMs,
            int grpcMaxInboundMessageBytes,
            boolean grpcUseCompression) {
        this.coordinatorHost = Objects.requireNonNull(coordinatorHost, "coordinatorHost");
        this.coordinatorGrpcPort = coordinatorGrpcPort;
        this.coordinatorHttpPort = coordinatorHttpPort;
        this.coordinatorHttpBindHost =
                Objects.requireNonNull(coordinatorHttpBindHost, "coordinatorHttpBindHost");
        this.heartbeatInterval = Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        this.heartbeatMissThreshold = heartbeatMissThreshold;
        this.shardCount = shardCount;
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir");
        this.poolSize = poolSize;
        this.poolWaitMs = poolWaitMs;
        this.replicationFactor = replicationFactor;
        this.vnodesPerWorker = vnodesPerWorker;
        this.watcherIntervalMs = watcherIntervalMs;
        this.cacheMaxShards = cacheMaxShards;
        this.fragmentWaitMs = fragmentWaitMs;
        this.logFragmentSql = logFragmentSql;
        this.asyncResultTtlMs = asyncResultTtlMs;
        this.asyncMaxStoredJobs = asyncMaxStoredJobs;
        this.asyncMaxStoredBytes = asyncMaxStoredBytes;
        this.shardPinLeaseMs = shardPinLeaseMs;
        this.workerRpcDeadlineMs = workerRpcDeadlineMs;
        this.maxConcurrentFragments = maxConcurrentFragments;
        this.maxAsyncQueryThreads = maxAsyncQueryThreads;
        this.heartbeatSuspectThreshold = heartbeatSuspectThreshold;
        this.maxConcurrentQueries = maxConcurrentQueries;
        this.queryTimeoutMs = queryTimeoutMs;
        this.mergePoolSize = mergePoolSize;
        this.mergeMemoryLimitMb = mergeMemoryLimitMb;
        this.workerMemoryLimitMb = workerMemoryLimitMb;
        this.metadataSnapshotIntervalMs = metadataSnapshotIntervalMs;
        this.grpcMaxInboundMessageBytes = Math.max(1024 * 1024, grpcMaxInboundMessageBytes);
        this.grpcUseCompression = grpcUseCompression;
    }

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
            long watcherIntervalMs,
            int cacheMaxShards,
            long fragmentWaitMs) {
        this(coordinatorHost, coordinatorGrpcPort, coordinatorHttpPort, "0.0.0.0", heartbeatInterval,
                heartbeatMissThreshold,
                shardCount, dataDir, poolSize, poolWaitMs, replicationFactor, vnodesPerWorker, watcherIntervalMs,
                cacheMaxShards, fragmentWaitMs, false,
                Duration.ofMinutes(30).toMillis(), 200, 64L * 1024 * 1024,
                Duration.ofMinutes(10).toMillis(), Duration.ofMinutes(2).toMillis(), 64, 8,
                2, 32, Duration.ofMinutes(5).toMillis(), 4, 2048, 4096, Duration.ofMinutes(5).toMillis(),
                32 * 1024 * 1024,
                true);
    }

    public static ClusterConfig fromEnvironment() {
        int defaultPoolSize = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() - 1));
        return new ClusterConfig(
                getenv("DUCKCLUSTER_COORDINATOR_HOST", "127.0.0.1"),
                parseInt("DUCKCLUSTER_COORDINATOR_GRPC_PORT", 9090),
                parseInt("DUCKCLUSTER_COORDINATOR_HTTP_PORT", 8080),
                getenv("DUCKCLUSTER_COORDINATOR_HTTP_BIND_HOST", "0.0.0.0"),
                Duration.ofSeconds(parseInt("DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC", 5)),
                parseInt("DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD", 6),
                parseInt("DUCKCLUSTER_SHARD_COUNT", 3),
                getenv("DUCKCLUSTER_DATA_DIR", "./data"),
                parseInt("DUCKCLUSTER_POOL_SIZE", defaultPoolSize),
                parseLong("DUCKCLUSTER_POOL_WAIT_MS", 200),
                parseInt("DUCKCLUSTER_REPLICATION_FACTOR", 2),
                parseInt("DUCKCLUSTER_VNODES_PER_WORKER", 100),
                parseLong("DUCKCLUSTER_WATCHER_INTERVAL_MS", 2000),
                parseInt("DUCKCLUSTER_CACHE_MAX_SHARDS", 32),
                parseLong("DUCKCLUSTER_FRAGMENT_WAIT_MS", 60_000),
                parseBoolean("DUCKCLUSTER_LOG_FRAGMENT_SQL", false),
                Duration.ofSeconds(parseInt("DUCKCLUSTER_ASYNC_RESULT_TTL_SEC", 1800)).toMillis(),
                parseInt("DUCKCLUSTER_ASYNC_MAX_STORED_JOBS", 200),
                parseLong("DUCKCLUSTER_ASYNC_MAX_STORED_BYTES", 64L * 1024 * 1024),
                Duration.ofSeconds(parseInt("DUCKCLUSTER_SHARD_PIN_LEASE_SEC", 600)).toMillis(),
                Duration.ofSeconds(parseInt("DUCKCLUSTER_WORKER_RPC_DEADLINE_SEC", 120)).toMillis(),
                parseInt("DUCKCLUSTER_MAX_CONCURRENT_FRAGMENTS", 64),
                parseInt("DUCKCLUSTER_MAX_ASYNC_QUERY_THREADS", 8),
                parseInt("DUCKCLUSTER_HEARTBEAT_SUSPECT_THRESHOLD", 2),
                parseInt("DUCKCLUSTER_MAX_CONCURRENT_QUERIES", 32),
                Duration.ofSeconds(parseInt("DUCKCLUSTER_QUERY_TIMEOUT_SEC", 300)).toMillis(),
                parseInt("DUCKCLUSTER_MERGE_POOL_SIZE", 4),
                parseLong("DUCKCLUSTER_MERGE_MEMORY_LIMIT_MB", 2048),
                parseLong("DUCKCLUSTER_WORKER_MEMORY_LIMIT_MB", 4096),
                Duration.ofSeconds(parseInt("DUCKCLUSTER_METADATA_SNAPSHOT_INTERVAL_SEC", 300)).toMillis(),
                parseInt("DUCKCLUSTER_GRPC_MAX_INBOUND_MB", 32) * 1024 * 1024,
                parseBoolean("DUCKCLUSTER_GRPC_COMPRESSION", true));
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

    public String coordinatorHttpBindHost() {
        return coordinatorHttpBindHost;
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

    public int cacheMaxShards() {
        return cacheMaxShards;
    }

    public long fragmentWaitMs() {
        return fragmentWaitMs;
    }

    public boolean logFragmentSql() {
        return logFragmentSql;
    }

    public long asyncResultTtlMs() {
        return asyncResultTtlMs;
    }

    public int asyncMaxStoredJobs() {
        return asyncMaxStoredJobs;
    }

    public long asyncMaxStoredBytes() {
        return asyncMaxStoredBytes;
    }

    public long shardPinLeaseMs() {
        return shardPinLeaseMs;
    }

    public long workerRpcDeadlineMs() {
        return workerRpcDeadlineMs;
    }

    public int maxConcurrentFragments() {
        return maxConcurrentFragments;
    }

    public int maxAsyncQueryThreads() {
        return maxAsyncQueryThreads;
    }

    public int heartbeatSuspectThreshold() {
        return heartbeatSuspectThreshold;
    }

    public int maxConcurrentQueries() {
        return maxConcurrentQueries;
    }

    public long queryTimeoutMs() {
        return queryTimeoutMs;
    }

    public int mergePoolSize() {
        return mergePoolSize;
    }

    public long mergeMemoryLimitMb() {
        return mergeMemoryLimitMb;
    }

    public long workerMemoryLimitMb() {
        return workerMemoryLimitMb;
    }

    public long metadataSnapshotIntervalMs() {
        return metadataSnapshotIntervalMs;
    }

    public int grpcMaxInboundMessageBytes() {
        return grpcMaxInboundMessageBytes;
    }

    public boolean grpcUseCompression() {
        return grpcUseCompression;
    }

    private static boolean parseBoolean(String key, boolean defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
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
