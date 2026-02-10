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

    public ClusterConfig(
            String coordinatorHost,
            int coordinatorGrpcPort,
            int coordinatorHttpPort,
            Duration heartbeatInterval,
            int heartbeatMissThreshold,
            int shardCount) {
        this.coordinatorHost = Objects.requireNonNull(coordinatorHost, "coordinatorHost");
        this.coordinatorGrpcPort = coordinatorGrpcPort;
        this.coordinatorHttpPort = coordinatorHttpPort;
        this.heartbeatInterval = Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        this.heartbeatMissThreshold = heartbeatMissThreshold;
        this.shardCount = shardCount;
    }

    public static ClusterConfig fromEnvironment() {
        return new ClusterConfig(
                getenv("DUCKCLUSTER_COORDINATOR_HOST", "127.0.0.1"),
                parseInt("DUCKCLUSTER_COORDINATOR_GRPC_PORT", 9090),
                parseInt("DUCKCLUSTER_COORDINATOR_HTTP_PORT", 8080),
                Duration.ofSeconds(parseInt("DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC", 5)),
                parseInt("DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD", 3),
                parseInt("DUCKCLUSTER_SHARD_COUNT", 3));
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
}
