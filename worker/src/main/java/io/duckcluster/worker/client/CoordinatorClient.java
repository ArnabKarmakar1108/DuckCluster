package io.duckcluster.worker.client;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.proto.v1.CoordinatorServiceGrpc;
import io.duckcluster.proto.v1.HeartbeatRequest;
import io.duckcluster.proto.v1.HeartbeatResponse;
import io.duckcluster.proto.v1.RegisterWorkerRequest;
import io.duckcluster.proto.v1.RegisterWorkerResponse;
import io.duckcluster.proto.v1.ShardOwnership;
import io.duckcluster.proto.v1.UpdateShardCacheRequest;
import io.duckcluster.proto.v1.UpdateShardRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleSupplier;

public final class CoordinatorClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorClient.class);

    private final ClusterConfig config;
    private final ManagedChannel channel;
    private final CoordinatorServiceGrpc.CoordinatorServiceBlockingStub blockingStub;
    private final CoordinatorServiceGrpc.CoordinatorServiceStub asyncStub;
    private ScheduledExecutorService heartbeatExecutor;
    private volatile StreamObserver<HeartbeatRequest> heartbeatRequestObserver;
    private volatile long lastCoordinatorBootEpoch;
    private volatile Runnable reconnectHandler;
    private final AtomicBoolean heartbeatStreamOpen = new AtomicBoolean();

    public CoordinatorClient(ClusterConfig config) {
        this.config = config;
        this.channel = ManagedChannelBuilder
                .forAddress(config.coordinatorHost(), config.coordinatorGrpcPort())
                .usePlaintext()
                .build();
        this.blockingStub = CoordinatorServiceGrpc.newBlockingStub(channel);
        this.asyncStub = CoordinatorServiceGrpc.newStub(channel);
    }

    public void setReconnectHandler(Runnable reconnectHandler) {
        this.reconnectHandler = reconnectHandler;
    }

    public void registerWorker(String workerId, String host, int port, int numThreads,
                               List<ShardOwnership> ownedShards) {
        RegisterWorkerRequest request = RegisterWorkerRequest.newBuilder()
                .setWorkerId(workerId)
                .setHost(host)
                .setPort(port)
                .setNumThreads(numThreads)
                .addAllOwnedShards(ownedShards)
                .build();
        RegisterWorkerResponse response = blockingStub.registerWorker(request);
        if (!response.getAccepted()) {
            throw new IllegalStateException("Worker registration rejected: " + response.getMessage());
        }
        observeBootEpoch(response.getCoordinatorBootEpoch(), workerId);
        LOG.info("Registered with coordinator as {} ({} shards)", workerId, ownedShards.size());
    }

    public void updateShardOwnership(String workerId, List<ShardOwnership> ownedShards) {
        UpdateShardRequest request = UpdateShardRequest.newBuilder()
                .setWorkerId(workerId)
                .addAllOwnedShards(ownedShards)
                .build();
        blockingStub.updateShardOwnership(request);
        LOG.debug("Updated shard ownership: {} shards", ownedShards.size());
    }

    public void updateShardCache(String workerId, List<io.duckcluster.worker.duckdb.TempShardCache.CacheEntry> cachedEntries) {
        List<ShardOwnership> cachedShards = cachedEntries.stream()
                .map(e -> ShardOwnership.newBuilder()
                        .setTableName(e.tableName())
                        .setShardId(e.shardId())
                        .build())
                .toList();
        UpdateShardCacheRequest request = UpdateShardCacheRequest.newBuilder()
                .setWorkerId(workerId)
                .addAllCachedShards(cachedShards)
                .build();
        blockingStub.updateShardCache(request);
        LOG.debug("Updated shard cache: {} cached shards", cachedShards.size());
    }

    public void startHeartbeat(String workerId, Duration interval, DoubleSupplier loadSupplier) {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "worker-heartbeat");
            thread.setDaemon(true);
            return thread;
        });

        openHeartbeatStream(workerId);

        heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (!heartbeatStreamOpen.get()) {
                openHeartbeatStream(workerId);
            }
            StreamObserver<HeartbeatRequest> observer = heartbeatRequestObserver;
            if (observer == null) {
                return;
            }
            try {
                double load = loadSupplier == null ? 0.0 : loadSupplier.getAsDouble();
                observer.onNext(HeartbeatRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setLoad(load)
                        .build());
            } catch (Exception e) {
                LOG.warn("Failed to send heartbeat for {}: {}", workerId, e.getMessage());
                heartbeatStreamOpen.set(false);
            }
        }, 0, interval.toSeconds(), TimeUnit.SECONDS);
    }

    private void openHeartbeatStream(String workerId) {
        StreamObserver<HeartbeatResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(HeartbeatResponse value) {
                if (!value.getHealthy()) {
                    LOG.warn("Coordinator marked heartbeat unhealthy for {}", workerId);
                }
                observeBootEpoch(value.getCoordinatorBootEpoch(), workerId);
            }

            @Override
            public void onError(Throwable t) {
                LOG.warn("Heartbeat response stream error for {}: {}", workerId, t.getMessage());
                heartbeatStreamOpen.set(false);
                scheduleReconnect(workerId);
            }

            @Override
            public void onCompleted() {
                LOG.info("Heartbeat response stream completed for {}", workerId);
                heartbeatStreamOpen.set(false);
                scheduleReconnect(workerId);
            }
        };

        heartbeatRequestObserver = asyncStub.heartbeat(responseObserver);
        heartbeatStreamOpen.set(true);
        LOG.debug("Opened heartbeat stream for {}", workerId);
    }

    private void scheduleReconnect(String workerId) {
        if (heartbeatExecutor == null) {
            return;
        }
        heartbeatExecutor.schedule(() -> {
            Runnable handler = reconnectHandler;
            if (handler != null) {
                try {
                    LOG.info("Re-registering worker {} after coordinator reconnect", workerId);
                    handler.run();
                } catch (Exception e) {
                    LOG.warn("Worker re-registration failed for {}: {}", workerId, e.getMessage());
                }
            }
            openHeartbeatStream(workerId);
        }, 2, TimeUnit.SECONDS);
    }

    private void observeBootEpoch(long bootEpoch, String workerId) {
        if (bootEpoch == 0) {
            return;
        }
        long previous = lastCoordinatorBootEpoch;
        if (previous != 0 && previous != bootEpoch) {
            LOG.warn("Coordinator boot epoch changed for {} ({} -> {}), re-registering",
                    workerId, previous, bootEpoch);
            Runnable handler = reconnectHandler;
            if (handler != null) {
                try {
                    handler.run();
                } catch (Exception e) {
                    LOG.warn("Worker re-registration after boot epoch change failed: {}", e.getMessage());
                }
            }
        }
        lastCoordinatorBootEpoch = bootEpoch;
    }

    @Override
    public void close() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        channel.shutdown();
    }
}
