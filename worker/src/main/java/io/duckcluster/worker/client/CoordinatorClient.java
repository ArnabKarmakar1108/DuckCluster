package io.duckcluster.worker.client;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.proto.v1.CoordinatorServiceGrpc;
import io.duckcluster.proto.v1.HeartbeatRequest;
import io.duckcluster.proto.v1.HeartbeatResponse;
import io.duckcluster.proto.v1.RegisterWorkerRequest;
import io.duckcluster.proto.v1.RegisterWorkerResponse;
import io.duckcluster.proto.v1.ShardOwnership;
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

public final class CoordinatorClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorClient.class);

    private final ClusterConfig config;
    private final ManagedChannel channel;
    private final CoordinatorServiceGrpc.CoordinatorServiceBlockingStub blockingStub;
    private final CoordinatorServiceGrpc.CoordinatorServiceStub asyncStub;
    private ScheduledExecutorService heartbeatExecutor;

    public CoordinatorClient(ClusterConfig config) {
        this.config = config;
        this.channel = ManagedChannelBuilder
                .forAddress(config.coordinatorHost(), config.coordinatorGrpcPort())
                .usePlaintext()
                .build();
        this.blockingStub = CoordinatorServiceGrpc.newBlockingStub(channel);
        this.asyncStub = CoordinatorServiceGrpc.newStub(channel);
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

    public void startHeartbeat(String workerId, Duration interval) {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "worker-heartbeat");
            thread.setDaemon(true);
            return thread;
        });

        StreamObserver<HeartbeatResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(HeartbeatResponse value) {
                if (!value.getHealthy()) {
                    LOG.warn("Coordinator marked heartbeat unhealthy for {}", workerId);
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.warn("Heartbeat response stream error for {}: {}", workerId, t.getMessage());
            }

            @Override
            public void onCompleted() {
                LOG.info("Heartbeat response stream completed for {}", workerId);
            }
        };

        StreamObserver<HeartbeatRequest> requestObserver = asyncStub.heartbeat(responseObserver);

        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                requestObserver.onNext(HeartbeatRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setLoad(0.0)
                        .build());
            } catch (Exception e) {
                LOG.warn("Failed to send heartbeat for {}: {}", workerId, e.getMessage());
            }
        }, 0, interval.toSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        channel.shutdown();
    }
}
