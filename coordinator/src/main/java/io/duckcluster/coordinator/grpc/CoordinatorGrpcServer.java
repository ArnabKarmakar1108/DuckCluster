package io.duckcluster.coordinator.grpc;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.proto.v1.CoordinatorServiceGrpc;
import io.duckcluster.proto.v1.HeartbeatRequest;
import io.duckcluster.proto.v1.HeartbeatResponse;
import io.duckcluster.proto.v1.RegisterWorkerRequest;
import io.duckcluster.proto.v1.RegisterWorkerResponse;
import io.duckcluster.proto.v1.UpdateShardRequest;
import io.duckcluster.proto.v1.UpdateShardResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.duckcluster.coordinator.replication.ShardReplicator;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public final class CoordinatorGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcServer.class);

    private final ClusterConfig config;
    private final WorkerRegistry registry;
    private final ShardCatalog shardCatalog;
    private final ShardReplicator shardReplicator;
    private Server server;

    public CoordinatorGrpcServer(ClusterConfig config, WorkerRegistry registry,
                                 ShardCatalog shardCatalog, ShardReplicator shardReplicator) {
        this.config = config;
        this.registry = registry;
        this.shardCatalog = shardCatalog;
        this.shardReplicator = shardReplicator;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(config.coordinatorGrpcPort())
                .addService(new CoordinatorServiceImpl())
                .build()
                .start();
        LOG.info("Coordinator gRPC listening on port {}", config.coordinatorGrpcPort());
    }

    public void stop() {
        if (server == null) {
            return;
        }
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private final class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
        @Override
        public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> responseObserver) {
            registry.register(
                    request.getWorkerId(),
                    request.getHost(),
                    request.getPort(),
                    request.getNumThreads());
            shardCatalog.onWorkerAdded(request.getWorkerId());
            if (!request.getOwnedShardsList().isEmpty()) {
                shardCatalog.registerOwnership(request.getWorkerId(), request.getOwnedShardsList());
            }
            LOG.info(
                    "Registered worker {} at {}:{} (ring size: {}, shards: {})",
                    request.getWorkerId(),
                    request.getHost(),
                    request.getPort(),
                    shardCatalog.getWorkerCount(),
                    request.getOwnedShardsCount());

            shardReplicator.triggerReconcile();

            RegisterWorkerResponse response = RegisterWorkerResponse.newBuilder()
                    .setAccepted(true)
                    .setMessage("registered")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void updateShardOwnership(UpdateShardRequest request, StreamObserver<UpdateShardResponse> responseObserver) {
            shardCatalog.registerOwnership(request.getWorkerId(), request.getOwnedShardsList());
            LOG.info("Updated shard ownership for worker {}: {} shards",
                    request.getWorkerId(), request.getOwnedShardsList().size());
            responseObserver.onNext(UpdateShardResponse.newBuilder().setAccepted(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<HeartbeatRequest> heartbeat(StreamObserver<HeartbeatResponse> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(HeartbeatRequest request) {
                    registry.heartbeat(request.getWorkerId(), request.getLoad());
                    responseObserver.onNext(
                            HeartbeatResponse.newBuilder().setHealthy(true).build());
                }

                @Override
                public void onError(Throwable t) {
                    LOG.warn("Heartbeat stream error: {}", t.getMessage());
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
