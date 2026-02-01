package io.duckcluster.coordinator.grpc;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.proto.v1.CoordinatorServiceGrpc;
import io.duckcluster.proto.v1.HeartbeatRequest;
import io.duckcluster.proto.v1.HeartbeatResponse;
import io.duckcluster.proto.v1.RegisterWorkerRequest;
import io.duckcluster.proto.v1.RegisterWorkerResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public final class CoordinatorGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcServer.class);

    private final ClusterConfig config;
    private final WorkerRegistry registry;
    private Server server;

    public CoordinatorGrpcServer(ClusterConfig config, WorkerRegistry registry) {
        this.config = config;
        this.registry = registry;
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
            LOG.info(
                    "Registered worker {} at {}:{}",
                    request.getWorkerId(),
                    request.getHost(),
                    request.getPort());

            RegisterWorkerResponse response = RegisterWorkerResponse.newBuilder()
                    .setAccepted(true)
                    .setMessage("registered")
                    .build();
            responseObserver.onNext(response);
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
