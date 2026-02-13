package io.duckcluster.worker.grpc;

import io.duckcluster.proto.v1.FragmentRequest;
import io.duckcluster.proto.v1.PingRequest;
import io.duckcluster.proto.v1.PingResponse;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.duckcluster.worker.duckdb.FragmentExecutor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public final class WorkerGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerGrpcServer.class);

    private final String workerId;
    private final String host;
    private final int port;
    private final FragmentExecutor fragmentExecutor;
    private Server server;

    public WorkerGrpcServer(String workerId, String host, int port, FragmentExecutor fragmentExecutor) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.fragmentExecutor = fragmentExecutor;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new WorkerServiceImpl())
                .build()
                .start();
        LOG.info("Worker {} gRPC listening on {}:{}", workerId, host, port);
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

    private final class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
        @Override
        public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
            responseObserver.onNext(PingResponse.newBuilder()
                    .setOk(true)
                    .setWorkerId(workerId)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void executeFragment(FragmentRequest request, StreamObserver<RowBatch> responseObserver) {
            LOG.info("Executing fragment {} for query {}", request.getFragmentId(), request.getQueryId());
            fragmentExecutor.execute(request.getSql(), responseObserver);
        }
    }
}
