package io.duckcluster.worker.grpc;

import io.duckcluster.proto.v1.FragmentRequest;
import io.duckcluster.proto.v1.PingRequest;
import io.duckcluster.proto.v1.PingResponse;
import io.duckcluster.proto.v1.ReadShardRequest;
import io.duckcluster.proto.v1.ReceiveShardResponse;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.ShardDataChunk;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.duckcluster.worker.duckdb.FragmentExecutor;
import io.duckcluster.worker.duckdb.ShardFileMetadata;
import io.duckcluster.worker.duckdb.ShardManager;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class WorkerGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerGrpcServer.class);
    private static final int CHUNK_SIZE = 64 * 1024;

    private final String workerId;
    private final String host;
    private final int port;
    private final FragmentExecutor fragmentExecutor;
    private final ShardManager shardManager;
    private Server server;

    public WorkerGrpcServer(String workerId, String host, int port,
                            FragmentExecutor fragmentExecutor, ShardManager shardManager) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.fragmentExecutor = fragmentExecutor;
        this.shardManager = shardManager;
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

        @Override
        public void readShard(ReadShardRequest request, StreamObserver<ShardDataChunk> responseObserver) {
            String tableName = request.getTableName();
            int shardId = request.getShardId();
            Optional<ShardFileMetadata> meta = shardManager.getShard(tableName, shardId);
            if (meta.isEmpty()) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Shard not found: " + tableName + "_shard" + shardId)
                        .asRuntimeException());
                return;
            }
            Path filePath = meta.get().filePath();
            LOG.info("Streaming shard file {} for replication", filePath);
            try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
                byte[] buffer = new byte[CHUNK_SIZE];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    ShardDataChunk chunk = ShardDataChunk.newBuilder()
                            .setTableName(tableName)
                            .setShardId(shardId)
                            .setData(ByteString.copyFrom(buffer, 0, bytesRead))
                            .setIsLast(false)
                            .build();
                    responseObserver.onNext(chunk);
                }
                responseObserver.onNext(ShardDataChunk.newBuilder()
                        .setTableName(tableName)
                        .setShardId(shardId)
                        .setData(ByteString.EMPTY)
                        .setIsLast(true)
                        .build());
                responseObserver.onCompleted();
            } catch (IOException e) {
                LOG.error("Error reading shard file {}: {}", filePath, e.getMessage());
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Failed to read shard file: " + e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public StreamObserver<ShardDataChunk> receiveShard(StreamObserver<ReceiveShardResponse> responseObserver) {
            return new StreamObserver<>() {
                private FileOutputStream outputStream;
                private Path tempPath;
                private Path finalPath;
                private String tableName;
                private int shardId;

                @Override
                public void onNext(ShardDataChunk chunk) {
                    try {
                        if (outputStream == null) {
                            tableName = chunk.getTableName();
                            shardId = chunk.getShardId();
                            String fileName = tableName + "_shard" + shardId + ".duckdb";
                            finalPath = shardManager.getDataDir().resolve(fileName);
                            tempPath = shardManager.getDataDir().resolve(fileName + ".tmp");
                            outputStream = new FileOutputStream(tempPath.toFile());
                            LOG.info("Receiving shard: {}_shard{}", tableName, shardId);
                        }
                        if (!chunk.getData().isEmpty()) {
                            chunk.getData().writeTo(outputStream);
                        }
                        if (chunk.getIsLast()) {
                            outputStream.close();
                            outputStream = null;
                            Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
                            LOG.info("Shard received and placed: {}", finalPath);
                        }
                    } catch (IOException e) {
                        LOG.error("Error writing shard chunk: {}", e.getMessage());
                        cleanup();
                        responseObserver.onError(Status.INTERNAL
                                .withDescription("Failed to write shard: " + e.getMessage())
                                .asRuntimeException());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    LOG.warn("ReceiveShard stream error: {}", t.getMessage());
                    cleanup();
                }

                @Override
                public void onCompleted() {
                    cleanup();
                    responseObserver.onNext(ReceiveShardResponse.newBuilder()
                            .setAccepted(true)
                            .setMessage("Shard received: " + tableName + "_shard" + shardId)
                            .build());
                    responseObserver.onCompleted();
                }

                private void cleanup() {
                    if (outputStream != null) {
                        try {
                            outputStream.close();
                        } catch (IOException ignored) {}
                        outputStream = null;
                    }
                }
            };
        }
    }
}
