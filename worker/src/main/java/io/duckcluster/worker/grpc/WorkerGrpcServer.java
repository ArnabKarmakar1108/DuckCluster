package io.duckcluster.worker.grpc;

import io.duckcluster.proto.v1.FragmentRequest;
import io.duckcluster.proto.v1.LoadTempDataResponse;
import io.duckcluster.proto.v1.PingRequest;
import io.duckcluster.proto.v1.PingResponse;
import io.duckcluster.proto.v1.ReadShardRequest;
import io.duckcluster.proto.v1.ReceiveShardResponse;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.ShardDataChunk;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.duckcluster.worker.duckdb.BroadcastMaterializer;
import io.duckcluster.worker.duckdb.FragmentExecutor;
import io.duckcluster.worker.duckdb.ShardFileMetadata;
import io.duckcluster.worker.duckdb.ShardManager;
import io.duckcluster.worker.duckdb.TempShardCache;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final TempShardCache tempShardCache;
    private final BroadcastMaterializer broadcastMaterializer;
    private Server server;

    public WorkerGrpcServer(String workerId, String host, int port,
                            FragmentExecutor fragmentExecutor, ShardManager shardManager,
                            TempShardCache tempShardCache, BroadcastMaterializer broadcastMaterializer) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.fragmentExecutor = fragmentExecutor;
        this.shardManager = shardManager;
        this.tempShardCache = tempShardCache;
        this.broadcastMaterializer = broadcastMaterializer;
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
                private final AtomicBoolean responseFinished = new AtomicBoolean(false);
                private FileOutputStream outputStream;
                private Path tempPath;
                private Path finalPath;
                private String tableName;
                private int shardId;
                private boolean receivedLast;

                @Override
                public void onNext(ShardDataChunk chunk) {
                    if (responseFinished.get()) {
                        return;
                    }
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
                            receivedLast = true;
                            outputStream.close();
                            outputStream = null;
                            Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
                            tempPath = null;
                            LOG.info("Shard received and placed: {}", finalPath);
                        }
                    } catch (IOException e) {
                        LOG.error("Error writing shard chunk: {}", e.getMessage());
                        fail("Failed to write shard: " + e.getMessage(), e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    LOG.warn("ReceiveShard stream error: {}", t.getMessage());
                    fail("Client stream failed: " + t.getMessage(), t);
                }

                @Override
                public void onCompleted() {
                    if (responseFinished.get()) {
                        return;
                    }
                    if (!receivedLast) {
                        fail("Shard stream ended before final chunk", null);
                        return;
                    }
                    succeed();
                }

                private void succeed() {
                    if (!responseFinished.compareAndSet(false, true)) {
                        return;
                    }
                    responseObserver.onNext(ReceiveShardResponse.newBuilder()
                            .setAccepted(true)
                            .setMessage("Shard received: " + tableName + "_shard" + shardId)
                            .build());
                    responseObserver.onCompleted();
                }

                private void fail(String description, Throwable cause) {
                    if (!responseFinished.compareAndSet(false, true)) {
                        return;
                    }
                    cleanup();
                    Status status = Status.INTERNAL.withDescription(description);
                    if (cause != null) {
                        status = status.withCause(cause);
                    }
                    responseObserver.onError(status.asRuntimeException());
                }

                private void cleanup() {
                    if (outputStream != null) {
                        try {
                            outputStream.close();
                        } catch (IOException ignored) {}
                        outputStream = null;
                    }
                    if (tempPath != null) {
                        try {
                            Files.deleteIfExists(tempPath);
                        } catch (IOException ignored) {}
                        tempPath = null;
                    }
                }
            };
        }

        @Override
        public StreamObserver<ShardDataChunk> loadTempData(StreamObserver<LoadTempDataResponse> responseObserver) {
            return new StreamObserver<>() {
                private final AtomicBoolean responseFinished = new AtomicBoolean(false);
                private FileOutputStream outputStream;
                private Path tempPath;
                private String tableName;
                private int shardId;
                private boolean receivedLast;

                @Override
                public void onNext(ShardDataChunk chunk) {
                    if (responseFinished.get()) {
                        return;
                    }
                    try {
                        if (outputStream == null) {
                            tableName = chunk.getTableName();
                            shardId = chunk.getShardId();
                            tempPath = Files.createTempFile("shard_cache_", ".duckdb.tmp");
                            outputStream = new FileOutputStream(tempPath.toFile());
                            LOG.info("Loading temp data: {}_shard{}", tableName, shardId);
                        }
                        if (!chunk.getData().isEmpty()) {
                            chunk.getData().writeTo(outputStream);
                        }
                        if (chunk.getIsLast()) {
                            receivedLast = true;
                            outputStream.close();
                            outputStream = null;
                        }
                    } catch (IOException e) {
                        LOG.error("Error writing temp data chunk: {}", e.getMessage());
                        fail("Failed to write temp data: " + e.getMessage(), e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    LOG.warn("LoadTempData stream error: {}", t.getMessage());
                    fail("Client stream failed: " + t.getMessage(), t);
                }

                @Override
                public void onCompleted() {
                    if (responseFinished.get()) {
                        return;
                    }
                    if (!receivedLast || tempPath == null) {
                        fail("Temp data stream ended before final chunk", null);
                        return;
                    }
                    try {
                        String catalogName = tempShardCache.loadShard(tableName, shardId, tempPath);
                        tempPath = null;
                        succeed(catalogName);
                    } catch (Exception e) {
                        LOG.error("Failed to cache shard {}_shard{}: {}", tableName, shardId, e.getMessage());
                        fail("Failed to cache shard: " + e.getMessage(), e);
                    }
                }

                private void succeed(String catalogName) {
                    if (!responseFinished.compareAndSet(false, true)) {
                        return;
                    }
                    responseObserver.onNext(LoadTempDataResponse.newBuilder()
                            .setAccepted(true)
                            .setCatalogName(catalogName)
                            .build());
                    responseObserver.onCompleted();
                }

                private void fail(String description, Throwable cause) {
                    if (!responseFinished.compareAndSet(false, true)) {
                        return;
                    }
                    cleanup();
                    Status status = Status.INTERNAL.withDescription(description);
                    if (cause != null) {
                        status = status.withCause(cause);
                    }
                    responseObserver.onError(status.asRuntimeException());
                }

                private void cleanup() {
                    if (outputStream != null) {
                        try {
                            outputStream.close();
                        } catch (IOException ignored) {}
                        outputStream = null;
                    }
                    if (tempPath != null) {
                        try {
                            Files.deleteIfExists(tempPath);
                        } catch (IOException ignored) {}
                        tempPath = null;
                    }
                }
            };
        }

        @Override
        public void beginShardPin(
                io.duckcluster.proto.v1.BeginShardPinRequest request,
                StreamObserver<io.duckcluster.proto.v1.BeginShardPinResponse> responseObserver) {
            tempShardCache.beginPinSession();
            responseObserver.onNext(io.duckcluster.proto.v1.BeginShardPinResponse.newBuilder()
                    .setAccepted(true)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void endShardPin(
                io.duckcluster.proto.v1.EndShardPinRequest request,
                StreamObserver<io.duckcluster.proto.v1.EndShardPinResponse> responseObserver) {
            tempShardCache.endPinSession();
            responseObserver.onNext(io.duckcluster.proto.v1.EndShardPinResponse.newBuilder()
                    .setAccepted(true)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void materializeBroadcast(
                io.duckcluster.proto.v1.MaterializeBroadcastRequest request,
                StreamObserver<io.duckcluster.proto.v1.MaterializeBroadcastResponse> responseObserver) {
            try {
                String tempTable = broadcastMaterializer.materialize(
                        request.getTableName(), request.getShardCount());
                responseObserver.onNext(io.duckcluster.proto.v1.MaterializeBroadcastResponse.newBuilder()
                        .setAccepted(true)
                        .setTempTableName(tempTable)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("Failed to materialize broadcast {} for query {}",
                        request.getTableName(), request.getQueryId(), e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage()).asRuntimeException());
            }
        }

        @Override
        public void clearBroadcastTables(
                io.duckcluster.proto.v1.ClearBroadcastTablesRequest request,
                StreamObserver<io.duckcluster.proto.v1.ClearBroadcastTablesResponse> responseObserver) {
            try {
                broadcastMaterializer.clearAll();
                responseObserver.onNext(io.duckcluster.proto.v1.ClearBroadcastTablesResponse.newBuilder()
                        .setAccepted(true)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("Failed to clear broadcast tables for query {}", request.getQueryId(), e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage()).asRuntimeException());
            }
        }
    }
}
