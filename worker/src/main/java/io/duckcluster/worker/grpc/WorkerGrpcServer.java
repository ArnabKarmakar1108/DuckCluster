package io.duckcluster.worker.grpc;

import io.duckcluster.proto.v1.ActivateTableVersionRequest;
import io.duckcluster.proto.v1.ActivateTableVersionResponse;
import io.duckcluster.proto.v1.BeginShardPinRequest;
import io.duckcluster.proto.v1.BeginShardPinResponse;
import io.duckcluster.proto.v1.CancelQueryRequest;
import io.duckcluster.proto.v1.CancelQueryResponse;
import io.duckcluster.proto.v1.ClearBroadcastTablesRequest;
import io.duckcluster.proto.v1.ClearBroadcastTablesResponse;
import io.duckcluster.proto.v1.EndShardPinRequest;
import io.duckcluster.proto.v1.EndShardPinResponse;
import io.duckcluster.proto.v1.FragmentRequest;
import io.duckcluster.proto.v1.LoadTempDataResponse;
import io.duckcluster.proto.v1.MaterializeBroadcastRequest;
import io.duckcluster.proto.v1.MaterializeBroadcastResponse;
import io.duckcluster.proto.v1.PingRequest;
import io.duckcluster.proto.v1.PingResponse;
import io.duckcluster.proto.v1.ReadShardRequest;
import io.duckcluster.proto.v1.ReceiveShardResponse;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.ShardDataChunk;
import io.duckcluster.proto.v1.ShufflePieceChunk;
import io.duckcluster.proto.v1.ShufflePieceResponse;
import io.duckcluster.proto.v1.ShuffleTableRequest;
import io.duckcluster.proto.v1.ShuffleTableResponse;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.duckcluster.common.shard.ShardNameValidator;
import io.duckcluster.common.shard.ShardTransferChecksum;
import io.duckcluster.worker.duckdb.BroadcastMaterializer;
import io.duckcluster.worker.duckdb.FragmentExecutor;
import io.duckcluster.worker.duckdb.RunningFragmentRegistry;
import io.duckcluster.worker.duckdb.ShardFileMetadata;
import io.duckcluster.worker.duckdb.ShardManager;
import io.duckcluster.worker.duckdb.ShardVersionActivator;
import io.duckcluster.worker.duckdb.ShardOwnershipReporter;
import io.duckcluster.worker.duckdb.TempShardCache;
import io.duckcluster.worker.client.CoordinatorClient;
import io.duckcluster.worker.shuffle.ShuffleExchange;
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
import java.util.zip.CRC32;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class WorkerGrpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerGrpcServer.class);

    private final String workerId;
    private final String host;
    private final int port;
    private final FragmentExecutor fragmentExecutor;
    private final RunningFragmentRegistry runningFragments;
    private final ShardManager shardManager;
    private final TempShardCache tempShardCache;
    private final BroadcastMaterializer broadcastMaterializer;
    private final Path transferTempDir;
    private final int maxInboundMessageBytes;
    private final int shardTransferChunkBytes;
    private final CoordinatorClient coordinatorClient;
    private final ShuffleExchange shuffleExchange;
    private Server server;

    public WorkerGrpcServer(String workerId, String host, int port,
                            FragmentExecutor fragmentExecutor, RunningFragmentRegistry runningFragments,
                            ShardManager shardManager, TempShardCache tempShardCache,
                            BroadcastMaterializer broadcastMaterializer, Path transferTempDir,
                            int maxInboundMessageBytes, int shardTransferChunkBytes,
                            CoordinatorClient coordinatorClient, ShuffleExchange shuffleExchange) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.fragmentExecutor = fragmentExecutor;
        this.runningFragments = runningFragments;
        this.shardManager = shardManager;
        this.tempShardCache = tempShardCache;
        this.broadcastMaterializer = broadcastMaterializer;
        this.transferTempDir = transferTempDir;
        this.maxInboundMessageBytes = maxInboundMessageBytes;
        this.shardTransferChunkBytes = Math.max(4096, shardTransferChunkBytes);
        this.coordinatorClient = coordinatorClient;
        this.shuffleExchange = shuffleExchange;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .maxInboundMessageSize(maxInboundMessageBytes)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .permitKeepAliveTime(20, TimeUnit.SECONDS)
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
            fragmentExecutor.execute(
                    request.getSql(), request.getQueryId(), request.getFragmentId(), responseObserver);
        }

        @Override
        public void cancelQuery(CancelQueryRequest request, StreamObserver<CancelQueryResponse> responseObserver) {
            int cancelled = runningFragments.cancelQuery(request.getQueryId());
            responseObserver.onNext(CancelQueryResponse.newBuilder()
                    .setAccepted(true)
                    .setCancelledFragments(cancelled)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void activateTableVersion(
                ActivateTableVersionRequest request,
                StreamObserver<ActivateTableVersionResponse> responseObserver) {
            try {
                ShardNameValidator.validateTableName(request.getTableName());
                if (request.getVersion() < 1) {
                    throw new IllegalArgumentException("version must be >= 1");
                }
                ShardVersionActivator activator = new ShardVersionActivator(shardManager.getDataDir(), shardManager);
                int shards = activator.activate(request.getTableName(), request.getVersion());
                if (shards > 0) {
                    reportShardOwnership();
                }
                responseObserver.onNext(ActivateTableVersionResponse.newBuilder()
                        .setAccepted(true)
                        .setShardsActivated(shards)
                        .setMessage("activated " + shards + " shard file(s)")
                        .build());
                responseObserver.onCompleted();
            } catch (IllegalArgumentException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            } catch (Exception e) {
                LOG.error("ActivateTableVersion failed: {}", e.getMessage());
                responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            }
        }

        @Override
        public void readShard(ReadShardRequest request, StreamObserver<ShardDataChunk> responseObserver) {
            String tableName = request.getTableName();
            int shardId = request.getShardId();
            try {
                ShardNameValidator.validateTableName(tableName);
                ShardNameValidator.validateShardId(shardId);
            } catch (IllegalArgumentException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
                return;
            }
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
                byte[] buffer = new byte[shardTransferChunkBytes];
                CRC32 crc = ShardTransferChecksum.newCrc();
                long totalBytes = 0;
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    ShardTransferChecksum.update(crc, buffer, 0, bytesRead);
                    totalBytes += bytesRead;
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
                        .setContentLength(totalBytes)
                        .setCrc32((int) crc.getValue())
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
                private final CRC32 crc = ShardTransferChecksum.newCrc();
                private long bytesWritten;

                @Override
                public void onNext(ShardDataChunk chunk) {
                    if (responseFinished.get()) {
                        return;
                    }
                    try {
                        if (outputStream == null) {
                            tableName = chunk.getTableName();
                            shardId = chunk.getShardId();
                            ShardNameValidator.validateTableName(tableName);
                            ShardNameValidator.validateShardId(shardId);
                            String fileName = tableName + "_shard" + shardId + ".duckdb";
                            long stagingVersion = chunk.getStagingVersion();
                            if (stagingVersion > 0) {
                                Path stagingDir = shardManager.getDataDir()
                                        .resolve(".staging")
                                        .resolve("v" + stagingVersion);
                                Files.createDirectories(stagingDir);
                                finalPath = stagingDir.resolve(fileName);
                                ShardNameValidator.validatePathUnderBase(stagingDir, finalPath);
                            } else {
                                finalPath = shardManager.getDataDir().resolve(fileName);
                                ShardNameValidator.validatePathUnderBase(shardManager.getDataDir(), finalPath);
                            }
                            tempPath = transferTempDir.resolve(fileName + ".tmp");
                            ShardNameValidator.validatePathUnderBase(transferTempDir, tempPath);
                            outputStream = new FileOutputStream(tempPath.toFile());
                            LOG.info("Receiving shard: {}_shard{}", tableName, shardId);
                        }
                        if (!chunk.getData().isEmpty()) {
                            byte[] data = chunk.getData().toByteArray();
                            ShardTransferChecksum.update(crc, data, 0, data.length);
                            bytesWritten += data.length;
                            outputStream.write(data);
                        }
                        if (chunk.getIsLast()) {
                            receivedLast = true;
                            outputStream.close();
                            outputStream = null;
                            if (chunk.getContentLength() > 0) {
                                verifyTransferChecksum(bytesWritten, crc.getValue(), chunk);
                            }
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
                private String pinQueryId;
                private boolean receivedLast;
                private final CRC32 crc = ShardTransferChecksum.newCrc();
                private long bytesWritten;

                @Override
                public void onNext(ShardDataChunk chunk) {
                    if (responseFinished.get()) {
                        return;
                    }
                    try {
                        if (outputStream == null) {
                            tableName = chunk.getTableName();
                            shardId = chunk.getShardId();
                            pinQueryId = chunk.getQueryId();
                            ShardNameValidator.validateTableName(tableName);
                            ShardNameValidator.validateShardId(shardId);
                            tempPath = transferTempDir.resolve(
                                    tableName + "_shard" + shardId + "_" + System.nanoTime() + ".tmp");
                            outputStream = new FileOutputStream(tempPath.toFile());
                            LOG.info("Loading temp data: {}_shard{}", tableName, shardId);
                        }
                        if (!chunk.getData().isEmpty()) {
                            byte[] data = chunk.getData().toByteArray();
                            ShardTransferChecksum.update(crc, data, 0, data.length);
                            bytesWritten += data.length;
                            outputStream.write(data);
                        }
                        if (chunk.getIsLast()) {
                            receivedLast = true;
                            if (chunk.getContentLength() > 0) {
                                verifyTransferChecksum(bytesWritten, crc.getValue(), chunk);
                            }
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
                        String catalogName = tempShardCache.loadShard(
                                tableName, shardId, tempPath, pinQueryId);
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
        public void beginShardPin(BeginShardPinRequest request, StreamObserver<BeginShardPinResponse> responseObserver) {
            long leaseMs = request.getLeaseTtlSec() > 0
                    ? TimeUnit.SECONDS.toMillis(request.getLeaseTtlSec())
                    : 0L;
            tempShardCache.beginPinSession(request.getQueryId(), leaseMs);
            responseObserver.onNext(BeginShardPinResponse.newBuilder()
                    .setAccepted(true)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void endShardPin(EndShardPinRequest request, StreamObserver<EndShardPinResponse> responseObserver) {
            if (shouldInjectEndPinFailure()) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("injected EndShardPin failure (test hook)")
                        .asRuntimeException());
                return;
            }
            tempShardCache.endPinSession(request.getQueryId());
            responseObserver.onNext(EndShardPinResponse.newBuilder()
                    .setAccepted(true)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void materializeBroadcast(
                MaterializeBroadcastRequest request,
                StreamObserver<MaterializeBroadcastResponse> responseObserver) {
            try {
                String tempTable = broadcastMaterializer.materialize(
                        request.getQueryId(),
                        request.getTableName(),
                        request.getShardCount(),
                        request.getTableDataVersion());
                responseObserver.onNext(MaterializeBroadcastResponse.newBuilder()
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
                ClearBroadcastTablesRequest request,
                StreamObserver<ClearBroadcastTablesResponse> responseObserver) {
            try {
                broadcastMaterializer.clearQuery(request.getQueryId());
                shuffleExchange.clearQuery(request.getQueryId());
                responseObserver.onNext(ClearBroadcastTablesResponse.newBuilder()
                        .setAccepted(true)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("Failed to clear broadcast tables for query {}", request.getQueryId(), e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage()).asRuntimeException());
            }
        }

        @Override
        public void shuffleTable(ShuffleTableRequest request, StreamObserver<ShuffleTableResponse> responseObserver) {
            try {
                responseObserver.onNext(shuffleExchange.shuffle(request));
                responseObserver.onCompleted();
            } catch (IllegalArgumentException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            } catch (Exception e) {
                LOG.error("Shuffle of {} for query {} failed", request.getTableName(), request.getQueryId(), e);
                responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            }
        }

        @Override
        public StreamObserver<ShufflePieceChunk> receiveShufflePiece(
                StreamObserver<ShufflePieceResponse> responseObserver) {
            return shuffleExchange.receive(responseObserver);
        }

        private void reportShardOwnership() {
            coordinatorClient.updateShardOwnership(
                    workerId,
                    ShardOwnershipReporter.toOwnership(shardManager, shardManager.getAttachedShards()));
        }
    }

    private static boolean shouldInjectEndPinFailure() {
        return "1".equals(System.getenv("DUCKCLUSTER_TEST_FAIL_END_SHARD_PIN"))
                || "true".equalsIgnoreCase(System.getenv("DUCKCLUSTER_TEST_FAIL_END_SHARD_PIN"));
    }

    private static void verifyTransferChecksum(long bytesWritten, long crcValue, ShardDataChunk chunk)
            throws IOException {
        if (chunk.getContentLength() == 0) {
            return;
        }
        if (bytesWritten != chunk.getContentLength()) {
            throw new IOException(
                    "shard transfer length mismatch: expected " + chunk.getContentLength() + " got " + bytesWritten);
        }
        if ((int) crcValue != chunk.getCrc32()) {
            throw new IOException("shard transfer checksum mismatch");
        }
    }
}
