package io.duckcluster.coordinator.worker;

import io.duckcluster.common.merger.RowBatchConverter;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.proto.v1.ActivateTableVersionRequest;
import io.duckcluster.proto.v1.ActivateTableVersionResponse;
import io.duckcluster.proto.v1.FragmentRequest;
import io.duckcluster.proto.v1.MergeHint;
import io.duckcluster.proto.v1.ReadShardRequest;
import io.duckcluster.proto.v1.LoadTempDataResponse;
import io.duckcluster.proto.v1.ReceiveShardResponse;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.ShardDataChunk;
import io.duckcluster.proto.v1.ShuffleTableRequest;
import io.duckcluster.proto.v1.ShuffleTableResponse;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.duckcluster.coordinator.execution.FragmentExecutionException;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkerNodeClient {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNodeClient.class);

    private final WorkerChannelPool channelPool;
    private final long rpcDeadlineMs;
    private final boolean useCompression;

    public WorkerNodeClient(WorkerChannelPool channelPool, long rpcDeadlineMs) {
        this(channelPool, rpcDeadlineMs, true);
    }

    public WorkerNodeClient(WorkerChannelPool channelPool, long rpcDeadlineMs, boolean useCompression) {
        this.channelPool = channelPool;
        this.rpcDeadlineMs = rpcDeadlineMs;
        this.useCompression = useCompression;
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub(WorkerRegistry.WorkerRecord worker) {
        ManagedChannel channel = channelPool.getChannel(worker);
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(rpcDeadlineMs, TimeUnit.MILLISECONDS);
        return useCompression ? stub.withCompression("gzip") : stub;
    }

    private WorkerServiceGrpc.WorkerServiceStub asyncStub(WorkerRegistry.WorkerRecord worker) {
        ManagedChannel channel = channelPool.getChannel(worker);
        WorkerServiceGrpc.WorkerServiceStub stub = WorkerServiceGrpc.newStub(channel)
                .withDeadlineAfter(rpcDeadlineMs, TimeUnit.MILLISECONDS);
        return useCompression ? stub.withCompression("gzip") : stub;
    }

    public List<RowBatchData> executeFragment(
            WorkerRegistry.WorkerRecord worker, String queryId, FragmentSpec fragment) {
        List<RowBatchData> batches = new ArrayList<>();
        consumeFragmentBatches(worker, queryId, fragment, batches::add);
        return batches;
    }

    public void consumeFragmentBatches(
            WorkerRegistry.WorkerRecord worker,
            String queryId,
            FragmentSpec fragment,
            Consumer<RowBatchData> batchConsumer) {
        FragmentRequest request = FragmentRequest.newBuilder()
                .setQueryId(queryId)
                .setFragmentId(fragment.fragmentId())
                .setSql(fragment.sql())
                .setMergeHint(toMergeHint(fragment.mergeStrategy()))
                .build();

        try {
            Iterator<RowBatch> iterator = blockingStub(worker).executeFragment(request);
            while (iterator.hasNext()) {
                batchConsumer.accept(RowBatchConverter.fromProto(iterator.next()));
            }
        } catch (StatusRuntimeException e) {
            throw new FragmentExecutionException(e.getStatus(), e);
        }
    }

    public void beginShardPin(WorkerRegistry.WorkerRecord worker, String queryId, long leaseTtlSec) {
        blockingStub(worker).beginShardPin(
                io.duckcluster.proto.v1.BeginShardPinRequest.newBuilder()
                        .setQueryId(queryId)
                        .setLeaseTtlSec(leaseTtlSec)
                        .build());
    }

    public void endShardPin(WorkerRegistry.WorkerRecord worker, String queryId) {
        blockingStub(worker).endShardPin(
                io.duckcluster.proto.v1.EndShardPinRequest.newBuilder()
                        .setQueryId(queryId)
                        .build());
    }

    public void materializeBroadcast(
            WorkerRegistry.WorkerRecord worker,
            String queryId,
            String tableName,
            int shardCount,
            long tableDataVersion) {
        io.duckcluster.proto.v1.MaterializeBroadcastResponse response =
                blockingStub(worker).materializeBroadcast(
                        io.duckcluster.proto.v1.MaterializeBroadcastRequest.newBuilder()
                                .setQueryId(queryId)
                                .setTableName(tableName)
                                .setShardCount(shardCount)
                                .setTableDataVersion(tableDataVersion)
                                .build());
        if (!response.getAccepted()) {
            throw new IllegalStateException(
                    "Worker " + worker.workerId() + " rejected broadcast materialization for " + tableName);
        }
    }

    public int cancelQuery(WorkerRegistry.WorkerRecord worker, String queryId) {
        io.duckcluster.proto.v1.CancelQueryResponse response = blockingStub(worker).cancelQuery(
                io.duckcluster.proto.v1.CancelQueryRequest.newBuilder()
                        .setQueryId(queryId)
                        .build());
        return response.getCancelledFragments();
    }

    public void clearBroadcastTables(WorkerRegistry.WorkerRecord worker, String queryId) {
        blockingStub(worker).clearBroadcastTables(
                io.duckcluster.proto.v1.ClearBroadcastTablesRequest.newBuilder()
                        .setQueryId(queryId)
                        .build());
    }

    public Iterator<ShardDataChunk> streamShardFrom(
            WorkerRegistry.WorkerRecord worker, String tableName, int shardId) {
        ReadShardRequest request = ReadShardRequest.newBuilder()
                .setTableName(tableName)
                .setShardId(shardId)
                .build();
        return blockingStub(worker).readShard(request);
    }

    public boolean loadTempData(WorkerRegistry.WorkerRecord worker, Iterator<ShardDataChunk> chunks) {
        return loadTempData(worker, chunks, null);
    }

    public boolean loadTempData(WorkerRegistry.WorkerRecord worker, Iterator<ShardDataChunk> chunks, String queryId) {
        try {
            AtomicBoolean success = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean timedOut = new AtomicBoolean(false);

            StreamObserver<ShardDataChunk> requestObserver = asyncStub(worker).loadTempData(
                    new StreamObserver<>() {
                        @Override
                        public void onNext(LoadTempDataResponse response) {
                            success.set(response.getAccepted());
                        }

                        @Override
                        public void onError(Throwable t) {
                            LOG.warn("loadTempData error: {}", t.getMessage());
                            latch.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            latch.countDown();
                        }
                    });

            while (chunks.hasNext()) {
                ShardDataChunk chunk = chunks.next();
                if (queryId != null && !queryId.isBlank()) {
                    chunk = chunk.toBuilder().setQueryId(queryId).build();
                }
                requestObserver.onNext(chunk);
            }
            requestObserver.onCompleted();

            if (!latch.await(rpcDeadlineMs, TimeUnit.MILLISECONDS)) {
                timedOut.set(true);
                requestObserver.onError(Status.DEADLINE_EXCEEDED.withDescription("loadTempData timed out").asRuntimeException());
            }
            return success.get() && !timedOut.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean pushShardTo(WorkerRegistry.WorkerRecord worker, Iterator<ShardDataChunk> chunks) {
        try {
            AtomicBoolean success = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean timedOut = new AtomicBoolean(false);

            StreamObserver<ShardDataChunk> requestObserver = asyncStub(worker).receiveShard(
                    new StreamObserver<>() {
                        @Override
                        public void onNext(ReceiveShardResponse response) {
                            success.set(response.getAccepted());
                        }

                        @Override
                        public void onError(Throwable t) {
                            LOG.warn("pushShardTo error: {}", t.getMessage());
                            latch.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            latch.countDown();
                        }
                    });

            while (chunks.hasNext()) {
                requestObserver.onNext(chunks.next());
            }
            requestObserver.onCompleted();

            if (!latch.await(rpcDeadlineMs, TimeUnit.MILLISECONDS)) {
                timedOut.set(true);
                requestObserver.onError(Status.DEADLINE_EXCEEDED.withDescription("pushShardTo timed out").asRuntimeException());
            }
            return success.get() && !timedOut.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public ShuffleTableResponse shuffleTable(WorkerRegistry.WorkerRecord worker, ShuffleTableRequest request) {
        ShuffleTableResponse response = blockingStub(worker).shuffleTable(request);
        if (!response.getAccepted()) {
            throw new IllegalStateException(
                    "Worker " + worker.workerId() + " rejected shuffle of " + request.getTableName());
        }
        return response;
    }

    public ActivateTableVersionResponse activateTableVersion(
            WorkerRegistry.WorkerRecord worker, String tableName, long version) {
        ActivateTableVersionRequest request = ActivateTableVersionRequest.newBuilder()
                .setTableName(tableName)
                .setVersion(version)
                .build();
        return blockingStub(worker).activateTableVersion(request);
    }

    private static MergeHint toMergeHint(MergeStrategyType mergeStrategy) {
        return switch (mergeStrategy) {
            case CONCATENATE -> MergeHint.CONCAT;
            case PARTIAL_AGG -> MergeHint.PARTIAL_AGG;
            case GROUP_BY_MERGE -> MergeHint.GROUP_BY;
            case NESTED_GROUP_BY_MERGE -> MergeHint.GROUP_BY;
            case WITH_CTE_MERGE -> MergeHint.GROUP_BY;
            case TOP_K -> MergeHint.TOP_K;
        };
    }
}
