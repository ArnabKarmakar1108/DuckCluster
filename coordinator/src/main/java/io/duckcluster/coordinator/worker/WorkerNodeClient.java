package io.duckcluster.coordinator.worker;

import io.duckcluster.common.merger.RowBatchConverter;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.proto.v1.FragmentRequest;
import io.duckcluster.proto.v1.MergeHint;
import io.duckcluster.proto.v1.ReadShardRequest;
import io.duckcluster.proto.v1.LoadTempDataResponse;
import io.duckcluster.proto.v1.ReceiveShardResponse;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.ShardDataChunk;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkerNodeClient {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNodeClient.class);

    private final WorkerChannelPool channelPool;

    public WorkerNodeClient(WorkerChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    public List<RowBatchData> executeFragment(
            WorkerRegistry.WorkerRecord worker, String queryId, FragmentSpec fragment) {
        ManagedChannel channel = channelPool.getChannel(worker);
        WorkerServiceGrpc.WorkerServiceBlockingStub stub =
                WorkerServiceGrpc.newBlockingStub(channel);
        FragmentRequest request = FragmentRequest.newBuilder()
                .setQueryId(queryId)
                .setFragmentId(fragment.fragmentId())
                .setSql(fragment.sql())
                .setMergeHint(toMergeHint(fragment.mergeStrategy()))
                .build();

        List<RowBatchData> batches = new ArrayList<>();
        Iterator<RowBatch> iterator = stub.executeFragment(request);
        while (iterator.hasNext()) {
            batches.add(RowBatchConverter.fromProto(iterator.next()));
        }
        return batches;
    }

    public Iterator<ShardDataChunk> streamShardFrom(
            WorkerRegistry.WorkerRecord worker, String tableName, int shardId) {
        ManagedChannel channel = channelPool.getChannel(worker);
        WorkerServiceGrpc.WorkerServiceBlockingStub stub =
                WorkerServiceGrpc.newBlockingStub(channel);
        ReadShardRequest request = ReadShardRequest.newBuilder()
                .setTableName(tableName)
                .setShardId(shardId)
                .build();
        return stub.readShard(request);
    }

    public boolean pushShardTo(WorkerRegistry.WorkerRecord worker, Iterator<ShardDataChunk> chunks) {
        ManagedChannel channel = channelPool.getChannel(worker);
        try {
            WorkerServiceGrpc.WorkerServiceStub asyncStub = WorkerServiceGrpc.newStub(channel);
            AtomicBoolean success = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(1);

            StreamObserver<ShardDataChunk> requestObserver = asyncStub.receiveShard(
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

            latch.await(60, TimeUnit.SECONDS);
            return success.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean loadTempData(WorkerRegistry.WorkerRecord worker, Iterator<ShardDataChunk> chunks) {
        ManagedChannel channel = channelPool.getChannel(worker);
        try {
            WorkerServiceGrpc.WorkerServiceStub asyncStub = WorkerServiceGrpc.newStub(channel);
            AtomicBoolean success = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(1);

            StreamObserver<ShardDataChunk> requestObserver = asyncStub.loadTempData(
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
                requestObserver.onNext(chunks.next());
            }
            requestObserver.onCompleted();

            latch.await(60, TimeUnit.SECONDS);
            return success.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static MergeHint toMergeHint(MergeStrategyType mergeStrategy) {
        return switch (mergeStrategy) {
            case CONCATENATE -> MergeHint.CONCAT;
            case PARTIAL_AGG -> MergeHint.PARTIAL_AGG;
            case GROUP_BY_MERGE -> MergeHint.GROUP_BY;
            case TOP_K -> MergeHint.TOP_K;
        };
    }
}
