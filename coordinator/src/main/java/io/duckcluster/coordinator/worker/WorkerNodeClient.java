package io.duckcluster.coordinator.worker;

import io.duckcluster.common.merger.RowBatchConverter;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.proto.v1.FragmentRequest;
import io.duckcluster.proto.v1.MergeHint;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class WorkerNodeClient {
    public List<RowBatchData> executeFragment(
            WorkerRegistry.WorkerRecord worker, String queryId, FragmentSpec fragment) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(worker.host(), worker.port())
                .usePlaintext()
                .build();
        try {
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
        } finally {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
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
