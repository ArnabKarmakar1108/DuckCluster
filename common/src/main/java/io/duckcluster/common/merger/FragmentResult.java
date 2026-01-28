package io.duckcluster.common.merger;

import java.util.List;

public record FragmentResult(
        int fragmentId, int shardId, String workerId, long durationMs, List<RowBatchData> batches) {}
