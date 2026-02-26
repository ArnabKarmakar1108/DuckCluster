package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.planner.MergeSqlBuilder;

import java.util.ArrayList;
import java.util.List;

final class MergeSqlColumns {
    private MergeSqlColumns() {}

    static List<String> fromContext(MergeContext context) {
        return MergeSqlBuilder.tempTableColumns(context.plan().analysis());
    }

    static List<String> outputColumns(QueryAnalysis analysis) {
        return new ArrayList<>(analysis.outputColumnNames());
    }
}
