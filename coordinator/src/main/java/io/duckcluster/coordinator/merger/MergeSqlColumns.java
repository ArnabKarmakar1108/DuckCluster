package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.planner.MergeSqlBuilder;

import java.util.ArrayList;
import java.util.List;

final class MergeSqlColumns {
    private MergeSqlColumns() {}

    static List<String> fromContext(MergeContext context) {
        List<String> columns = MergeSqlBuilder.tempTableColumns(context.plan().analysis());
        if (isValid(columns)) {
            return columns;
        }
        return columnsFromFragments(context);
    }

    static List<String> outputColumns(MergeContext context) {
        List<String> columns = new ArrayList<>(context.plan().analysis().outputColumnNames());
        if (isValid(columns)) {
            return columns;
        }
        return columnsFromFragments(context);
    }

    static List<String> outputColumns(QueryAnalysis analysis) {
        return new ArrayList<>(analysis.outputColumnNames());
    }

    private static List<String> columnsFromFragments(MergeContext context) {
        for (FragmentResult fragment : context.fragmentResults()) {
            for (RowBatchData batch : fragment.batches()) {
                List<String> columnNames = batch.columnNames();
                if (isValid(columnNames)) {
                    return new ArrayList<>(columnNames);
                }
            }
        }
        throw new IllegalStateException("No fragment column metadata available for merge");
    }

    private static boolean isValid(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return false;
        }
        for (String column : columns) {
            if (column == null || column.isBlank() || "*".equals(column)) {
                return false;
            }
        }
        return true;
    }
}
