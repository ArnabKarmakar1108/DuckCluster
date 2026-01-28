package io.duckcluster.common.merger;

import java.util.List;

public record RowBatchData(List<String> columnNames, List<List<String>> rows) {
    public RowBatchData {
        columnNames = List.copyOf(columnNames);
        rows = List.copyOf(rows);
    }
}
