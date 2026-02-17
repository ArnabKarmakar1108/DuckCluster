package io.duckcluster.common.merger;

import io.duckcluster.proto.v1.Column;
import io.duckcluster.proto.v1.RowBatch;

import java.util.ArrayList;
import java.util.List;

public final class RowBatchConverter {
    private RowBatchConverter() {}

    public static RowBatchData fromProto(RowBatch batch) {
        List<String> columnNames = new ArrayList<>(batch.getColumnNamesList());
        if (columnNames.isEmpty() && !batch.getColumnValuesList().isEmpty()) {
            for (Column column : batch.getColumnValuesList()) {
                columnNames.add(column.getName());
            }
        }

        int rowCount = (int) batch.getRowCount();
        List<List<String>> rows = new ArrayList<>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            List<String> row = new ArrayList<>(columnNames.size());
            for (Column column : batch.getColumnValuesList()) {
                row.add(column.getValues(rowIndex));
            }
            rows.add(row);
        }
        return new RowBatchData(columnNames, rows);
    }
}
