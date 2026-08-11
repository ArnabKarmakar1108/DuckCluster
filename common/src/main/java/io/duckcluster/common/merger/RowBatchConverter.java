package io.duckcluster.common.merger;

import io.duckcluster.proto.v1.Column;
import io.duckcluster.proto.v1.RowBatch;

import java.util.ArrayList;
import java.util.List;

public final class RowBatchConverter {
    private RowBatchConverter() {}

    public static RowBatchData fromProto(RowBatch batch) {
        List<String> columnNames = new ArrayList<>(batch.getColumnNamesList());
        List<Column> columns = batch.getColumnValuesList();
        if (columnNames.isEmpty() && !columns.isEmpty()) {
            for (Column column : columns) {
                columnNames.add(column.getName());
            }
        }

        int rowCount = (int) batch.getRowCount();
        List<List<String>> rows = new ArrayList<>(rowCount);
        List<List<Boolean>> rowNulls = new ArrayList<>(rowCount);
        List<String> columnSqlTypes = new ArrayList<>(columns.size());
        for (Column column : columns) {
            columnSqlTypes.add(SqlJdbcTypes.fromProtoType(column.getType()));
        }
        while (columnSqlTypes.size() < columnNames.size()) {
            columnSqlTypes.add("VARCHAR");
        }

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            List<String> row = new ArrayList<>(columnNames.size());
            List<Boolean> nulls = new ArrayList<>(columnNames.size());
            for (Column column : columns) {
                boolean isNull = column.getNullsCount() > rowIndex && column.getNulls(rowIndex);
                nulls.add(isNull);
                if (isNull) {
                    row.add("");
                } else {
                    row.add(column.getValues(rowIndex));
                }
            }
            rows.add(row);
            rowNulls.add(nulls);
        }
        return new RowBatchData(columnNames, rows, columnSqlTypes, rowNulls);
    }
}
