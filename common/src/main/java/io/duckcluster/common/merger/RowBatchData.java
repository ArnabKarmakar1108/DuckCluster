package io.duckcluster.common.merger;

import java.util.Collections;
import java.util.List;

public record RowBatchData(
        List<String> columnNames,
        List<List<String>> rows,
        List<String> columnSqlTypes,
        List<List<Boolean>> rowNulls) {

    public RowBatchData(List<String> columnNames, List<List<String>> rows) {
        this(columnNames, rows, List.of(), List.of());
    }

    public RowBatchData {
        columnNames = List.copyOf(columnNames);
        rows = List.copyOf(rows);
        columnSqlTypes = columnSqlTypes == null ? List.of() : List.copyOf(columnSqlTypes);
        rowNulls = rowNulls == null ? List.of() : List.copyOf(rowNulls);
    }

    public List<String> resolvedColumnSqlTypes() {
        if (!columnSqlTypes.isEmpty()) {
            return columnSqlTypes;
        }
        return Collections.nCopies(columnNames.size(), "VARCHAR");
    }

    public boolean isNull(int rowIndex, int columnIndex) {
        if (rowNulls.isEmpty() || rowIndex >= rowNulls.size()) {
            return false;
        }
        List<Boolean> flags = rowNulls.get(rowIndex);
        if (columnIndex >= flags.size()) {
            return false;
        }
        return flags.get(columnIndex);
    }

    public String cellValue(int rowIndex, int columnIndex) {
        if (isNull(rowIndex, columnIndex)) {
            return null;
        }
        return rows.get(rowIndex).get(columnIndex);
    }
}
