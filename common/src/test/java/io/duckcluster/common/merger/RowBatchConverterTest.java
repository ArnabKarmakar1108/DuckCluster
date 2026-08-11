package io.duckcluster.common.merger;

import io.duckcluster.proto.v1.Column;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.SqlValueType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowBatchConverterTest {
    @Test
    void preservesNullFlagsAndColumnTypes() {
        RowBatch batch = RowBatch.newBuilder()
                .setRowCount(2)
                .addColumnNames("zip")
                .addColumnNames("count")
                .addColumnValues(Column.newBuilder()
                        .setName("zip")
                        .setType(SqlValueType.SQL_VARCHAR)
                        .addValues("02134")
                        .addNulls(false)
                        .addValues("")
                        .addNulls(true)
                        .build())
                .addColumnValues(Column.newBuilder()
                        .setName("count")
                        .setType(SqlValueType.SQL_BIGINT)
                        .addValues("10")
                        .addNulls(false)
                        .addValues("")
                        .addNulls(true)
                        .build())
                .build();

        RowBatchData data = RowBatchConverter.fromProto(batch);

        assertEquals("VARCHAR", data.columnSqlTypes().get(0));
        assertEquals("BIGINT", data.columnSqlTypes().get(1));
        assertEquals("02134", data.cellValue(0, 0));
        assertTrue(data.isNull(1, 0));
        assertTrue(data.isNull(1, 1));
    }
}
