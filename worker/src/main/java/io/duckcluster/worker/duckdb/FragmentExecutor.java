package io.duckcluster.worker.duckdb;

import io.duckcluster.proto.v1.Column;
import io.duckcluster.proto.v1.RowBatch;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class FragmentExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(FragmentExecutor.class);
    private static final int BATCH_SIZE = 1024;

    private final DuckDBConnectionPool pool;

    public FragmentExecutor(DuckDBConnectionPool pool) {
        this.pool = pool;
    }

    public void execute(String sql, StreamObserver<RowBatch> responseObserver) {
        Connection connection;
        try {
            connection = pool.checkout();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            responseObserver.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("Connection pool checkout interrupted").asRuntimeException());
            return;
        }
        if (connection == null) {
            responseObserver.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("Connection pool exhausted").asRuntimeException());
            return;
        }
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metadata = resultSet.getMetaData();
            int columnCount = metadata.getColumnCount();
            List<String> columnNames = new ArrayList<>(columnCount);
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                columnNames.add(metadata.getColumnName(columnIndex));
            }

            List<Column.Builder> columnBuilders = new ArrayList<>(columnCount);
            for (String columnName : columnNames) {
                columnBuilders.add(Column.newBuilder().setName(columnName));
            }

            int rowsInBatch = 0;
            while (resultSet.next()) {
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    String value = resultSet.getString(columnIndex + 1);
                    columnBuilders.get(columnIndex).addValues(value == null ? "" : value);
                }
                rowsInBatch++;
                if (rowsInBatch >= BATCH_SIZE) {
                    emitBatch(responseObserver, columnNames, columnBuilders, rowsInBatch);
                    columnBuilders = newColumnBuilders(columnNames);
                    rowsInBatch = 0;
                }
            }

            if (rowsInBatch > 0) {
                emitBatch(responseObserver, columnNames, columnBuilders, rowsInBatch);
            }
            responseObserver.onCompleted();
        } catch (SQLException e) {
            LOG.error("Failed to execute fragment SQL: {}", sql, e);
            if (e.getMessage() != null && e.getMessage().contains("Catalog") && e.getMessage().contains("does not exist")) {
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("Catalog not found: " + e.getMessage()).asRuntimeException());
            } else {
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage()).asRuntimeException());
            }
        } finally {
            pool.checkin(connection);
        }
    }

    private static List<Column.Builder> newColumnBuilders(List<String> columnNames) {
        List<Column.Builder> builders = new ArrayList<>(columnNames.size());
        for (String columnName : columnNames) {
            builders.add(Column.newBuilder().setName(columnName));
        }
        return builders;
    }

    private static void emitBatch(
            StreamObserver<RowBatch> responseObserver,
            List<String> columnNames,
            List<Column.Builder> columnBuilders,
            int rowCount) {
        RowBatch.Builder batchBuilder = RowBatch.newBuilder().setRowCount(rowCount);
        batchBuilder.addAllColumnNames(columnNames);
        for (Column.Builder columnBuilder : columnBuilders) {
            batchBuilder.addColumnValues(columnBuilder.build());
        }
        responseObserver.onNext(batchBuilder.build());
    }
}
