package io.duckcluster.worker.duckdb;

import io.duckcluster.common.merger.SqlJdbcTypes;
import io.duckcluster.proto.v1.Column;
import io.duckcluster.proto.v1.RowBatch;
import io.duckcluster.proto.v1.SqlValueType;
import io.grpc.Status;
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
import java.util.concurrent.atomic.AtomicInteger;

public final class FragmentExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(FragmentExecutor.class);
    private final int batchSize;
    private final int maxBatchBytes;
    private final DuckDBConnectionPool pool;
    private final RunningFragmentRegistry runningFragments;
    private final AtomicInteger inFlightFragments = new AtomicInteger();
    private volatile boolean acceptingFragments = true;

    public FragmentExecutor(
            DuckDBConnectionPool pool, RunningFragmentRegistry runningFragments, int batchSize, int maxBatchBytes) {
        this.pool = pool;
        this.runningFragments = runningFragments;
        this.batchSize = Math.max(1, batchSize);
        this.maxBatchBytes = Math.max(64 * 1024, maxBatchBytes);
    }

    public int inFlightCount() {
        return inFlightFragments.get();
    }

    public double loadFactor(int numThreads) {
        return (double) inFlightFragments.get() / Math.max(1, numThreads);
    }

    public void beginDrain() {
        acceptingFragments = false;
    }

    public void execute(String sql, String queryId, int fragmentId, StreamObserver<RowBatch> responseObserver) {
        if (!acceptingFragments) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("worker is draining and not accepting new fragments")
                    .asRuntimeException());
            return;
        }
        String runKey = queryId + ":" + fragmentId;
        inFlightFragments.incrementAndGet();
        long startMs = System.currentTimeMillis();
        LOG.debug("Executing SQL: {}", sql);
        Connection connection;
        try {
            connection = pool.checkout();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            inFlightFragments.decrementAndGet();
            responseObserver.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("Connection pool checkout interrupted").asRuntimeException());
            return;
        }
        if (connection == null) {
            inFlightFragments.decrementAndGet();
            LOG.warn("Connection pool exhausted, rejecting fragment");
            responseObserver.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("Connection pool exhausted").asRuntimeException());
            return;
        }
        try (Statement statement = connection.createStatement()) {
            runningFragments.register(runKey, statement);
            try (ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metadata = resultSet.getMetaData();
            int columnCount = metadata.getColumnCount();
            List<String> columnNames = new ArrayList<>(columnCount);
            List<SqlValueType> columnTypes = new ArrayList<>(columnCount);
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                columnNames.add(metadata.getColumnName(columnIndex));
                columnTypes.add(SqlJdbcTypes.toProtoType(
                        metadata.getColumnType(columnIndex), metadata.getColumnTypeName(columnIndex)));
            }

            List<Column.Builder> columnBuilders = newColumnBuilders(columnNames, columnTypes);

            int rowsInBatch = 0;
            int totalRows = 0;
            int batchCount = 0;
            while (resultSet.next()) {
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    Object value = resultSet.getObject(columnIndex + 1);
                    boolean isNull = resultSet.wasNull() || value == null;
                    Column.Builder builder = columnBuilders.get(columnIndex);
                    builder.addNulls(isNull);
                    if (isNull) {
                        builder.addValues("");
                    } else {
                        builder.addValues(value.toString());
                    }
                }
                rowsInBatch++;
                if (rowsInBatch >= batchSize || estimatedBatchBytes(columnBuilders) >= maxBatchBytes) {
                    emitBatch(responseObserver, columnNames, columnBuilders, rowsInBatch);
                    totalRows += rowsInBatch;
                    batchCount++;
                    columnBuilders = newColumnBuilders(columnNames, columnTypes);
                    rowsInBatch = 0;
                }
            }

            if (rowsInBatch > 0) {
                emitBatch(responseObserver, columnNames, columnBuilders, rowsInBatch);
                totalRows += rowsInBatch;
                batchCount++;
            }
            long durationMs = System.currentTimeMillis() - startMs;
            LOG.info("Fragment executed: {} rows, {} batches, {}ms", totalRows, batchCount, durationMs);
            responseObserver.onCompleted();
            }
        } catch (SQLException e) {
            if (Thread.currentThread().isInterrupted()) {
                responseObserver.onError(Status.CANCELLED.withDescription("fragment cancelled").asRuntimeException());
                return;
            }
            LOG.error("Failed to execute fragment SQL: {}", sql, e);
            if (e.getMessage() != null && e.getMessage().contains("Catalog") && e.getMessage().contains("does not exist")) {
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("Catalog not found: " + e.getMessage()).asRuntimeException());
            } else {
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage()).asRuntimeException());
            }
        } finally {
            runningFragments.unregister(runKey);
            pool.checkin(connection);
            inFlightFragments.decrementAndGet();
        }
    }

    private static List<Column.Builder> newColumnBuilders(List<String> columnNames, List<SqlValueType> columnTypes) {
        List<Column.Builder> builders = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            builders.add(Column.newBuilder()
                    .setName(columnNames.get(i))
                    .setType(columnTypes.get(i)));
        }
        return builders;
    }

    private static int estimatedBatchBytes(List<Column.Builder> columnBuilders) {
        int bytes = 0;
        for (Column.Builder builder : columnBuilders) {
            for (String value : builder.getValuesList()) {
                bytes += value.length();
            }
        }
        return bytes;
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
