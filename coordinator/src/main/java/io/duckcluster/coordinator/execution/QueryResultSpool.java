package io.duckcluster.coordinator.execution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.duckcluster.common.merger.RowBatchData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Append-only row spool for async result pagination without holding all rows in heap (4.6). */
final class QueryResultSpool implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<Object>> ROW_TYPE = new TypeReference<>() {};

    private final Path file;
    private final BufferedWriter writer;
    private final AtomicLong rowCount = new AtomicLong();
    private volatile List<String> columns = List.of();
    private volatile boolean closed;

    private QueryResultSpool(Path file, BufferedWriter writer) {
        this.file = file;
        this.writer = writer;
    }

    static QueryResultSpool open(String queryId) {
        return open(queryId, null);
    }

    static QueryResultSpool open(String queryId, Path dataDir) {
        try {
            Path spoolDir = dataDir != null ? dataDir.resolve(".query-spool") : null;
            if (spoolDir != null) {
                Files.createDirectories(spoolDir);
            }
            Path file = spoolDir != null
                    ? Files.createTempFile(spoolDir, "query-" + queryId + "-", ".ndjson")
                    : Files.createTempFile("duckcluster-query-" + queryId + "-", ".ndjson");
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(Files.newOutputStream(file), StandardCharsets.UTF_8));
            return new QueryResultSpool(file, writer);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open query result spool", e);
        }
    }

    List<String> columns() {
        return columns;
    }

    boolean hasColumns() {
        return !columns.isEmpty();
    }

    long rowCount() {
        return rowCount.get();
    }

    long retainedBytes() {
        try {
            return Files.size(file);
        } catch (IOException e) {
            return 0;
        }
    }

    void appendRow(List<Object> row) {
        try {
            writeRow(row);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append row to result spool", e);
        }
    }

    void flush() throws IOException {
        writer.flush();
    }

    void appendBatch(RowBatchData batch) {
        if (batch == null || batch.rows().isEmpty()) {
            return;
        }
        if (columns.isEmpty()) {
            columns = List.copyOf(batch.columnNames());
        }
        try {
            for (List<String> row : batch.rows()) {
                writeRow(row);
            }
            writer.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append batch to result spool", e);
        }
    }

    void writeAll(List<String> resultColumns, List<List<Object>> rows) {
        if (resultColumns != null && !resultColumns.isEmpty()) {
            columns = List.copyOf(resultColumns);
        }
        if (rows == null || rows.isEmpty()) {
            return;
        }
        try {
            for (List<Object> row : rows) {
                writeRow(row);
            }
            writer.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write rows to result spool", e);
        }
    }

    private void writeRow(List<?> row) throws IOException {
        writer.write(MAPPER.writeValueAsString(row));
        writer.newLine();
        rowCount.incrementAndGet();
    }

    List<List<Object>> readPage(int offset, int limit) {
        int safeOffset = Math.max(0, offset);
        int safeLimit = Math.max(1, limit);
        List<List<Object>> page = new ArrayList<>(Math.min(safeLimit, 256));
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            int index = 0;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                if (index < safeOffset) {
                    index++;
                    continue;
                }
                if (page.size() >= safeLimit) {
                    break;
                }
                List<Object> row = MAPPER.readValue(line, ROW_TYPE);
                page.add(row);
                index++;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read result spool", e);
        }
        return page;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            writer.close();
        } catch (IOException ignored) {
            // best effort
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // best effort
        }
    }
}
