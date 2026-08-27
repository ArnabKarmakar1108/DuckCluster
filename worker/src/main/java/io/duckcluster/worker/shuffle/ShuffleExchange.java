package io.duckcluster.worker.shuffle;

import com.google.protobuf.ByteString;
import io.duckcluster.common.planner.BroadcastSqlNames;
import io.duckcluster.common.shard.ShardNameValidator;
import io.duckcluster.proto.v1.ShufflePieceChunk;
import io.duckcluster.proto.v1.ShufflePieceResponse;
import io.duckcluster.proto.v1.ShuffleTableRequest;
import io.duckcluster.proto.v1.ShuffleTableResponse;
import io.duckcluster.proto.v1.ShuffleTarget;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.duckcluster.worker.duckdb.DuckDBConnectionPool;
import io.duckcluster.worker.duckdb.ShardManager;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;

/**
 * Worker side of the hash shuffle join (4.3). As a <i>source</i>, a worker re-buckets its local shards
 * of a table with the driving table's partition function and streams each bucket as a Parquet file to
 * the worker that will run that bucket's fragment. As a <i>target</i>, it appends received pieces
 * into a per-query table that fragment SQL reads as {@code __dc_shuf_<query>_<table>_b<bucket>}.
 */
public final class ShuffleExchange implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ShuffleExchange.class);
    private static final Pattern IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
    private static final Pattern SQL_TYPE = Pattern.compile("^[A-Za-z][A-Za-z0-9_(), ]*$");
    private static final Pattern QUERY_ID = Pattern.compile("^[A-Za-z0-9_-]+$");
    private static final String BUCKET_COLUMN = "__dc_bucket";
    private static final long CONNECTION_WAIT_MS = 30_000;

    private final String workerId;
    private final DuckDBConnectionPool pool;
    private final ShardManager shardManager;
    private final Path workDir;
    private final int chunkBytes;
    private final long peerDeadlineMs;
    private final PeerChannels peers;
    private final Map<String, Set<String>> tablesByQuery = new ConcurrentHashMap<>();
    private final Map<String, Object> tableLocks = new ConcurrentHashMap<>();

    public ShuffleExchange(
            String workerId,
            DuckDBConnectionPool pool,
            ShardManager shardManager,
            Path workDir,
            int chunkBytes,
            int maxInboundMessageBytes,
            long peerDeadlineMs) throws IOException {
        this.workerId = workerId;
        this.pool = pool;
        this.shardManager = shardManager;
        this.workDir = workDir;
        this.chunkBytes = Math.max(4096, chunkBytes);
        this.peerDeadlineMs = peerDeadlineMs;
        this.peers = new PeerChannels(maxInboundMessageBytes);
        dropLeftoverTables();
        Files.createDirectories(workDir);
    }

    public ShuffleTableResponse shuffle(ShuffleTableRequest request) throws Exception {
        validate(request);
        Map<Integer, ShuffleTarget> targets = new HashMap<>();
        for (ShuffleTarget target : request.getTargetsList()) {
            targets.put(target.getBucket(), target);
        }
        for (int bucket = 0; bucket < request.getBucketCount(); bucket++) {
            if (!targets.containsKey(bucket)) {
                throw new IllegalArgumentException("no shuffle target for bucket " + bucket);
            }
        }

        long rows = 0;
        int pieces = 0;
        long bytes = 0;
        for (int sourceShard : request.getSourceShardIdsList()) {
            Path partitionDir = queryDir(request.getQueryId())
                    .resolve(request.getTableName())
                    .resolve("s" + sourceShard);
            try {
                rows += writeBuckets(request, sourceShard, partitionDir);
                for (int bucket = 0; bucket < request.getBucketCount(); bucket++) {
                    for (Path piece : bucketFiles(partitionDir, bucket)) {
                        bytes += Files.size(piece);
                        send(targets.get(bucket), request.getQueryId(), request.getTableName(), bucket, sourceShard, piece);
                        pieces++;
                    }
                }
            } finally {
                deleteRecursively(partitionDir);
            }
        }
        LOG.info("Query [{}] shuffled {} rows of {} (shards {}) into {} buckets: {} pieces, {} bytes",
                request.getQueryId(), rows, request.getTableName(), request.getSourceShardIdsList(),
                request.getBucketCount(), pieces, bytes);
        return ShuffleTableResponse.newBuilder()
                .setAccepted(true)
                .setRowsShuffled(rows)
                .setPiecesSent(pieces)
                .setBytesSent(bytes)
                .build();
    }

    /** One scan per source shard writes every bucket; a schema-only file stands in for empty buckets. */
    private long writeBuckets(ShuffleTableRequest request, int sourceShard, Path partitionDir)
            throws SQLException, InterruptedException, IOException {
        String table = request.getTableName();
        if (shardManager.getShard(table, sourceShard).isEmpty() && !shardManager.hasCachedShard(table, sourceShard)) {
            throw new IllegalStateException(
                    "worker " + workerId + " does not hold " + table + "_shard" + sourceShard + " for shuffle");
        }
        Files.createDirectories(partitionDir);
        String source = quote(table + "_shard" + sourceShard) + "." + quote(table);
        String bucketExpression = "(abs(hash(CAST(" + quote(request.getJoinColumn()) + " AS "
                + request.getJoinKeyType() + "))) % " + request.getBucketCount() + ")::INTEGER";
        Connection connection = checkout();
        try (Statement statement = connection.createStatement()) {
            long rows = statement.executeUpdate("COPY (SELECT *, " + bucketExpression + " AS " + quote(BUCKET_COLUMN)
                    + " FROM " + source + ") TO " + literal(partitionDir)
                    + " (FORMAT PARQUET, PARTITION_BY (" + quote(BUCKET_COLUMN) + "), OVERWRITE_OR_IGNORE TRUE)");
            statement.execute("COPY (SELECT * FROM " + source + " LIMIT 0) TO "
                    + literal(partitionDir.resolve("empty.parquet")) + " (FORMAT PARQUET)");
            return rows;
        } finally {
            pool.checkin(connection);
        }
    }

    private static List<Path> bucketFiles(Path partitionDir, int bucket) throws IOException {
        Path bucketDir = partitionDir.resolve(BUCKET_COLUMN + "=" + bucket);
        List<Path> files = new ArrayList<>();
        if (Files.isDirectory(bucketDir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(bucketDir, "*.parquet")) {
                stream.forEach(files::add);
            }
        }
        if (files.isEmpty()) {
            files.add(partitionDir.resolve("empty.parquet"));
        }
        files.sort(Comparator.naturalOrder());
        return files;
    }

    private void send(ShuffleTarget target, String queryId, String table, int bucket, int sourceShard, Path piece)
            throws IOException, SQLException, InterruptedException {
        if (workerId.equals(target.getWorkerId())) {
            loadPiece(queryId, table, bucket, piece);
            return;
        }
        WorkerServiceGrpc.WorkerServiceStub stub = WorkerServiceGrpc.newStub(peers.channel(target.getHost(), target.getPort()))
                .withDeadlineAfter(peerDeadlineMs, TimeUnit.MILLISECONDS);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ShufflePieceResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ClientCallStreamObserver<ShufflePieceChunk> requests =
                (ClientCallStreamObserver<ShufflePieceChunk>) stub.receiveShufflePiece(new StreamObserver<>() {
                    @Override
                    public void onNext(ShufflePieceResponse value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        failure.set(t);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });

        long length = Files.size(piece);
        int crc = crc32(piece);
        byte[] buffer = new byte[chunkBytes];
        try (InputStream input = Files.newInputStream(piece)) {
            boolean first = true;
            int read;
            while ((read = input.read(buffer)) > 0 || first) {
                while (!requests.isReady() && done.getCount() > 0) {
                    LockSupport.parkNanos(100_000);
                }
                if (done.getCount() == 0) {
                    break;
                }
                ShufflePieceChunk.Builder chunk = ShufflePieceChunk.newBuilder()
                        .setData(ByteString.copyFrom(buffer, 0, Math.max(read, 0)));
                if (first) {
                    chunk.setQueryId(queryId)
                            .setTableName(table)
                            .setBucket(bucket)
                            .setSourceShard(sourceShard)
                            .setContentLength(length)
                            .setCrc32(crc);
                    first = false;
                }
                requests.onNext(chunk.build());
            }
        }
        requests.onCompleted();
        if (!done.await(peerDeadlineMs, TimeUnit.MILLISECONDS)) {
            requests.cancel("shuffle piece timed out", null);
            throw new IOException("timed out sending shuffle piece to " + target.getWorkerId());
        }
        if (failure.get() != null) {
            throw new IOException("shuffle piece to " + target.getWorkerId() + " failed: "
                    + failure.get().getMessage(), failure.get());
        }
        if (response.get() == null || !response.get().getAccepted()) {
            throw new IOException("shuffle piece rejected by " + target.getWorkerId());
        }
    }

    /** Server-side handler for a piece streamed from a peer worker. */
    public StreamObserver<ShufflePieceChunk> receive(StreamObserver<ShufflePieceResponse> responseObserver) {
        return new StreamObserver<>() {
            private Path file;
            private java.io.OutputStream output;
            private String queryId;
            private String table;
            private int bucket;
            private long expectedLength;
            private int expectedCrc;
            private long written;
            private final CRC32 crc = new CRC32();
            private boolean failed;

            @Override
            public void onNext(ShufflePieceChunk chunk) {
                if (failed) {
                    return;
                }
                try {
                    if (file == null) {
                        queryId = chunk.getQueryId();
                        table = chunk.getTableName();
                        bucket = chunk.getBucket();
                        expectedLength = chunk.getContentLength();
                        expectedCrc = chunk.getCrc32();
                        validateQueryId(queryId);
                        ShardNameValidator.validateTableName(table);
                        file = Files.createTempFile(incomingDir(), "piece-", ".parquet");
                        output = Files.newOutputStream(file);
                    }
                    byte[] data = chunk.getData().toByteArray();
                    output.write(data);
                    crc.update(data, 0, data.length);
                    written += data.length;
                } catch (Exception e) {
                    fail(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.warn("Shuffle piece stream from peer failed: {}", t.getMessage());
                cleanup();
            }

            @Override
            public void onCompleted() {
                if (failed) {
                    return;
                }
                try {
                    output.close();
                    if (written != expectedLength) {
                        throw new IOException("shuffle piece length mismatch: expected " + expectedLength + " got " + written);
                    }
                    if (expectedCrc != (int) crc.getValue()) {
                        throw new IOException("shuffle piece checksum mismatch");
                    }
                    long rows = loadPiece(queryId, table, bucket, file);
                    responseObserver.onNext(ShufflePieceResponse.newBuilder().setAccepted(true).setRowsLoaded(rows).build());
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    fail(e);
                } finally {
                    cleanup();
                }
            }

            private void fail(Exception e) {
                failed = true;
                LOG.error("Failed to receive shuffle piece for query {}: {}", queryId, e.getMessage());
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
                cleanup();
            }

            private void cleanup() {
                try {
                    if (output != null) {
                        output.close();
                    }
                    if (file != null) {
                        Files.deleteIfExists(file);
                    }
                } catch (IOException ignored) {
                }
            }
        };
    }

    long loadPiece(String queryId, String table, int bucket, Path piece)
            throws SQLException, InterruptedException {
        String target = BroadcastSqlNames.shuffleBucketTable(queryId, table, bucket);
        tablesByQuery.computeIfAbsent(queryId, ignored -> ConcurrentHashMap.newKeySet()).add(target);
        String scan = "read_parquet(" + literal(piece) + ", hive_partitioning = false)";
        synchronized (tableLocks.computeIfAbsent(target, ignored -> new Object())) {
            Connection connection = checkout();
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE IF NOT EXISTS " + quote(target) + " AS SELECT * FROM " + scan + " LIMIT 0");
                return statement.executeUpdate("INSERT INTO " + quote(target) + " SELECT * FROM " + scan);
            } finally {
                pool.checkin(connection);
            }
        }
    }

    public void clearQuery(String queryId) throws SQLException, InterruptedException {
        Set<String> tables = tablesByQuery.remove(queryId);
        if (tables != null && !tables.isEmpty()) {
            Connection connection = checkout();
            try (Statement statement = connection.createStatement()) {
                for (String table : tables) {
                    statement.execute("DROP TABLE IF EXISTS " + quote(table));
                    tableLocks.remove(table);
                }
            } finally {
                pool.checkin(connection);
            }
        }
        if (queryId != null && QUERY_ID.matcher(queryId).matches()) {
            deleteRecursively(queryDir(queryId));
        }
    }

    /** Tables orphaned by a coordinator or worker crash mid-query. */
    private void dropLeftoverTables() {
        try {
            Connection connection = pool.checkout();
            if (connection == null) {
                return;
            }
            try (Statement statement = connection.createStatement()) {
                List<String> leftovers = new ArrayList<>();
                try (ResultSet resultSet = statement.executeQuery(
                        "SELECT table_name FROM duckdb_tables() WHERE starts_with(table_name, '"
                                + BroadcastSqlNames.SHUFFLE_PREFIX + "')")) {
                    while (resultSet.next()) {
                        leftovers.add(resultSet.getString(1));
                    }
                }
                for (String table : leftovers) {
                    statement.execute("DROP TABLE IF EXISTS " + quote(table));
                }
                if (!leftovers.isEmpty()) {
                    LOG.info("Dropped {} leftover shuffle tables", leftovers.size());
                }
            } finally {
                pool.checkin(connection);
            }
        } catch (Exception e) {
            LOG.warn("Could not drop leftover shuffle tables: {}", e.getMessage());
        }
        deleteRecursively(workDir);
    }

    private void validate(ShuffleTableRequest request) {
        validateQueryId(request.getQueryId());
        ShardNameValidator.validateTableName(request.getTableName());
        if (!IDENTIFIER.matcher(request.getJoinColumn()).matches()) {
            throw new IllegalArgumentException("invalid shuffle join column: " + request.getJoinColumn());
        }
        if (!SQL_TYPE.matcher(request.getJoinKeyType()).matches()) {
            throw new IllegalArgumentException("invalid shuffle join key type: " + request.getJoinKeyType());
        }
        if (request.getBucketCount() < 1) {
            throw new IllegalArgumentException("bucket_count must be >= 1");
        }
    }

    private static void validateQueryId(String queryId) {
        if (queryId == null || !QUERY_ID.matcher(queryId).matches()) {
            throw new IllegalArgumentException("invalid query id for shuffle: " + queryId);
        }
    }

    private Connection checkout() throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + CONNECTION_WAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            Connection connection = pool.checkout();
            if (connection != null) {
                return connection;
            }
        }
        throw new SQLException("Connection pool exhausted during shuffle");
    }

    private Path queryDir(String queryId) {
        return workDir.resolve(queryId);
    }

    private Path incomingDir() throws IOException {
        Path dir = workDir.resolve("incoming");
        Files.createDirectories(dir);
        return dir;
    }

    private static int crc32(Path file) throws IOException {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[64 * 1024];
        try (InputStream input = Files.newInputStream(file)) {
            int read;
            while ((read = input.read(buffer)) > 0) {
                crc.update(buffer, 0, read);
            }
        }
        return (int) crc.getValue();
    }

    private static void deleteRecursively(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException e) {
            LOG.debug("Could not delete {}: {}", root, e.getMessage());
        }
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String literal(Path path) {
        return "'" + path.toAbsolutePath().toString().replace("'", "''") + "'";
    }

    @Override
    public void close() {
        peers.close();
    }
}
