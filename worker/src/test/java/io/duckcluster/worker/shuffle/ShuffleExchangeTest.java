package io.duckcluster.worker.shuffle;

import io.duckcluster.common.planner.BroadcastSqlNames;
import io.duckcluster.proto.v1.ShufflePieceChunk;
import io.duckcluster.proto.v1.ShufflePieceResponse;
import io.duckcluster.proto.v1.ShuffleTableRequest;
import io.duckcluster.proto.v1.ShuffleTableResponse;
import io.duckcluster.proto.v1.ShuffleTarget;
import io.duckcluster.proto.v1.WorkerServiceGrpc;
import io.duckcluster.worker.duckdb.DuckDBConnectionPool;
import io.duckcluster.worker.duckdb.ShardFileMetadata;
import io.duckcluster.worker.duckdb.ShardManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShuffleExchangeTest {
    private static final String QUERY = "query-1";
    private static final int BUCKETS = 3;
    private static final int ROWS = 1000;

    @TempDir
    Path tempDir;

    private Worker source;
    private Worker target;
    private Server targetServer;

    @BeforeEach
    void setUp() throws Exception {
        source = new Worker("w1", tempDir.resolve("w1"));
        target = new Worker("w2", tempDir.resolve("w2"));
        writeOrdersShard(source.dataDir.resolve("orders_shard0.duckdb"));
        source.shardManager.attachShard(
                new ShardFileMetadata("orders", 0, source.dataDir.resolve("orders_shard0.duckdb")));

        targetServer = ServerBuilder.forPort(0)
                .addService(new WorkerServiceGrpc.WorkerServiceImplBase() {
                    @Override
                    public StreamObserver<ShufflePieceChunk> receiveShufflePiece(
                            StreamObserver<ShufflePieceResponse> responseObserver) {
                        return target.exchange.receive(responseObserver);
                    }
                })
                .build()
                .start();
    }

    @AfterEach
    void tearDown() {
        targetServer.shutdownNow();
        source.close();
        target.close();
    }

    @Test
    void bucketsRowsByDrivingHashAcrossLocalAndRemoteTargets() throws Exception {
        ShuffleTableResponse response = source.exchange.shuffle(request());

        assertEquals(ROWS, response.getRowsShuffled());
        long total = 0;
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            Worker holder = bucket == 0 ? source : target;
            String table = BroadcastSqlNames.shuffleBucketTable(QUERY, "orders", bucket);
            total += holder.scalar("SELECT COUNT(*) FROM \"" + table + "\"");
            assertEquals(0, holder.scalar("SELECT COUNT(*) FROM \"" + table + "\" "
                    + "WHERE abs(hash(o_orderkey)) % " + BUCKETS + " <> " + bucket));
        }
        assertEquals(ROWS, total);
        assertEquals("DATE", target.string("SELECT typeof(o_orderdate) FROM \""
                + BroadcastSqlNames.shuffleBucketTable(QUERY, "orders", 1) + "\" LIMIT 1"));
        assertEquals("DECIMAL(12,2)", target.string("SELECT typeof(o_total) FROM \""
                + BroadcastSqlNames.shuffleBucketTable(QUERY, "orders", 1) + "\" LIMIT 1"));
    }

    @Test
    void clearQueryDropsBucketTables() throws Exception {
        source.exchange.shuffle(request());

        target.exchange.clearQuery(QUERY);
        source.exchange.clearQuery(QUERY);

        assertEquals(0, target.scalar("SELECT COUNT(*) FROM duckdb_tables() WHERE starts_with(table_name, '"
                + BroadcastSqlNames.shuffleQueryPrefix(QUERY) + "')"));
        assertEquals(0, source.scalar("SELECT COUNT(*) FROM duckdb_tables() WHERE starts_with(table_name, '"
                + BroadcastSqlNames.shuffleQueryPrefix(QUERY) + "')"));
    }

    @Test
    void rejectsUnsafeJoinColumn() {
        ShuffleTableRequest bad = request().toBuilder().setJoinColumn("o_orderkey); DROP TABLE x; --").build();
        assertThrows(IllegalArgumentException.class, () -> source.exchange.shuffle(bad));
    }

    @Test
    void failsWhenSourceShardIsNotLocal() {
        ShuffleTableRequest missing = request().toBuilder().clearSourceShardIds().addSourceShardIds(5).build();
        assertThrows(IllegalStateException.class, () -> source.exchange.shuffle(missing));
    }

    private ShuffleTableRequest request() {
        ShuffleTableRequest.Builder builder = ShuffleTableRequest.newBuilder()
                .setQueryId(QUERY)
                .setTableName("orders")
                .addSourceShardIds(0)
                .setJoinColumn("o_orderkey")
                .setJoinKeyType("BIGINT")
                .setBucketCount(BUCKETS)
                .addTargets(ShuffleTarget.newBuilder().setBucket(0).setWorkerId("w1"));
        for (int bucket = 1; bucket < BUCKETS; bucket++) {
            builder.addTargets(ShuffleTarget.newBuilder()
                    .setBucket(bucket)
                    .setWorkerId("w2")
                    .setHost("127.0.0.1")
                    .setPort(targetServer.getPort()));
        }
        return builder.build();
    }

    private static void writeOrdersShard(Path path) throws Exception {
        Files.createDirectories(path.getParent());
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:" + path);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders AS SELECT range::BIGINT AS o_orderkey, "
                    + "(range % 50)::BIGINT AS o_custkey, DATE '1995-01-01' + range::INTEGER AS o_orderdate, "
                    + "(range * 1.25)::DECIMAL(12,2) AS o_total FROM range(" + ROWS + ")");
        }
    }

    private static final class Worker implements AutoCloseable {
        final Path dataDir;
        final ShardManager shardManager;
        final DuckDBConnectionPool pool;
        final ShuffleExchange exchange;

        Worker(String id, Path dataDir) throws Exception {
            this.dataDir = dataDir;
            Files.createDirectories(dataDir);
            String db = dataDir.resolve(id + ".db").toString();
            shardManager = new ShardManager(db, dataDir);
            pool = new DuckDBConnectionPool(db, 2, 5000);
            exchange = new ShuffleExchange(id, pool, shardManager, dataDir.resolve(".shuffle-tmp"),
                    4096, 4 * 1024 * 1024, 30_000);
        }

        long scalar(String sql) throws Exception {
            Connection connection = pool.checkout();
            try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
                assertTrue(rs.next());
                return rs.getLong(1);
            } finally {
                pool.checkin(connection);
            }
        }

        String string(String sql) throws Exception {
            Connection connection = pool.checkout();
            try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
                assertTrue(rs.next());
                return rs.getString(1);
            } finally {
                pool.checkin(connection);
            }
        }

        @Override
        public void close() {
            exchange.close();
            pool.close();
            shardManager.close();
        }
    }
}
