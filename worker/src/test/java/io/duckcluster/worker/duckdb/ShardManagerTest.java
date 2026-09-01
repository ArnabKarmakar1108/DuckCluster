package io.duckcluster.worker.duckdb;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ShardManagerTest {

    @TempDir
    Path tempDir;
    private ShardManager shardManager;
    private Path shardsDir;

    @BeforeEach
    void setUp() throws Exception {
        shardsDir = tempDir.resolve("shards");
        Files.createDirectories(shardsDir);
        String mainDbPath = tempDir.resolve("main.db").toString();
        shardManager = new ShardManager(mainDbPath, shardsDir);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (ShardFileMetadata meta : shardManager.getAttachedShards()) {
            shardManager.detachShard(meta);
        }
        shardManager.close();
    }

    @Test
    void attachShard_makesShardAccessible() throws Exception {
        Path shardPath = createShardFile("events", 0);
        ShardFileMetadata meta = new ShardFileMetadata("events", 0, shardPath);

        shardManager.attachShard(meta);

        List<ShardFileMetadata> attached = shardManager.getAttachedShards();
        assertEquals(1, attached.size());
        assertEquals("events_shard0", attached.get(0).catalogName());
    }

    @Test
    void attachShard_idempotent() throws Exception {
        Path shardPath = createShardFile("events", 0);
        ShardFileMetadata meta = new ShardFileMetadata("events", 0, shardPath);

        shardManager.attachShard(meta);
        shardManager.attachShard(meta);

        assertEquals(1, shardManager.getAttachedShards().size());
    }

    @Test
    void detachShard_removesFromAttached() throws Exception {
        Path shardPath = createShardFile("events", 0);
        ShardFileMetadata meta = new ShardFileMetadata("events", 0, shardPath);

        shardManager.attachShard(meta);
        shardManager.detachShard(meta);

        assertTrue(shardManager.getAttachedShards().isEmpty());
    }

    @Test
    void scanAndAttachAll_findsMatchingFiles() throws Exception {
        createShardFile("events", 0);
        createShardFile("events", 1);
        createShardFile("orders", 0);
        Files.createFile(shardsDir.resolve("worker-1.db"));

        int count = shardManager.scanAndAttachAll();

        assertEquals(3, count);
        assertEquals(3, shardManager.getAttachedShards().size());
    }

    @Test
    void getShard_returnsCorrectMetadata() throws Exception {
        Path shardPath = createShardFile("events", 2);
        ShardFileMetadata meta = new ShardFileMetadata("events", 2, shardPath);
        shardManager.attachShard(meta);

        assertTrue(shardManager.getShard("events", 2).isPresent());
        assertTrue(shardManager.getShard("events", 99).isEmpty());
    }

    @Test
    void metadataReadsAreSafeConcurrentlyWithAttachDetachAndClose() throws Exception {
        Path stablePath = createShardFile("events", 0);
        Path churnPath = createShardFile("events", 1);
        ShardFileMetadata stable = new ShardFileMetadata("events", 0, stablePath);
        ShardFileMetadata churn = new ShardFileMetadata("events", 1, churnPath);
        shardManager.attachShard(stable);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < 6; thread++) {
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < 200; i++) {
                        shardManager.describeTable(stable);
                        shardManager.estimateRowCount(stable);
                    }
                    return null;
                }));
            }
            futures.add(executor.submit(() -> {
                for (int i = 0; i < 50; i++) {
                    shardManager.attachShard(churn);
                    shardManager.detachShard(churn);
                }
                return null;
            }));
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(2L, shardManager.estimateRowCount(stable));
        shardManager.close();
        assertEquals(0L, shardManager.estimateRowCount(stable));
        assertTrue(shardManager.describeTable(stable).isEmpty());
    }

    private Path createShardFile(String table, int shardId) throws Exception {
        Path path = shardsDir.resolve(table + "_shard" + shardId + ".duckdb");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + path)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + table + " (id INTEGER, value INTEGER)");
                stmt.execute("INSERT INTO " + table + " VALUES (1, 100), (2, 200)");
                stmt.execute("CHECKPOINT");
            }
        }
        return path;
    }
}
