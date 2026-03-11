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
import java.util.List;

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
