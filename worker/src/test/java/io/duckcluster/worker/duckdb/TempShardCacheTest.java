package io.duckcluster.worker.duckdb;

import io.duckcluster.worker.client.CoordinatorClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

class TempShardCacheTest {

    @TempDir
    Path tempDir;

    private ShardManager shardManager;
    private TempShardCache cache;
    private Path cacheDir;
    private String dbPath;

    @BeforeEach
    void setUp() throws Exception {
        Path dataDir = tempDir.resolve("data");
        Files.createDirectories(dataDir);
        dbPath = dataDir.resolve("main.db").toString();
        shardManager = new ShardManager(dbPath, dataDir);
        cacheDir = dataDir.resolve(".cache");
        cache = new TempShardCache(shardManager, null, "worker-1", cacheDir, 3);
    }

    @AfterEach
    void tearDown() {
        cache.close();
        shardManager.close();
    }

    @Test
    void loadShard_attachesAndQueries() throws Exception {
        Path shardFile = createShardFile("events", 0);

        String catalogName = cache.loadShard("events", 0, shardFile);

        assertEquals("events_shard0", catalogName);
        assertTrue(cache.hasShard("events", 0));
        assertTrue(Files.exists(cacheDir.resolve("events_shard0.duckdb")));

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM events_shard0.events")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void loadShard_evictsLRU_whenAtCapacity() throws Exception {
        cache.loadShard("events", 0, createShardFile("events", 0));
        cache.loadShard("events", 1, createShardFile("events", 1));
        cache.loadShard("events", 2, createShardFile("events", 2));

        assertTrue(cache.hasShard("events", 0));
        assertTrue(cache.hasShard("events", 1));
        assertTrue(cache.hasShard("events", 2));

        cache.loadShard("events", 3, createShardFile("events", 3));

        assertFalse(cache.hasShard("events", 0));
        assertTrue(cache.hasShard("events", 1));
        assertTrue(cache.hasShard("events", 2));
        assertTrue(cache.hasShard("events", 3));
        assertFalse(Files.exists(cacheDir.resolve("events_shard0.duckdb")));
    }

    @Test
    void touch_preventsEviction() throws Exception {
        cache.loadShard("events", 0, createShardFile("events", 0));
        cache.loadShard("events", 1, createShardFile("events", 1));
        cache.loadShard("events", 2, createShardFile("events", 2));

        cache.touch("events", 0);

        cache.loadShard("events", 3, createShardFile("events", 3));

        assertTrue(cache.hasShard("events", 0));
        assertFalse(cache.hasShard("events", 1));
    }

    @Test
    void loadShard_idempotent_forSameShard() throws Exception {
        cache.loadShard("events", 0, createShardFile("events", 0));
        String catalogName = cache.loadShard("events", 0, createShardFile("events", 0));

        assertEquals("events_shard0", catalogName);
        assertEquals(1, cache.getCachedShards().size());
    }

    @Test
    void close_detachesAllAndDeletesFiles() throws Exception {
        cache.loadShard("events", 0, createShardFile("events", 0));
        cache.loadShard("events", 1, createShardFile("events", 1));

        cache.close();

        assertFalse(Files.exists(cacheDir.resolve("events_shard0.duckdb")));
        assertFalse(Files.exists(cacheDir.resolve("events_shard1.duckdb")));
    }

    private Path createShardFile(String table, int shardId) throws Exception {
        Path tmpFile = tempDir.resolve(table + "_shard" + shardId + "_tmp.duckdb");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + tmpFile);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " (id INTEGER, name VARCHAR, value INTEGER)");
            stmt.execute("INSERT INTO " + table + " VALUES (1,'a',10), (2,'b',20), (3,'c',30)");
        }
        return tmpFile;
    }
}
