package io.duckcluster.worker.duckdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardVersionActivatorTest {

    @TempDir
    Path tempDir;

    @Test
    void promotesStagedShardIntoLiveDataDirectory() throws Exception {
        Path dataDir = tempDir.resolve("data");
        Files.createDirectories(dataDir);
        Path adminDb = dataDir.resolve("main.db");
        ShardManager shardManager = new ShardManager(adminDb.toString(), dataDir);

        Path staging = dataDir.resolve(".staging").resolve("v1");
        Files.createDirectories(staging);
        Path staged = staging.resolve("events_shard0.duckdb");
        createShardFile(staged);

        ShardVersionActivator activator = new ShardVersionActivator(dataDir, shardManager);
        assertEquals(1, activator.activate("events", 1));
        assertTrue(Files.isRegularFile(dataDir.resolve("events_shard0.duckdb")));
        assertTrue(shardManager.getShard("events", 0).isPresent());
        shardManager.close();
    }

    private static void createShardFile(Path target) throws Exception {
        Path tmpFile = target.resolveSibling(target.getFileName() + ".build");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + tmpFile);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE events (id INTEGER, category VARCHAR)");
            stmt.execute("INSERT INTO events VALUES (1, 'only')");
        }
        Files.move(tmpFile, target, StandardCopyOption.REPLACE_EXISTING);
    }
}
