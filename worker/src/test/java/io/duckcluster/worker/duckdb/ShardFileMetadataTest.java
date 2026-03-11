package io.duckcluster.worker.duckdb;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ShardFileMetadataTest {

    @Test
    void fromPath_validShardFile() {
        Path path = Path.of("/data/events_shard0.duckdb");
        Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
        assertTrue(meta.isPresent());
        assertEquals("events", meta.get().tableName());
        assertEquals(0, meta.get().shardId());
        assertEquals(path, meta.get().filePath());
    }

    @Test
    void fromPath_multiDigitShardId() {
        Path path = Path.of("/data/orders_shard42.duckdb");
        Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
        assertTrue(meta.isPresent());
        assertEquals("orders", meta.get().tableName());
        assertEquals(42, meta.get().shardId());
    }

    @Test
    void fromPath_underscoreInTableName() {
        Path path = Path.of("/data/user_events_shard3.duckdb");
        Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
        assertTrue(meta.isPresent());
        assertEquals("user_events", meta.get().tableName());
        assertEquals(3, meta.get().shardId());
    }

    @Test
    void fromPath_invalidExtension_returnsEmpty() {
        Path path = Path.of("/data/events_shard0.parquet");
        Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
        assertTrue(meta.isEmpty());
    }

    @Test
    void fromPath_noShardSuffix_returnsEmpty() {
        Path path = Path.of("/data/worker-1.duckdb");
        Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
        assertTrue(meta.isEmpty());
    }

    @Test
    void fromPath_tempFile_returnsEmpty() {
        Path path = Path.of("/data/events_shard0.duckdb.tmp");
        Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
        assertTrue(meta.isEmpty());
    }

    @Test
    void catalogName_returnsCorrectFormat() {
        ShardFileMetadata meta = new ShardFileMetadata("events", 2, Path.of("/data/events_shard2.duckdb"));
        assertEquals("events_shard2", meta.catalogName());
    }
}
