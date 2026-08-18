package io.duckcluster.common.shard;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShardNameValidatorTest {

    @TempDir
    Path dataDir;

    @Test
    void rejectsTraversalInTableName() {
        assertThrows(IllegalArgumentException.class, () -> ShardNameValidator.validateTableName("../etc"));
    }

    @Test
    void acceptsResolvedPathUnderDataDir() {
        Path target = dataDir.resolve("events_shard0.duckdb");
        assertDoesNotThrow(() -> ShardNameValidator.validatePathUnderBase(dataDir, target));
    }

    @Test
    void rejectsResolvedPathOutsideDataDir() {
        Path outside = dataDir.resolve("..").resolve("outside.duckdb");
        assertThrows(IllegalArgumentException.class, () -> ShardNameValidator.validatePathUnderBase(dataDir, outside));
    }
}
