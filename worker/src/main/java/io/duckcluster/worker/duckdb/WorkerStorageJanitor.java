package io.duckcluster.worker.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Removes stale transfer temp files on worker startup (3.5). */
public final class WorkerStorageJanitor {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerStorageJanitor.class);

    private WorkerStorageJanitor() {}

    public static void cleanupStaleTempFiles(Path dataDir) {
        cleanupGlob(dataDir.resolve(".transfer-tmp"), "*.tmp");
        cleanupGlob(dataDir, "*.swap.tmp");
    }

    private static void cleanupGlob(Path directory, String pattern) {
        if (!Files.isDirectory(directory)) {
            return;
        }
        try (var stream = Files.newDirectoryStream(directory, pattern)) {
            for (Path path : stream) {
                try {
                    Files.deleteIfExists(path);
                    LOG.debug("Removed stale temp file {}", path);
                } catch (IOException e) {
                    LOG.warn("Failed to remove stale temp file {}: {}", path, e.getMessage());
                }
            }
        } catch (IOException e) {
            LOG.warn("Failed to scan {} for stale files: {}", directory, e.getMessage());
        }
    }
}
