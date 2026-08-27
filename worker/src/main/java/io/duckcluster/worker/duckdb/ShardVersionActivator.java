package io.duckcluster.worker.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Optional;

/** Atomically promotes staged shard files into the live data directory (5.2). */
public final class ShardVersionActivator {
    private static final Logger LOG = LoggerFactory.getLogger(ShardVersionActivator.class);

    private final Path dataDir;
    private final ShardManager shardManager;

    public ShardVersionActivator(Path dataDir, ShardManager shardManager) {
        this.dataDir = dataDir;
        this.shardManager = shardManager;
    }

    public int activate(String tableName, long version) throws IOException, SQLException {
        String tableKey = tableName.toLowerCase(Locale.ROOT);
        Path stagingDir = dataDir.resolve(".staging").resolve("v" + version);
        if (!Files.isDirectory(stagingDir)) {
            return 0;
        }
        int activated = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(stagingDir, "*.duckdb")) {
            for (Path staged : stream) {
                Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(staged);
                if (meta.isEmpty() || !meta.get().tableName().equalsIgnoreCase(tableKey)) {
                    continue;
                }
                promoteShard(meta.get(), staged);
                activated++;
            }
        }
        if (activated > 0) {
            LOG.info("Activated {} shard file(s) for table {} version {}", activated, tableName, version);
        }
        return activated;
    }

    private void promoteShard(ShardFileMetadata stagedMeta, Path stagedPath)
            throws IOException, SQLException {
        String catalogName = stagedMeta.catalogName();
        Path livePath = dataDir.resolve(stagedMeta.filePath().getFileName());

        shardManager.getShard(stagedMeta.tableName(), stagedMeta.shardId()).ifPresent(existing -> {
            try {
                shardManager.detachShard(existing);
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to detach " + catalogName, e);
            }
        });

        Path swapTemp = dataDir.resolve(catalogName + ".swap.tmp");
        Files.move(stagedPath, swapTemp, StandardCopyOption.REPLACE_EXISTING);
        Files.move(swapTemp, livePath, StandardCopyOption.REPLACE_EXISTING);

        ShardFileMetadata liveMeta = new ShardFileMetadata(
                stagedMeta.tableName(), stagedMeta.shardId(), livePath);
        shardManager.attachShard(liveMeta);
    }
}
