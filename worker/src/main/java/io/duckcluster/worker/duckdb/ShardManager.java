package io.duckcluster.worker.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class ShardManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ShardManager.class);

    private final Connection adminConnection;
    private final Path dataDir;
    private final Map<String, ShardFileMetadata> attachedShards = new ConcurrentHashMap<>();
    private final Map<String, ShardFileMetadata> cachedShards = new ConcurrentHashMap<>();

    public ShardManager(String mainDbPath, Path dataDir) throws SQLException {
        this.adminConnection = DriverManager.getConnection("jdbc:duckdb:" + mainDbPath);
        this.dataDir = dataDir;
        LOG.info("ShardManager created with admin connection to {}", mainDbPath);
    }

    public synchronized void attachShard(ShardFileMetadata meta) throws SQLException {
        String catalogName = meta.catalogName();
        if (attachedShards.containsKey(catalogName)) {
            return;
        }
        String sql = String.format("ATTACH '%s' AS %s",
                meta.filePath().toAbsolutePath(), catalogName);
        try (Statement stmt = adminConnection.createStatement()) {
            stmt.execute(sql);
        }
        attachedShards.put(catalogName, meta);
        LOG.info("Attached shard: {} -> {}", catalogName, meta.filePath());
    }

    public synchronized void detachShard(ShardFileMetadata meta) throws SQLException {
        String catalogName = meta.catalogName();
        if (!attachedShards.containsKey(catalogName)) {
            return;
        }
        try (Statement stmt = adminConnection.createStatement()) {
            stmt.execute("DETACH " + catalogName);
        }
        attachedShards.remove(catalogName);
        LOG.info("Detached shard: {}", catalogName);
    }

    public List<ShardFileMetadata> getAttachedShards() {
        return new ArrayList<>(attachedShards.values());
    }

    public Optional<ShardFileMetadata> getShard(String tableName, int shardId) {
        String catalogName = tableName + "_shard" + shardId;
        return Optional.ofNullable(attachedShards.get(catalogName));
    }

    public int scanAndAttachAll() throws IOException, SQLException {
        int count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.duckdb")) {
            for (Path path : stream) {
                Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
                if (meta.isPresent()) {
                    try {
                        attachShard(meta.get());
                        count++;
                    } catch (SQLException e) {
                        LOG.warn("Failed to attach {}: {}", path, e.getMessage());
                    }
                }
            }
        }
        LOG.info("Batch-attached {} shard files from {}", count, dataDir);
        return count;
    }

    public synchronized void attachCachedShard(ShardFileMetadata meta) throws SQLException {
        String catalogName = meta.catalogName();
        if (cachedShards.containsKey(catalogName) || attachedShards.containsKey(catalogName)) {
            return;
        }
        String sql = String.format("ATTACH '%s' AS %s (READ_ONLY)",
                meta.filePath().toAbsolutePath(), catalogName);
        try (Statement stmt = adminConnection.createStatement()) {
            stmt.execute(sql);
        }
        cachedShards.put(catalogName, meta);
        LOG.info("Attached cached shard: {} -> {}", catalogName, meta.filePath());
    }

    public synchronized void detachCachedShard(ShardFileMetadata meta) throws SQLException {
        String catalogName = meta.catalogName();
        if (!cachedShards.containsKey(catalogName)) {
            return;
        }
        try (Statement stmt = adminConnection.createStatement()) {
            stmt.execute("DETACH " + catalogName);
        }
        cachedShards.remove(catalogName);
        LOG.info("Detached cached shard: {}", catalogName);
    }

    public boolean hasCachedShard(String tableName, int shardId) {
        String catalogName = tableName + "_shard" + shardId;
        return cachedShards.containsKey(catalogName);
    }

    public int scanAndAttachDimensions(Path dimensionsDir) throws IOException, SQLException {
        int count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dimensionsDir, "*.duckdb")) {
            for (Path path : stream) {
                String filename = path.getFileName().toString();
                String catalogName = filename.substring(0, filename.length() - ".duckdb".length());
                try {
                    attachDimensionTable(catalogName, path);
                    count++;
                } catch (SQLException e) {
                    LOG.warn("Failed to attach dimension {}: {}", catalogName, e.getMessage());
                }
            }
        }
        if (count > 0) {
            LOG.info("Attached {} dimension tables from {}", count, dimensionsDir);
        }
        return count;
    }

    public synchronized void attachDimensionTable(String catalogName, Path filePath) throws SQLException {
        String sql = String.format("ATTACH '%s' AS %s (READ_ONLY)", filePath.toAbsolutePath(), catalogName);
        try (Statement stmt = adminConnection.createStatement()) {
            stmt.execute(sql);
        }
        LOG.info("Attached dimension table: {} -> {}", catalogName, filePath);
    }

    public Path getDataDir() {
        return dataDir;
    }

    @Override
    public synchronized void close() {
        try {
            adminConnection.close();
        } catch (SQLException e) {
            LOG.warn("Error closing admin connection: {}", e.getMessage());
        }
        LOG.info("ShardManager closed");
    }
}
