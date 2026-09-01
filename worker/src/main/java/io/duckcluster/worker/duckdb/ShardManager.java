package io.duckcluster.worker.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
    private boolean closed;

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
        executeAttach(sql, meta.filePath());
        attachedShards.put(catalogName, meta);
        LOG.info("Attached shard: {} -> {}", catalogName, meta.filePath());
    }

    public synchronized void detachShard(ShardFileMetadata meta) throws SQLException {
        String catalogName = meta.catalogName();
        if (closed || !attachedShards.containsKey(catalogName)) {
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

    private void executeAttach(String sql, Path filePath) throws SQLException {
        try (Statement stmt = adminConnection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            String message = e.getMessage();
            if (message != null
                    && (message.toLowerCase().contains("serialization")
                            || message.toLowerCase().contains("storage")
                            || message.toLowerCase().contains("version"))) {
                throw new SQLException(
                        "DuckDB shard file may be incompatible with this worker JDBC driver: "
                                + filePath.toAbsolutePath()
                                + " (" + message + ")",
                        e);
            }
            throw e;
        }
    }

    public synchronized void attachCachedShard(ShardFileMetadata meta) throws SQLException {
        String catalogName = meta.catalogName();
        if (cachedShards.containsKey(catalogName) || attachedShards.containsKey(catalogName)) {
            return;
        }
        String sql = String.format("ATTACH '%s' AS %s (READ_ONLY)",
                meta.filePath().toAbsolutePath(), catalogName);
        executeAttach(sql, meta.filePath());
        cachedShards.put(catalogName, meta);
        LOG.info("Attached cached shard: {} -> {}", catalogName, meta.filePath());
    }

    public synchronized void detachCachedShard(ShardFileMetadata meta) throws SQLException {
        String catalogName = meta.catalogName();
        if (closed || !cachedShards.containsKey(catalogName)) {
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

    public synchronized long estimateRowCount(ShardFileMetadata meta) {
        if (closed || !attachedShards.containsKey(meta.catalogName())) {
            return 0L;
        }
        String qualified = meta.catalogName() + "." + meta.tableName();
        try (Statement statement = adminConnection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + qualified)) {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            LOG.debug("Could not estimate row count for {}: {}", qualified, e.getMessage());
        }
        return 0L;
    }

    public synchronized List<ShardColumnMetadata> describeTable(ShardFileMetadata meta) {
        if (closed || !attachedShards.containsKey(meta.catalogName())) {
            return List.of();
        }
        String qualified = meta.catalogName() + "." + meta.tableName();
        List<ShardColumnMetadata> columns = new ArrayList<>();
        try (Statement statement = adminConnection.createStatement();
                ResultSet resultSet = statement.executeQuery("DESCRIBE " + qualified)) {
            while (resultSet.next()) {
                String name = resultSet.getString("column_name");
                String type = resultSet.getString("column_type");
                if (name != null && !name.isBlank()) {
                    columns.add(new ShardColumnMetadata(name, type != null ? type : "UNKNOWN"));
                }
            }
        } catch (SQLException e) {
            LOG.debug("Could not describe {}: {}", qualified, e.getMessage());
        }
        return columns;
    }

    public record ShardColumnMetadata(String name, String type) {}

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            adminConnection.close();
        } catch (SQLException e) {
            LOG.warn("Error closing admin connection: {}", e.getMessage());
        }
        LOG.info("ShardManager closed");
    }
}
