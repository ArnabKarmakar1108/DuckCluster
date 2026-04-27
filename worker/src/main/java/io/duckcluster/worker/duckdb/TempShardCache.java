package io.duckcluster.worker.duckdb;

import io.duckcluster.worker.client.CoordinatorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TempShardCache implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TempShardCache.class);

    private final ShardManager shardManager;
    private final CoordinatorClient coordinatorClient;
    private final String workerId;
    private final Path cacheDir;
    private final int maxShards;
    private final LinkedHashMap<String, CacheEntry> entries;

    public TempShardCache(ShardManager shardManager, CoordinatorClient coordinatorClient,
                          String workerId, Path cacheDir, int maxShards) throws IOException {
        this.shardManager = shardManager;
        this.coordinatorClient = coordinatorClient;
        this.workerId = workerId;
        this.cacheDir = cacheDir;
        this.maxShards = maxShards;
        this.entries = new LinkedHashMap<>(maxShards, 0.75f, true);
        Files.createDirectories(cacheDir);
    }

    public synchronized String loadShard(String tableName, int shardId, Path tmpFile) throws IOException, SQLException {
        String key = tableName + ":" + shardId;
        if (entries.containsKey(key)) {
            LOG.debug("Cache HIT: {}_shard{}", tableName, shardId);
            entries.get(key);
            return entries.get(key).catalogName();
        }
        LOG.debug("Cache MISS: {}_shard{}, loading...", tableName, shardId);

        while (entries.size() >= maxShards) {
            evictLRU();
        }

        String catalogName = tableName + "_shard" + shardId;
        Path targetPath = cacheDir.resolve(catalogName + ".duckdb");
        Files.move(tmpFile, targetPath, StandardCopyOption.REPLACE_EXISTING);

        ShardFileMetadata meta = new ShardFileMetadata(tableName, shardId, targetPath);
        shardManager.attachCachedShard(meta);

        entries.put(key, new CacheEntry(catalogName, targetPath, tableName, shardId));
        LOG.info("Cached shard: {} (cache size: {}/{})", catalogName, entries.size(), maxShards);

        notifyCoordinator();
        return catalogName;
    }

    public synchronized boolean hasShard(String tableName, int shardId) {
        return entries.containsKey(tableName + ":" + shardId);
    }

    public synchronized void touch(String tableName, int shardId) {
        entries.get(tableName + ":" + shardId);
    }

    public synchronized List<CacheEntry> getCachedShards() {
        return new ArrayList<>(entries.values());
    }

    private void evictLRU() {
        Map.Entry<String, CacheEntry> eldest = entries.entrySet().iterator().next();
        CacheEntry entry = eldest.getValue();
        try {
            ShardFileMetadata meta = new ShardFileMetadata(entry.tableName(), entry.shardId(), entry.filePath());
            shardManager.detachCachedShard(meta);
            Files.deleteIfExists(entry.filePath());
        } catch (SQLException | IOException e) {
            LOG.warn("Error evicting cached shard {}: {}", entry.catalogName(), e.getMessage());
        }
        entries.remove(eldest.getKey());
        LOG.info("Evicted cached shard: {}", entry.catalogName());
    }

    private void notifyCoordinator() {
        try {
            coordinatorClient.updateShardCache(workerId, getCachedShards());
        } catch (Exception e) {
            LOG.warn("Failed to notify coordinator of cache update: {}", e.getMessage());
        }
    }

    @Override
    public synchronized void close() {
        for (CacheEntry entry : entries.values()) {
            try {
                ShardFileMetadata meta = new ShardFileMetadata(entry.tableName(), entry.shardId(), entry.filePath());
                shardManager.detachCachedShard(meta);
                Files.deleteIfExists(entry.filePath());
            } catch (SQLException | IOException e) {
                LOG.warn("Error closing cached shard {}: {}", entry.catalogName(), e.getMessage());
            }
        }
        entries.clear();
        LOG.info("TempShardCache closed");
    }

    public record CacheEntry(String catalogName, Path filePath, String tableName, int shardId) {}
}
