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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class TempShardCache implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TempShardCache.class);

    private final ShardManager shardManager;
    private final CoordinatorClient coordinatorClient;
    private final String workerId;
    private final Path cacheDir;
    private final int maxShards;
    private final long defaultPinLeaseMs;
    private final LinkedHashMap<String, CacheEntry> entries;
    private final Map<String, QueryPinSession> pinSessions = new LinkedHashMap<>();
    private final Set<String> pinnedKeys = new HashSet<>();
    private final ScheduledExecutorService reaper;

    public TempShardCache(ShardManager shardManager, CoordinatorClient coordinatorClient,
                          String workerId, Path cacheDir, int maxShards) throws IOException {
        this(shardManager, coordinatorClient, workerId, cacheDir, maxShards, TimeUnit.MINUTES.toMillis(10));
    }

    public TempShardCache(ShardManager shardManager, CoordinatorClient coordinatorClient,
                          String workerId, Path cacheDir, int maxShards, long defaultPinLeaseMs) throws IOException {
        this.shardManager = shardManager;
        this.coordinatorClient = coordinatorClient;
        this.workerId = workerId;
        this.cacheDir = cacheDir;
        this.maxShards = maxShards;
        this.defaultPinLeaseMs = defaultPinLeaseMs;
        this.entries = new LinkedHashMap<>(maxShards, 0.75f, true);
        Files.createDirectories(cacheDir);
        this.reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "temp-shard-pin-reaper");
            thread.setDaemon(true);
            return thread;
        });
        long reaperIntervalMs = parseReaperIntervalMs();
        long initialDelayMs = Math.min(reaperIntervalMs, 30_000L);
        this.reaper.scheduleAtFixedRate(
                this::expireAbandonedPins, initialDelayMs, reaperIntervalMs, TimeUnit.MILLISECONDS);
    }

    private static long parseReaperIntervalMs() {
        String raw = System.getenv("DUCKCLUSTER_PIN_REAPER_INTERVAL_MS");
        if (raw == null || raw.isBlank()) {
            return TimeUnit.SECONDS.toMillis(30);
        }
        try {
            return Math.max(100L, Long.parseLong(raw.trim()));
        } catch (NumberFormatException e) {
            return TimeUnit.SECONDS.toMillis(30);
        }
    }

    public synchronized void beginPinSession(String queryId, long leaseMs) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId is required for shard pin");
        }
        long ttl = leaseMs > 0 ? leaseMs : defaultPinLeaseMs;
        pinSessions.put(queryId, new QueryPinSession(queryId, System.currentTimeMillis() + ttl));
        LOG.debug("Begin shard pin for query {} (lease {}ms)", queryId, ttl);
    }

    public synchronized void endPinSession(String queryId) {
        if (queryId == null || queryId.isBlank()) {
            return;
        }
        QueryPinSession removed = pinSessions.remove(queryId);
        if (removed == null) {
            return;
        }
        for (String key : removed.pinnedKeys) {
            unpinnedIfNoSessions(key);
        }
        LOG.debug("End shard pin for query {}", queryId);
        trimToMax();
    }

    public synchronized String loadShard(String tableName, int shardId, Path tmpFile) throws IOException, SQLException {
        return loadShard(tableName, shardId, tmpFile, null);
    }

    public synchronized String loadShard(
            String tableName, int shardId, Path tmpFile, String queryId) throws IOException, SQLException {
        String key = tableName + ":" + shardId;
        if (entries.containsKey(key)) {
            LOG.debug("Cache HIT: {}_shard{}", tableName, shardId);
            entries.get(key);
            pinKeyForQuery(key, queryId);
            Files.deleteIfExists(tmpFile);
            return entries.get(key).catalogName();
        }
        LOG.debug("Cache MISS: {}_shard{}, loading...", tableName, shardId);

        makeRoom();

        String catalogName = tableName + "_shard" + shardId;
        Path targetPath = cacheDir.resolve(catalogName + ".duckdb");
        Files.move(tmpFile, targetPath, StandardCopyOption.REPLACE_EXISTING);

        ShardFileMetadata meta = new ShardFileMetadata(tableName, shardId, targetPath);
        shardManager.attachCachedShard(meta);

        entries.put(key, new CacheEntry(catalogName, targetPath, tableName, shardId));
        pinKeyForQuery(key, queryId);
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

    private void pinKeyForQuery(String key, String queryId) {
        if (queryId == null || queryId.isBlank()) {
            return;
        }
        QueryPinSession session = pinSessions.get(queryId);
        if (session == null) {
            return;
        }
        session.pinnedKeys.add(key);
        pinnedKeys.add(key);
    }

    private void unpinnedIfNoSessions(String key) {
        for (QueryPinSession session : pinSessions.values()) {
            if (session.pinnedKeys.contains(key)) {
                return;
            }
        }
        pinnedKeys.remove(key);
    }

    private synchronized void expireAbandonedPins() {
        long now = System.currentTimeMillis();
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, QueryPinSession> entry : pinSessions.entrySet()) {
            if (entry.getValue().expiresAtMs <= now) {
                expired.add(entry.getKey());
            }
        }
        for (String queryId : expired) {
            LOG.warn("Expiring abandoned shard pin for query {}", queryId);
            endPinSession(queryId);
        }
    }

    /** Runs the pin reaper once (used by unit tests). */
    synchronized void runPinReaperOnce() {
        expireAbandonedPins();
    }

    private void makeRoom() {
        while (entries.size() >= maxShards) {
            if (!pinSessions.isEmpty()) {
                if (!evictUnpinned()) {
                    return;
                }
            } else if (!evictLRU()) {
                return;
            }
        }
    }

    private void trimToMax() {
        while (entries.size() > maxShards) {
            if (!evictUnpinned() && !evictLRU()) {
                return;
            }
        }
    }

    private boolean evictUnpinned() {
        Iterator<Map.Entry<String, CacheEntry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CacheEntry> entry = iterator.next();
            if (pinnedKeys.contains(entry.getKey())) {
                continue;
            }
            evictEntry(entry.getKey(), entry.getValue());
            iterator.remove();
            return true;
        }
        return false;
    }

    private boolean evictLRU() {
        if (entries.isEmpty()) {
            return false;
        }
        Map.Entry<String, CacheEntry> eldest = entries.entrySet().iterator().next();
        evictEntry(eldest.getKey(), eldest.getValue());
        entries.remove(eldest.getKey());
        return true;
    }

    private void evictEntry(String key, CacheEntry entry) {
        try {
            ShardFileMetadata meta = new ShardFileMetadata(entry.tableName(), entry.shardId(), entry.filePath());
            shardManager.detachCachedShard(meta);
            Files.deleteIfExists(entry.filePath());
        } catch (SQLException | IOException e) {
            LOG.warn("Error evicting cached shard {}: {}", entry.catalogName(), e.getMessage());
        }
        pinnedKeys.remove(key);
        for (QueryPinSession session : pinSessions.values()) {
            session.pinnedKeys.remove(key);
        }
        LOG.info("Evicted cached shard: {}", entry.catalogName());
        notifyCoordinator();
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
        reaper.shutdownNow();
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
        pinnedKeys.clear();
        pinSessions.clear();
        LOG.info("TempShardCache closed");
    }

    private static final class QueryPinSession {
        final String queryId;
        final long expiresAtMs;
        final Set<String> pinnedKeys = new HashSet<>();

        QueryPinSession(String queryId, long expiresAtMs) {
            this.queryId = queryId;
            this.expiresAtMs = expiresAtMs;
        }
    }

    public record CacheEntry(String catalogName, Path filePath, String tableName, int shardId) {}
}
