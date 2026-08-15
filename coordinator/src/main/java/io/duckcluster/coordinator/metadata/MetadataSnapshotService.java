package io.duckcluster.coordinator.metadata;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Periodically snapshots coordinator metadata to disk and loads it on startup (2.4). */
public final class MetadataSnapshotService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataSnapshotService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path snapshotPath;
    private final ShardCatalog shardCatalog;
    private final ClusterCatalog clusterCatalog;
    private final ScheduledExecutorService executor;

    public MetadataSnapshotService(
            ClusterConfig config,
            ShardCatalog shardCatalog,
            ClusterCatalog clusterCatalog) {
        this.shardCatalog = shardCatalog;
        this.clusterCatalog = clusterCatalog;
        Path dataDir = Path.of(config.dataDir());
        this.snapshotPath = dataDir.resolve("coordinator-metadata.json");
        long intervalMs = config.metadataSnapshotIntervalMs();
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "metadata-snapshot");
            thread.setDaemon(true);
            return thread;
        });
        loadIfPresent();
        if (intervalMs > 0) {
            executor.scheduleAtFixedRate(this::writeSnapshot, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
            LOG.info("MetadataSnapshotService writing to {} every {}s", snapshotPath, intervalMs / 1000);
        }
    }

    public void loadIfPresent() {
        if (!Files.isRegularFile(snapshotPath)) {
            return;
        }
        try {
            JsonNode root = MAPPER.readTree(snapshotPath.toFile());
            List<TableShardConfig> tables = parseTables(root.path("tables"));
            Map<String, Set<String>> ownership = parseOwnership(root.path("ownership"));
            shardCatalog.restoreFromSnapshot(tables, ownership);
            LOG.info(
                    "Loaded metadata snapshot from {} ({} tables, {} ownership entries)",
                    snapshotPath,
                    tables.size(),
                    ownership.size());
        } catch (Exception e) {
            LOG.warn("Failed to load metadata snapshot from {}: {}", snapshotPath, e.getMessage());
        }
    }

    public void writeSnapshot() {
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("writtenAt", Instant.now().toString());
            body.put("tables", catalogTablesForSnapshot());
            body.put("ownership", shardCatalog.exportOwnership());
            body.put("underReplicatedShards", shardCatalog.getUnderReplicatedShards());
            Path temp = snapshotPath.resolveSibling(snapshotPath.getFileName() + ".tmp");
            Files.createDirectories(snapshotPath.getParent());
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), body);
            Files.move(temp, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            LOG.debug("Wrote metadata snapshot to {}", snapshotPath);
        } catch (Exception e) {
            LOG.warn("Failed to write metadata snapshot: {}", e.getMessage());
        }
    }

    private List<Map<String, Object>> catalogTablesForSnapshot() {
        List<Map<String, Object>> tables = new ArrayList<>();
        for (TableShardConfig table : clusterCatalog.listTables()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", table.tableName());
            entry.put("shardCount", table.shardCount());
            entry.put("shardKey", table.shardKey());
            tables.add(entry);
        }
        return tables;
    }

    private static List<TableShardConfig> parseTables(JsonNode tablesNode) {
        List<TableShardConfig> tables = new ArrayList<>();
        if (!tablesNode.isArray()) {
            return tables;
        }
        for (JsonNode node : tablesNode) {
            String name = node.path("name").asText(null);
            if (name == null || name.isBlank()) {
                continue;
            }
            int shardCount = node.path("shardCount").asInt(1);
            String shardKey = node.path("shardKey").asText("rowid");
            tables.add(new TableShardConfig(name, shardCount, shardKey));
        }
        return tables;
    }

    private static Map<String, Set<String>> parseOwnership(JsonNode ownershipNode) {
        Map<String, Set<String>> ownership = new LinkedHashMap<>();
        if (!ownershipNode.isObject()) {
            return ownership;
        }
        ownershipNode.fields().forEachRemaining(entry -> {
            Set<String> keys = new HashSet<>();
            if (entry.getValue().isArray()) {
                for (JsonNode keyNode : entry.getValue()) {
                    keys.add(keyNode.asText());
                }
            }
            ownership.put(entry.getKey(), keys);
        });
        return ownership;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
