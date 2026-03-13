package io.duckcluster.worker.duckdb;

import io.duckcluster.proto.v1.ShardOwnership;
import io.duckcluster.worker.client.CoordinatorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class ShardFileWatcher implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ShardFileWatcher.class);

    private final Path dataDir;
    private final ShardManager shardManager;
    private final CoordinatorClient coordinatorClient;
    private final String workerId;
    private final ScheduledExecutorService executor;

    public ShardFileWatcher(Path dataDir, ShardManager shardManager,
                            CoordinatorClient coordinatorClient, String workerId,
                            long intervalMs) {
        this.dataDir = dataDir;
        this.shardManager = shardManager;
        this.coordinatorClient = coordinatorClient;
        this.workerId = workerId;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "shard-file-watcher");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::poll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        LOG.info("ShardFileWatcher started for {} (interval={}ms)", dataDir, intervalMs);
    }

    private void poll() {
        try {
            Set<String> currentFiles = new HashSet<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.duckdb")) {
                for (Path path : stream) {
                    Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
                    if (meta.isPresent()) {
                        currentFiles.add(meta.get().catalogName());
                    }
                }
            }

            Set<String> attachedNames = shardManager.getAttachedShards().stream()
                    .map(ShardFileMetadata::catalogName)
                    .collect(Collectors.toSet());

            boolean changed = false;

            for (String name : currentFiles) {
                if (!attachedNames.contains(name)) {
                    Path path = findShardFile(name);
                    if (path != null) {
                        Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
                        if (meta.isPresent()) {
                            shardManager.attachShard(meta.get());
                            changed = true;
                            LOG.info("Watcher detected new shard: {}", name);
                        }
                    }
                }
            }

            for (String name : attachedNames) {
                if (!currentFiles.contains(name)) {
                    shardManager.getShard(parseTable(name), parseShardId(name))
                            .ifPresent(meta -> {
                                try {
                                    shardManager.detachShard(meta);
                                    LOG.info("Watcher detected removed shard: {}", name);
                                } catch (SQLException e) {
                                    LOG.warn("Failed to detach {}: {}", name, e.getMessage());
                                }
                            });
                    changed = true;
                }
            }

            if (changed) {
                notifyCoordinator();
            }
        } catch (IOException | SQLException e) {
            LOG.warn("Watcher poll error: {}", e.getMessage());
        }
    }

    private void notifyCoordinator() {
        try {
            List<ShardOwnership> ownedShards = shardManager.getAttachedShards().stream()
                    .map(meta -> ShardOwnership.newBuilder()
                            .setTableName(meta.tableName())
                            .setShardId(meta.shardId())
                            .setRowCount(0)
                            .build())
                    .toList();
            coordinatorClient.updateShardOwnership(workerId, ownedShards);
        } catch (Exception e) {
            LOG.warn("Failed to notify coordinator of ownership change: {}", e.getMessage());
        }
    }

    private Path findShardFile(String catalogName) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.duckdb")) {
            for (Path path : stream) {
                Optional<ShardFileMetadata> meta = ShardFileMetadata.fromPath(path);
                if (meta.isPresent() && meta.get().catalogName().equals(catalogName)) {
                    return path;
                }
            }
        } catch (IOException e) {
            LOG.warn("Error searching for shard file {}: {}", catalogName, e.getMessage());
        }
        return null;
    }

    private static String parseTable(String catalogName) {
        int idx = catalogName.lastIndexOf("_shard");
        return idx > 0 ? catalogName.substring(0, idx) : catalogName;
    }

    private static int parseShardId(String catalogName) {
        int idx = catalogName.lastIndexOf("_shard");
        if (idx < 0) return -1;
        return Integer.parseInt(catalogName.substring(idx + 6));
    }

    @Override
    public void close() {
        executor.shutdownNow();
        LOG.info("ShardFileWatcher stopped");
    }
}
