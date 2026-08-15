package io.duckcluster.coordinator.catalog;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.TableColumn;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.common.routing.ConsistentHashRing;
import io.duckcluster.proto.v1.ShardOwnership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ShardCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(ShardCatalog.class);

    private final ConsistentHashRing ring;
    private final int replicationFactor;
    private final ClusterCatalog clusterCatalog;
    private final Map<String, Set<String>> actualOwnership = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> shardCache = new ConcurrentHashMap<>();
    private final Map<String, Long> rowsByShardKey = new ConcurrentHashMap<>();

    public ShardCatalog(ConsistentHashRing ring, int replicationFactor, ClusterCatalog clusterCatalog) {
        this.ring = ring;
        this.replicationFactor = replicationFactor;
        this.clusterCatalog = clusterCatalog;
    }

    public ShardCatalog(ConsistentHashRing ring, int replicationFactor) {
        this(ring, replicationFactor, new ClusterCatalog());
    }

    public void onWorkerAdded(String workerId) {
        ring.addWorker(workerId);
        LOG.info("Worker {} added to hash ring (total workers: {})", workerId, ring.getWorkerCount());
    }

    public void onWorkerRemoved(String workerId) {
        ring.removeWorker(workerId);
        actualOwnership.remove(workerId);
        shardCache.remove(workerId);
        LOG.info("Worker {} removed from hash ring (total workers: {})", workerId, ring.getWorkerCount());
    }

    public void registerOwnership(String workerId, List<ShardOwnership> shards) {
        Set<String> keys = new HashSet<>();
        for (ShardOwnership shard : shards) {
            keys.add(shardKey(shard.getTableName(), shard.getShardId()));
            clusterCatalog.registerTable(shard.getTableName(), shard.getShardId() + 1);
            if (shard.getRowCount() > 0) {
                String rowKey = shardKey(shard.getTableName(), shard.getShardId());
                rowsByShardKey.merge(rowKey, shard.getRowCount(), Math::max);
            }
            if (!shard.getColumnsList().isEmpty()) {
                List<TableColumn> columns = shard.getColumnsList().stream()
                        .map(col -> new TableColumn(col.getName(), col.getType()))
                        .toList();
                if (!clusterCatalog.updateTableSchemaIfCompatible(shard.getTableName(), columns)) {
                    LOG.warn(
                            "Schema drift for table {} reported by worker {} (shard {}); keeping first registered schema",
                            shard.getTableName(),
                            workerId,
                            shard.getShardId());
                }
            }
        }
        actualOwnership.put(workerId, keys);
        refreshEstimatedRowCounts();
        LOG.debug("Worker {} reports {} shards", workerId, shards.size());
    }

    private void refreshEstimatedRowCounts() {
        Map<String, Long> totals = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : rowsByShardKey.entrySet()) {
            String[] parts = entry.getKey().split(":", 2);
            if (parts.length != 2) {
                continue;
            }
            totals.merge(parts[0], entry.getValue(), Long::sum);
        }
        clusterCatalog.replaceEstimatedRowCounts(totals);
    }

    public List<String> getOwners(String tableName, int shardId) {
        return ring.getOwners(tableName + ":" + shardId, replicationFactor);
    }

    public List<String> getActualOwners(String tableName, int shardId) {
        String key = shardKey(tableName, shardId);
        List<String> owners = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : actualOwnership.entrySet()) {
            if (entry.getValue().contains(key)) {
                owners.add(entry.getKey());
            }
        }
        return owners;
    }

    public List<UnderReplicatedShard> getUnderReplicatedShards() {
        Set<String> allShardKeys = new HashSet<>();
        for (Set<String> keys : actualOwnership.values()) {
            allShardKeys.addAll(keys);
        }

        List<UnderReplicatedShard> result = new ArrayList<>();
        for (String key : allShardKeys) {
            String[] parts = key.split(":", 2);
            String tableName = parts[0];
            int shardId = Integer.parseInt(parts[1]);

            List<String> actual = getActualOwners(tableName, shardId);
            if (actual.size() < replicationFactor) {
                List<String> expected = getOwners(tableName, shardId);
                List<String> missingOn = new ArrayList<>();
                for (String worker : expected) {
                    if (!actual.contains(worker)) {
                        missingOn.add(worker);
                    }
                }
                if (!missingOn.isEmpty() && !actual.isEmpty()) {
                    result.add(new UnderReplicatedShard(
                            tableName, shardId, actual.get(0), missingOn));
                }
            }
        }
        return result;
    }

    public void registerCachedShards(String workerId, List<ShardOwnership> shards) {
        Set<String> keys = new HashSet<>();
        for (ShardOwnership shard : shards) {
            keys.add(shardKey(shard.getTableName(), shard.getShardId()));
        }
        shardCache.put(workerId, keys);
        LOG.debug("Worker {} reports {} cached shards", workerId, shards.size());
    }

    public List<String> getCachedWorkers(String tableName, int shardId) {
        String key = shardKey(tableName, shardId);
        List<String> workers = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : shardCache.entrySet()) {
            if (entry.getValue().contains(key)) {
                workers.add(entry.getKey());
            }
        }
        return workers;
    }

    public int getWorkerCount() {
        return ring.getWorkerCount();
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public Map<String, Set<String>> exportOwnership() {
        Map<String, Set<String>> copy = new ConcurrentHashMap<>();
        actualOwnership.forEach((workerId, keys) -> copy.put(workerId, Set.copyOf(keys)));
        return copy;
    }

    public void restoreFromSnapshot(List<TableShardConfig> tables, Map<String, Set<String>> ownership) {
        if (tables != null) {
            for (TableShardConfig table : tables) {
                clusterCatalog.restoreTable(table);
            }
        }
        if (ownership != null && !ownership.isEmpty()) {
            actualOwnership.clear();
            ownership.forEach((workerId, keys) -> actualOwnership.put(workerId, new HashSet<>(keys)));
            LOG.info("Restored shard ownership for {} workers from metadata snapshot", ownership.size());
        }
    }

    public ClusterCatalog clusterCatalog() {
        return clusterCatalog;
    }

    private static String shardKey(String tableName, int shardId) {
        return tableName + ":" + shardId;
    }

    public record UnderReplicatedShard(
            String tableName, int shardId, String sourceWorker, List<String> targetWorkers) {}
}
