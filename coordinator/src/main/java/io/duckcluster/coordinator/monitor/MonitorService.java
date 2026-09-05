package io.duckcluster.coordinator.monitor;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.TableColumn;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.execution.AsyncQueryService;
import io.duckcluster.coordinator.execution.QueryExecutionService;
import io.duckcluster.common.registry.WorkerRegistry;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public final class MonitorService {
    private final ClusterConfig config;
    private final WorkerRegistry registry;
    private final ShardCatalog shardCatalog;
    private final ClusterCatalog clusterCatalog;
    private final QueryActivityTracker activityTracker;
    private final QueryExecutionService queryExecutionService;
    private final Duration heartbeatTimeout;

    public MonitorService(
            ClusterConfig config,
            WorkerRegistry registry,
            ShardCatalog shardCatalog,
            ClusterCatalog clusterCatalog,
            QueryActivityTracker activityTracker,
            QueryExecutionService queryExecutionService) {
        this.config = config;
        this.registry = registry;
        this.shardCatalog = shardCatalog;
        this.clusterCatalog = clusterCatalog;
        this.activityTracker = activityTracker;
        this.queryExecutionService = queryExecutionService;
        this.heartbeatTimeout =
                config.heartbeatInterval().multipliedBy(config.heartbeatMissThreshold());
    }

    public void declareCatalogTable(String name, int shardCount, String shardKey) {
        clusterCatalog.declareTable(new TableShardConfig(name, shardCount, shardKey));
    }

    public boolean dropCatalogTable(String name) {
        return clusterCatalog.removeTable(name);
    }

    public Optional<Map<String, Object>> describeTable(String tableName) {
        if (!clusterCatalog.hasTable(tableName)) {
            return Optional.empty();
        }
        TableShardConfig table = clusterCatalog.table(tableName);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", table.tableName());
        body.put("shardCount", table.shardCount());
        body.put("shardKey", table.shardKey());
        body.put("activeDataVersion", clusterCatalog.activeDataVersion(table.tableName()));
        body.put("estimatedRowCount", clusterCatalog.estimatedRowCount(table.tableName()));
        List<Map<String, String>> columns = new ArrayList<>();
        for (TableColumn column : clusterCatalog.tableColumns(table.tableName())) {
            Map<String, String> columnView = new LinkedHashMap<>();
            columnView.put("name", column.name());
            columnView.put("type", column.type());
            columns.add(columnView);
        }
        body.put("columns", columns);
        return Optional.of(body);
    }

    public Map<String, Object> catalogTables() {
        List<Map<String, Object>> tables = new ArrayList<>();
        for (TableShardConfig table : clusterCatalog.listTables()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", table.tableName());
            entry.put("shardCount", table.shardCount());
            entry.put("shardKey", table.shardKey());
            entry.put("activeDataVersion", clusterCatalog.activeDataVersion(table.tableName()));
            entry.put("estimatedRowCount", clusterCatalog.estimatedRowCount(table.tableName()));
            List<Map<String, String>> columns = new ArrayList<>();
            for (TableColumn column : clusterCatalog.tableColumns(table.tableName())) {
                Map<String, String> columnView = new LinkedHashMap<>();
                columnView.put("name", column.name());
                columnView.put("type", column.type());
                columns.add(columnView);
            }
            if (!columns.isEmpty()) {
                entry.put("columns", columns);
            }
            tables.add(entry);
        }
        return Map.of("tables", tables);
    }

    public boolean isReady() {
        return registry.workerCount() > 0 && !clusterCatalog.getShardedTableNames().isEmpty();
    }

    public Optional<Map<String, Object>> queryProfile(String queryId, AsyncQueryService asyncQueryService) {
        Optional<QueryActivityTracker.ActiveQuery> active = activityTracker.findActive(queryId);
        if (active.isPresent()) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("queryId", queryId);
            body.put("state", "RUNNING");
            body.put("active", toActiveView(active.get()));
            return Optional.of(body);
        }

        Optional<AsyncQueryService.StatusView> asyncStatus = asyncQueryService.status(queryId);
        if (asyncStatus.isPresent()) {
            AsyncQueryService.StatusView status = asyncStatus.get();
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("queryId", queryId);
            body.put("state", status.status().name());
            body.put("phase", status.phase() != null ? status.phase().name() : null);
            body.put("fragmentsDone", status.fragmentsDone());
            body.put("fragmentsTotal", status.fragmentsTotal());
            body.put("elapsedMs", status.elapsedMs());
            body.put("error", status.error());
            if (status.stats() != null) {
                body.put("mergeStrategy", status.stats().mergeStrategy().name());
                body.put("durationMs", status.stats().durationMs());
                body.put("workersUsed", status.stats().workersUsed());
                body.put("fragmentsExecuted", status.stats().fragmentsExecuted());
                body.put("workerDurationsMs", status.stats().workerDurationsMs());
            }
            return Optional.of(body);
        }

        return activityTracker.findRecent(queryId).map(recent -> {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("queryId", queryId);
            body.put("state", "FINISHED");
            body.put("recent", toRecentView(recent));
            return body;
        });
    }

    public Map<String, Object> summary() {
        Instant now = Instant.now();
        List<WorkerRegistry.WorkerRecord> workers = registry.listWorkers();
        int healthyWorkers = 0;
        List<Map<String, Object>> workerViews = new ArrayList<>();

        for (WorkerRegistry.WorkerRecord worker : workers) {
            long heartbeatAgeMs = Duration.between(worker.lastHeartbeatAt(), now).toMillis();
            boolean stale = heartbeatAgeMs > heartbeatTimeout.toMillis();
            if (worker.status() == WorkerRegistry.WorkerStatus.HEALTHY && !stale) {
                healthyWorkers++;
            }
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("workerId", worker.workerId());
            view.put("host", worker.host());
            view.put("port", worker.port());
            view.put("numThreads", worker.numThreads());
            view.put("status", worker.status().name());
            view.put("stale", stale);
            view.put("lastHeartbeatAgeMs", heartbeatAgeMs);
            view.put("load", queryExecutionService.workerLoad(worker.workerId()));
            workerViews.add(view);
        }

        Map<String, Object> cluster = new LinkedHashMap<>();
        cluster.put("status", registry.isHealthy() && healthyWorkers == workers.size() ? "UP" : "DEGRADED");
        cluster.put("workerCount", workers.size());
        cluster.put("healthyWorkers", healthyWorkers);
        cluster.put("replicationFactor", shardCatalog.getReplicationFactor());

        Map<String, Object> coordinator = new LinkedHashMap<>();
        coordinator.put("host", config.coordinatorHost());
        coordinator.put("httpPort", config.coordinatorHttpPort());
        coordinator.put("grpcPort", config.coordinatorGrpcPort());

        List<Map<String, Object>> activeQueries = activityTracker.activeQueries().stream()
                .map(this::toActiveView)
                .toList();
        List<Map<String, Object>> recentQueries = activityTracker.recentQueries().stream()
                .map(this::toRecentView)
                .toList();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("cluster", cluster);
        body.put("coordinator", coordinator);
        body.put("workers", workerViews);
        body.put("activeQueries", activeQueries);
        body.put("recentQueries", recentQueries);
        body.put("refreshedAtEpochMs", now.toEpochMilli());
        return body;
    }

    public Map<String, Object> shards() {
        Set<String> tableNames = new TreeSet<>(clusterCatalog.getShardedTableNames());
        List<Map<String, Object>> tables = new ArrayList<>();

        for (String tableName : tableNames) {
            TableShardConfig table = clusterCatalog.table(tableName);
            List<Map<String, Object>> shards = new ArrayList<>();
            for (int shardId = 0; shardId < table.shardCount(); shardId++) {
                Map<String, Object> shard = new LinkedHashMap<>();
                shard.put("shardId", shardId);
                shard.put("owners", shardCatalog.getActualOwners(tableName, shardId));
                shard.put("expectedOwners", shardCatalog.getOwners(tableName, shardId));
                shard.put("cachedWorkers", shardCatalog.getCachedWorkers(tableName, shardId));
                shards.add(shard);
            }
            Map<String, Object> tableView = new LinkedHashMap<>();
            tableView.put("tableName", table.tableName());
            tableView.put("shardCount", table.shardCount());
            tableView.put("shards", shards);
            tables.add(tableView);
        }

        List<Map<String, Object>> underReplicated = shardCatalog.getUnderReplicatedShards().stream()
                .map(shard -> {
                    Map<String, Object> view = new LinkedHashMap<>();
                    view.put("tableName", shard.tableName());
                    view.put("shardId", shard.shardId());
                    view.put("sourceWorker", shard.sourceWorker());
                    view.put("targetWorkers", shard.targetWorkers());
                    return view;
                })
                .toList();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("tables", tables);
        body.put("underReplicated", underReplicated);
        body.put("refreshedAtEpochMs", Instant.now().toEpochMilli());
        return body;
    }

    private Map<String, Object> toActiveView(QueryActivityTracker.ActiveQuery query) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("queryId", query.queryId());
        view.put("phase", query.phase().name());
        view.put("elapsedMs", query.elapsedMs());
        view.put("sqlPreview", query.sqlPreview());
        view.put("fragmentsTotal", query.fragmentsTotal());
        view.put("fragmentsDone", query.fragmentsDone());
        return view;
    }

    private Map<String, Object> toRecentView(QueryActivityTracker.RecentQuery query) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("queryId", query.queryId());
        view.put("durationMs", query.durationMs());
        view.put("mergeStrategy", query.mergeStrategy());
        view.put("status", query.status());
        view.put("sqlPreview", query.sqlPreview());
        view.put("completedAtEpochMs", query.completedAtEpochMs());
        return view;
    }
}
