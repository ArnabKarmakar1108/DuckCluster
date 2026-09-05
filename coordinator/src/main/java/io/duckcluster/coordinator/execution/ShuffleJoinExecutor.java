package io.duckcluster.coordinator.execution;

import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.ShuffleJoinTable;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import io.duckcluster.proto.v1.ShuffleTableRequest;
import io.duckcluster.proto.v1.ShuffleTableResponse;
import io.duckcluster.proto.v1.ShuffleTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

/**
 * Coordinates the hash shuffle for {@link PlannedQuery#shuffleJoinTables()}: every source shard is
 * re-bucketed by a worker that holds it, and bucket {@code i} is pushed worker-to-worker to the
 * worker that will run fragment {@code i}. Rows never pass through the coordinator.
 */
final class ShuffleJoinExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ShuffleJoinExecutor.class);

    record ShuffleStats(long rows, long bytes, int pieces, long durationMs) {}

    private final WorkerRegistry registry;
    private final ShardCatalog shardCatalog;
    private final WorkerNodeClient workerClient;
    private final ExecutorService executor;

    ShuffleJoinExecutor(
            WorkerRegistry registry, ShardCatalog shardCatalog, WorkerNodeClient workerClient, ExecutorService executor) {
        this.registry = registry;
        this.shardCatalog = shardCatalog;
        this.workerClient = workerClient;
        this.executor = executor;
    }

    /** Workers that will hold bucket tables and must be told to drop them when the query ends. */
    static Set<String> bucketHolders(PlannedQuery plan, Map<Integer, String> fragmentTargets) {
        return plan.shuffleJoinTables().isEmpty() ? Set.of() : new LinkedHashSet<>(fragmentTargets.values());
    }

    ShuffleStats run(String queryId, PlannedQuery plan, Map<Integer, String> fragmentTargets) {
        long start = System.currentTimeMillis();
        long rows = 0;
        long bytes = 0;
        int pieces = 0;
        for (ShuffleJoinTable table : plan.shuffleJoinTables()) {
            List<ShuffleTarget> targets = targets(table, fragmentTargets);
            List<CompletableFuture<ShuffleTableResponse>> calls = new ArrayList<>();
            for (Map.Entry<WorkerRegistry.WorkerRecord, List<Integer>> source : sources(table).entrySet()) {
                ShuffleTableRequest request = ShuffleTableRequest.newBuilder()
                        .setQueryId(queryId)
                        .setTableName(table.tableName())
                        .addAllSourceShardIds(source.getValue())
                        .setJoinColumn(table.joinColumn())
                        .setJoinKeyType(table.joinKeyType())
                        .setBucketCount(table.bucketCount())
                        .addAllTargets(targets)
                        .build();
                calls.add(CompletableFuture.supplyAsync(
                        () -> workerClient.shuffleTable(source.getKey(), request), executor));
            }
            for (CompletableFuture<ShuffleTableResponse> call : calls) {
                ShuffleTableResponse response;
                try {
                    response = call.join();
                } catch (CompletionException e) {
                    throw e.getCause() instanceof RuntimeException runtime ? runtime : e;
                }
                rows += response.getRowsShuffled();
                bytes += response.getBytesSent();
                pieces += response.getPiecesSent();
            }
        }
        ShuffleStats stats = new ShuffleStats(rows, bytes, pieces, System.currentTimeMillis() - start);
        LOG.info("Query [{}] shuffle complete: {} rows, {} bytes in {} pieces ({}ms)",
                queryId, stats.rows(), stats.bytes(), stats.pieces(), stats.durationMs());
        return stats;
    }

    private List<ShuffleTarget> targets(ShuffleJoinTable table, Map<Integer, String> fragmentTargets) {
        List<ShuffleTarget> targets = new ArrayList<>(table.bucketCount());
        for (int bucket = 0; bucket < table.bucketCount(); bucket++) {
            String workerId = fragmentTargets.get(bucket);
            WorkerRegistry.WorkerRecord worker = registry.getWorker(workerId)
                    .orElseThrow(() -> new IllegalStateException("Shuffle target worker " + workerId + " not available"));
            targets.add(ShuffleTarget.newBuilder()
                    .setBucket(bucket)
                    .setWorkerId(worker.workerId())
                    .setHost(worker.host())
                    .setPort(worker.port())
                    .build());
        }
        return targets;
    }

    /** Assigns each source shard to a worker that holds it, spreading shards across holders. */
    private Map<WorkerRegistry.WorkerRecord, List<Integer>> sources(ShuffleJoinTable table) {
        Map<String, List<Integer>> shardsByWorker = new LinkedHashMap<>();
        Map<String, Integer> load = new HashMap<>();
        for (int shardId = 0; shardId < table.sourceShardCount(); shardId++) {
            List<String> holders = new ArrayList<>();
            for (String ownerId : shardCatalog.getActualOwners(table.tableName(), shardId)) {
                if (registry.isSchedulable(ownerId)) {
                    holders.add(ownerId);
                }
            }
            if (holders.isEmpty()) {
                throw new IllegalStateException(
                        "No worker holds " + table.tableName() + "_shard" + shardId + " for shuffle");
            }
            String chosen = holders.stream()
                    .min((left, right) -> Integer.compare(load.getOrDefault(left, 0), load.getOrDefault(right, 0)))
                    .orElseThrow();
            load.merge(chosen, 1, Integer::sum);
            shardsByWorker.computeIfAbsent(chosen, ignored -> new ArrayList<>()).add(shardId);
        }
        Map<WorkerRegistry.WorkerRecord, List<Integer>> sources = new LinkedHashMap<>();
        shardsByWorker.forEach((workerId, shards) -> sources.put(
                registry.getWorker(workerId).orElseThrow(
                        () -> new IllegalStateException("Shuffle source worker " + workerId + " not available")),
                shards));
        return sources;
    }
}
