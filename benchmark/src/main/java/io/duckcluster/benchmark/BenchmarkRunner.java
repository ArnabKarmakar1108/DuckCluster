package io.duckcluster.benchmark;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public final class BenchmarkRunner {

    private final BenchmarkConfig config;
    private final QueryCatalog catalog;

    public BenchmarkRunner(BenchmarkConfig config, QueryCatalog catalog) {
        this.config = config;
        this.catalog = catalog;
    }

    public Map<String, Object> run() throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();

        if (config.verbose()) {
            System.out.printf(
                    "Benchmark run_id=%s sf=%s warmup=%d iterations=%d engines=%s concurrency=%s%n",
                    config.runId(),
                    config.scaleFactor(),
                    config.warmupIterations(),
                    config.measuredIterations(),
                    config.engines(),
                    config.concurrencyLevels());
        }

        for (Engine engine : config.engines()) {
            for (int concurrency : config.concurrencyLevels()) {
                if (config.verbose()) {
                    System.out.printf("%n=== engine=%s concurrency=%d ===%n", engine.id(), concurrency);
                }
                results.addAll(runMatrix(engine, concurrency));
            }
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("run_id", config.runId());
        payload.put("scale_factor", config.scaleFactor());
        payload.put("warmup_iterations", config.warmupIterations());
        payload.put("measured_iterations", config.measuredIterations());
        payload.put("unsupported_queries", catalog.unsupportedSummary());
        payload.put("results", results);
        payload.put("summary", Map.of(
                "status", "TODO",
                "note", "Crossover analysis pending — see docs/BENCHMARK.md"));
        return payload;
    }

    private List<Map<String, Object>> runMatrix(Engine engine, int concurrency) throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();

        for (QueryCatalog.QuerySpec unsupported : catalog.all()) {
            if (!unsupported.supported()) {
                results.add(statusResult(unsupported.id(), engine, concurrency, "UNSUPPORTED",
                        "correlated subquery not supported by planner"));
            }
        }

        List<QueryCatalog.QuerySpec> supported = catalog.supported();
        if (supported.isEmpty()) {
            return results;
        }

        warmUp(engine, supported);
        if (config.verbose()) {
            System.out.printf("  warmup done (%d x %d queries)%n", config.warmupIterations(), supported.size());
        }
        results.addAll(measureConcurrent(engine, concurrency, supported));
        return results;
    }

    private void warmUp(Engine engine, List<QueryCatalog.QuerySpec> queries) throws Exception {
        try (AutoCloseable client = openClient(engine)) {
            int total = config.warmupIterations() * queries.size();
            int done = 0;
            for (int i = 0; i < config.warmupIterations(); i++) {
                for (QueryCatalog.QuerySpec query : queries) {
                    try {
                        execute(client, query.sql());
                    } catch (Exception ignored) {
                        // Warmup failures are non-fatal; measured phase records ERROR entries.
                    }
                    done++;
                    if (config.verbose() && (done == 1 || done % 10 == 0 || done == total)) {
                        System.out.printf(
                                "  warmup [%d/%d] %s %s%n", done, total, engine.id(), query.id());
                    }
                }
            }
        }
    }

    private List<Map<String, Object>> measureConcurrent(
            Engine engine,
            int concurrency,
            List<QueryCatalog.QuerySpec> queries) throws Exception {

        Map<String, List<Long>> latenciesByQuery = new LinkedHashMap<>();
        Map<String, List<Long>> executionMsByQuery = new LinkedHashMap<>();
        Map<String, String> errorsByQuery = new LinkedHashMap<>();
        Map<String, DuckClusterClient.QueryExecution> lastExecutionByQuery = new LinkedHashMap<>();
        for (QueryCatalog.QuerySpec query : queries) {
            latenciesByQuery.put(query.id(), new ArrayList<>());
            executionMsByQuery.put(query.id(), new ArrayList<>());
        }

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        try {
            long start = System.nanoTime();
            List<Future<Void>> futures = new ArrayList<>();
            AtomicInteger completedQueries = new AtomicInteger();

            for (int clientId = 0; clientId < concurrency; clientId++) {
                futures.add(executor.submit(runClientLoop(
                        engine,
                        queries,
                        latenciesByQuery,
                        executionMsByQuery,
                        errorsByQuery,
                        lastExecutionByQuery,
                        completedQueries)));
            }

            for (Future<Void> future : futures) {
                future.get();
            }
            long totalDurationMs = (System.nanoTime() - start) / 1_000_000L;

            List<Map<String, Object>> results = new ArrayList<>();
            for (QueryCatalog.QuerySpec query : queries) {
                List<Long> latencies = latenciesByQuery.get(query.id());
                List<Long> executionSamples = executionMsByQuery.get(query.id());
                String error = errorsByQuery.get(query.id());
                if (error != null && latencies.isEmpty()) {
                    results.add(statusResult(query.id(), engine, concurrency, "ERROR", error));
                    if (config.verbose()) {
                        System.out.printf("  %s %-12s ERROR %s%n", engine.id(), query.id(), truncate(error, 80));
                    }
                    continue;
                }

                LatencyStats.Percentiles clientLatency = LatencyStats.fromMillis(latencies);
                LatencyStats.Percentiles executionMs = LatencyStats.fromMillis(executionSamples);
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("query", query.id());
                entry.put("concurrency", concurrency);
                entry.put("engine", engine.id());
                entry.put("status", error == null ? "OK" : "PARTIAL");
                entry.put("samples", latencies.size());
                entry.put("execution_ms", Map.of(
                        "p50", executionMs.p50(),
                        "p95", executionMs.p95(),
                        "p99", executionMs.p99()));
                entry.put("client_latency_ms", Map.of(
                        "p50", clientLatency.p50(),
                        "p95", clientLatency.p95(),
                        "p99", clientLatency.p99()));
                entry.put("latency_ms", entry.get("execution_ms"));
                entry.put("throughput_qps", LatencyStats.throughputQps(latencies, totalDurationMs));
                entry.put("correct", "TODO");
                if (error != null) {
                    entry.put("error", error);
                }

                DuckClusterClient.QueryExecution last = lastExecutionByQuery.get(query.id());
                if (last != null && !last.detail().isEmpty()) {
                    entry.put("detail", last.detail());
                }
                results.add(entry);
                if (config.verbose()) {
                    System.out.printf(
                            "  %s %-12s %s exec_p50=%dms client_p50=%dms samples=%d%n",
                            engine.id(),
                            query.id(),
                            error == null ? "OK" : "PARTIAL",
                            executionMs.p50(),
                            clientLatency.p50(),
                            latencies.size());
                }
            }
            return results;
        } finally {
            executor.shutdownNow();
        }
    }

    private Callable<Void> runClientLoop(
            Engine engine,
            List<QueryCatalog.QuerySpec> queries,
            Map<String, List<Long>> latenciesByQuery,
            Map<String, List<Long>> executionMsByQuery,
            Map<String, String> errorsByQuery,
            Map<String, DuckClusterClient.QueryExecution> lastExecutionByQuery,
            AtomicInteger completedQueries) {

        return () -> {
            try (AutoCloseable client = openClient(engine)) {
                for (int iteration = 0; iteration < config.measuredIterations(); iteration++) {
                    for (QueryCatalog.QuerySpec query : queries) {
                        try {
                            DuckClusterClient.QueryExecution execution = execute(client, query.sql());
                            synchronized (latenciesByQuery) {
                                latenciesByQuery.get(query.id()).add(execution.latencyMs());
                                executionMsByQuery.get(query.id()).add(execution.executionMs());
                                lastExecutionByQuery.put(query.id(), execution);
                                if (config.verbose()) {
                                    int done = completedQueries.incrementAndGet();
                                    int total = config.measuredIterations() * queries.size();
                                    System.out.printf(
                                            "  [%d/%d] %s %-12s exec=%dms client=%dms%n",
                                            done,
                                            total,
                                            engine.id(),
                                            query.id(),
                                            execution.executionMs(),
                                            execution.latencyMs());
                                }
                            }
                        } catch (Exception exception) {
                            synchronized (errorsByQuery) {
                                errorsByQuery.putIfAbsent(query.id(), exception.getMessage());
                            }
                            if (config.verbose()) {
                                synchronized (latenciesByQuery) {
                                    int done = completedQueries.incrementAndGet();
                                    int total = config.measuredIterations() * queries.size();
                                    System.out.printf(
                                            "  [%d/%d] %s %-12s ERROR %s%n",
                                            done,
                                            total,
                                            engine.id(),
                                            query.id(),
                                            truncate(exception.getMessage(), 80));
                                }
                            }
                        }
                    }
                }
            }
            return null;
        };
    }

    private DuckClusterClient.QueryExecution execute(AutoCloseable client, String sql) throws Exception {
        if (client instanceof DuckClusterClient duckClusterClient) {
            return duckClusterClient.execute(sql);
        }
        if (client instanceof SingleNodeClient singleNodeClient) {
            return singleNodeClient.execute(sql);
        }
        throw new IllegalStateException("Unknown client type: " + client.getClass());
    }

    private AutoCloseable openClient(Engine engine) throws Exception {
        return switch (engine) {
            case DUCKCLUSTER -> new DuckClusterClient(config.coordinatorUrl());
            case DUCKDB_SINGLE -> new SingleNodeClient(config.duckdbPath());
        };
    }

    private static Map<String, Object> statusResult(
            String queryId, Engine engine, int concurrency, String status, String reason) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("query", queryId);
        entry.put("concurrency", concurrency);
        entry.put("engine", engine.id());
        entry.put("status", status);
        entry.put("reason", reason);
        return entry;
    }

    private static String truncate(String text, int maxLen) {
        if (text == null || text.length() <= maxLen) {
            return text;
        }
        return text.substring(0, maxLen - 3) + "...";
    }
}
