package io.duckcluster.benchmark;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public record BenchmarkConfig(
        double scaleFactor,
        List<Integer> concurrencyLevels,
        List<Engine> engines,
        String coordinatorUrl,
        Path duckdbPath,
        Path queriesDir,
        Path outputPath,
        int warmupIterations,
        int measuredIterations,
        String runId,
        boolean verbose) {

    public static BenchmarkConfig parse(String[] args) {
        double scaleFactor = 1;
        List<Integer> concurrency = List.of(1);
        List<Engine> engines = List.of(Engine.DUCKCLUSTER, Engine.DUCKDB_SINGLE);
        String coordinatorUrl = "http://127.0.0.1:8080";
        Path duckdbPath = null;
        Path queriesDir = null;
        Path outputPath = Path.of("benchmark-results.json");
        int warmup = 5;
        int measured = 15;
        String runId = null;
        boolean verbose = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--scale-factor" -> scaleFactor = Double.parseDouble(requireValue(args, ++i, "--scale-factor"));
                case "--concurrency" -> concurrency = parseIntList(requireValue(args, ++i, "--concurrency"));
                case "--engine" -> engines = parseEngineList(requireValue(args, ++i, "--engine"));
                case "--coordinator-url" -> coordinatorUrl = requireValue(args, ++i, "--coordinator-url");
                case "--duckdb-path" -> duckdbPath = Path.of(requireValue(args, ++i, "--duckdb-path"));
                case "--queries-dir" -> queriesDir = Path.of(requireValue(args, ++i, "--queries-dir"));
                case "--output" -> outputPath = Path.of(requireValue(args, ++i, "--output"));
                case "--warmup" -> warmup = Integer.parseInt(requireValue(args, ++i, "--warmup"));
                case "--iterations" -> measured = Integer.parseInt(requireValue(args, ++i, "--iterations"));
                case "--run-id" -> runId = requireValue(args, ++i, "--run-id");
                case "--verbose", "-v" -> verbose = true;
                case "--help", "-h" -> {
                    printUsage();
                    System.exit(0);
                }
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }

        if (duckdbPath == null) {
            throw new IllegalArgumentException("--duckdb-path is required");
        }
        if (queriesDir == null) {
            queriesDir = Path.of("benchmark/src/main/resources/queries");
        }
        if (runId == null) {
            runId = "sf" + scaleFactor + "-" + String.join(",", engines.stream().map(Engine::id).toList());
        }

        return new BenchmarkConfig(
                scaleFactor,
                concurrency,
                engines,
                coordinatorUrl,
                duckdbPath,
                queriesDir,
                outputPath,
                warmup,
                measured,
                runId,
                verbose);
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + flag);
        }
        return args[index];
    }

    private static List<Integer> parseIntList(String value) {
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(part -> !part.isEmpty())
                .map(Integer::parseInt)
                .toList();
    }

    private static List<Engine> parseEngineList(String value) {
        if ("both".equalsIgnoreCase(value)) {
            return List.of(Engine.DUCKCLUSTER, Engine.DUCKDB_SINGLE);
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(part -> !part.isEmpty())
                .map(Engine::fromId)
                .toList();
    }

    private static void printUsage() {
        System.out.println("""
                Usage: benchmark-harness [options]

                  --scale-factor <N>         TPC-H scale factor (default: 1)
                  --concurrency <list>       Comma-separated client counts (default: 1)
                  --engine <list|both>       duckcluster, duckdb-single, or both
                  --coordinator-url <url>    DuckCluster HTTP base URL
                  --duckdb-path <path>       Single-node DuckDB database path
                  --queries-dir <path>       Directory with Q01.sql..Q22.sql
                  --output <path>            Results JSON path
                  --warmup <N>               Warmup iterations per query (default: 5)
                  --iterations <N>           Measured iterations per query (default: 15)
                  --run-id <id>              Optional run identifier
                  --verbose, -v              Print per-query progress to stdout
                """);
    }
}
