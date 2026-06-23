package io.duckcluster.benchmark;

import java.util.Arrays;
import java.util.List;

public final class LatencyStats {

    private LatencyStats() {}

    public static Percentiles fromMillis(List<Long> samples) {
        if (samples.isEmpty()) {
            return new Percentiles(0, 0, 0);
        }
        long[] values = samples.stream().mapToLong(Long::longValue).sorted().toArray();
        return new Percentiles(
                percentile(values, 0.50),
                percentile(values, 0.95),
                percentile(values, 0.99));
    }

    public static double throughputQps(List<Long> samples, long totalDurationMs) {
        if (totalDurationMs <= 0 || samples.isEmpty()) {
            return 0.0;
        }
        return samples.size() * 1000.0 / totalDurationMs;
    }

    private static long percentile(long[] sorted, double quantile) {
        if (sorted.length == 1) {
            return sorted[0];
        }
        double rank = quantile * (sorted.length - 1);
        int lower = (int) Math.floor(rank);
        int upper = (int) Math.ceil(rank);
        if (lower == upper) {
            return sorted[lower];
        }
        double weight = rank - lower;
        return Math.round(sorted[lower] * (1.0 - weight) + sorted[upper] * weight);
    }

    public record Percentiles(long p50, long p95, long p99) {
        public List<Long> asList() {
            return List.of(p50, p95, p99);
        }
    }
}
