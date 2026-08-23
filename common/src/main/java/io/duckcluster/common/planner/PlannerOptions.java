package io.duckcluster.common.planner;

/**
 * Join placement switches. Broadcast is always available; co-partitioned and shuffle joins are used
 * only when declared shard keys prove they are correct.
 *
 * @param shuffleMinRows a table smaller than this (by catalog row estimate) is broadcast instead of shuffled
 */
public record PlannerOptions(boolean coPartitionJoins, boolean shuffleJoins, long shuffleMinRows) {
    public static final PlannerOptions DEFAULT = new PlannerOptions(true, true, 0L);

    public static PlannerOptions fromEnvironment() {
        return new PlannerOptions(
                envBoolean("DUCKCLUSTER_COPARTITION_JOINS", DEFAULT.coPartitionJoins()),
                envBoolean("DUCKCLUSTER_SHUFFLE_JOINS", DEFAULT.shuffleJoins()),
                envLong("DUCKCLUSTER_SHUFFLE_MIN_ROWS", DEFAULT.shuffleMinRows()));
    }

    private static boolean envBoolean(String name, boolean fallback) {
        String raw = System.getenv(name);
        return raw == null || raw.isBlank() ? fallback : Boolean.parseBoolean(raw.trim());
    }

    private static long envLong(String name, long fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
