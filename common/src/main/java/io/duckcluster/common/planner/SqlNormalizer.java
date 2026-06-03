package io.duckcluster.common.planner;

import java.util.List;

/** Normalizes SQL text before Calcite parsing. */
final class SqlNormalizer {
    private static final List<String> RESERVED_ALIASES = List.of("value");

    private SqlNormalizer() {}

    static String normalize(String sql) {
        String normalized = sql;
        for (String alias : RESERVED_ALIASES) {
            normalized = normalized.replaceAll("(?i)(\\bAS\\s+)" + alias + "\\b", "$1\"" + alias + "\"");
            normalized = normalized.replaceAll("(?i)(\\bORDER BY\\s+)" + alias + "\\b", "$1\"" + alias + "\"");
        }
        return normalized;
    }
}
