package io.duckcluster.common.planner;

public final class ShardPredicateInjector {
    private ShardPredicateInjector() {}

    public static String inject(String sql, String shardKey, int numShards, int shardId) {
        String trimmed = sql.strip();
        boolean hasSemicolon = trimmed.endsWith(";");
        if (hasSemicolon) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).strip();
        }

        String predicate = String.format("(%s %% %d) = %d", shardKey, numShards, shardId);
        String upper = trimmed.toUpperCase();
        String rewritten;
        if (upper.contains(" WHERE ")) {
            rewritten = trimmed + " AND " + predicate;
        } else {
            rewritten = insertBeforeClause(trimmed, upper, predicate);
        }

        return hasSemicolon ? rewritten + ";" : rewritten;
    }

    private static String insertBeforeClause(String sql, String upper, String predicate) {
        String[] terminators = {" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT ", " OFFSET ", " FETCH "};
        int insertAt = sql.length();
        for (String terminator : terminators) {
            int index = upper.indexOf(terminator);
            if (index >= 0 && index < insertAt) {
                insertAt = index;
            }
        }
        return sql.substring(0, insertAt) + " WHERE " + predicate + sql.substring(insertAt);
    }
}
