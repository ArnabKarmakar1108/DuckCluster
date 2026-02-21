package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.QueryAnalysis;

import java.util.List;

public final class MergeSqlBuilder {
    private static final String TEMP_TABLE = "__merge_temp";

    private MergeSqlBuilder() {}

    public static String buildGroupByMerge(QueryAnalysis analysis) {
        StringBuilder sql = new StringBuilder("SELECT ");
        boolean first = true;
        for (String groupColumn : analysis.groupByColumns()) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(quote(groupColumn));
            first = false;
        }
        for (AggregateSpec aggregate : analysis.aggregates()) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(mergeExpression(aggregate))
                    .append(" AS ")
                    .append(quote(aggregate.outputName()));
            first = false;
        }
        sql.append(" FROM ").append(TEMP_TABLE);
        if (!analysis.groupByColumns().isEmpty()) {
            sql.append(" GROUP BY ");
            for (int i = 0; i < analysis.groupByColumns().size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(quote(analysis.groupByColumns().get(i)));
            }
        }
        return sql.toString();
    }

    public static String buildPartialAggMerge(QueryAnalysis analysis) {
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < analysis.aggregates().size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            AggregateSpec aggregate = analysis.aggregates().get(i);
            sql.append(mergeExpression(aggregate)).append(" AS ").append(quote(aggregate.outputName()));
        }
        return sql.append(" FROM ").append(TEMP_TABLE).toString();
    }

    public static List<String> tempTableColumns(QueryAnalysis analysis) {
        java.util.ArrayList<String> columns = new java.util.ArrayList<>();
        columns.addAll(analysis.groupByColumns());
        for (AggregateSpec aggregate : analysis.aggregates()) {
            columns.add(aggregate.mergeColumnName());
        }
        return columns;
    }

    private static String mergeExpression(AggregateSpec aggregate) {
        String column = quote(aggregate.mergeColumnName());
        return switch (aggregate.function()) {
            case COUNT, SUM -> "SUM(" + column + ")";
            case MIN -> "MIN(" + column + ")";
            case MAX -> "MAX(" + column + ")";
            case AVG -> "AVG(" + column + ")";
        };
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
