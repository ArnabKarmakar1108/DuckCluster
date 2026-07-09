package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.ComputedOutputSpec;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TopKSpec;

import java.util.List;

public final class MergeSqlBuilder {
    private static final String TEMP_TABLE = "__merge_temp";

    private MergeSqlBuilder() {}

    public static String buildGroupByMerge(QueryAnalysis analysis) {
        return buildGroupByMerge(analysis, TopKSpec.none());
    }

    public static String buildGroupByMerge(QueryAnalysis analysis, TopKSpec topK) {
        return buildGroupByMerge(analysis, topK, null);
    }

    /** Intermediate hierarchical merge — keeps merge column names for another merge pass. */
    public static String buildGroupByIntermediateMerge(QueryAnalysis analysis) {
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
            sql.append(", ");
            sql.append(mergeExpression(aggregate));
            sql.append(" AS ").append(quote(aggregate.mergeColumnName()));
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

    public static String buildGroupByMerge(QueryAnalysis analysis, TopKSpec topK, List<String> outputAliases) {
        StringBuilder sql = new StringBuilder("SELECT ");
        boolean first = true;
        for (int outputIndex = 0; outputIndex < analysis.outputColumnNames().size(); outputIndex++) {
            String outputColumn = analysis.outputColumnNames().get(outputIndex);
            if (!first) {
                sql.append(", ");
            }
            if (analysis.groupByColumns().contains(outputColumn)) {
                sql.append(quote(outputColumn));
            } else if (computedOutputForColumn(analysis, outputColumn) != null) {
                sql.append(computedOutputForColumn(analysis, outputColumn).mergeExpressionSql());
                sql.append(" AS ").append(quote(outputColumn));
            } else {
                AggregateSpec aggregate = aggregateForOutput(analysis, outputColumn);
                if (aggregate.part() == AggregateSpec.AggregatePart.AVG_SUM) {
                    sql.append(avgMergeExpression(aggregate, nextAvgCount(analysis, aggregate)));
                } else {
                    sql.append(mergeExpression(aggregate));
                }
                sql.append(" AS ").append(quote(resolveOutputAlias(outputAliases, outputIndex, outputColumn)));
            }
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
        appendOrderByAndLimit(sql, topK, analysis, false);
        return sql.toString();
    }

    private static ComputedOutputSpec computedOutputForColumn(QueryAnalysis analysis, String outputColumn) {
        for (ComputedOutputSpec computed : analysis.computedOutputs()) {
            if (computed.outputName().equals(outputColumn)) {
                return computed;
            }
        }
        return null;
    }

    private static AggregateSpec aggregateForOutput(QueryAnalysis analysis, String outputColumn) {
        for (AggregateSpec aggregate : analysis.aggregates()) {
            if (aggregate.part() == AggregateSpec.AggregatePart.AVG_COUNT) {
                continue;
            }
            if (aggregate.outputName().equals(outputColumn)) {
                return aggregate;
            }
        }
        throw new IllegalStateException("Missing aggregate metadata for output column: " + outputColumn);
    }

    public static String buildPartialAggMerge(QueryAnalysis analysis) {
        if (analysis.hasComputedOutputs()) {
            StringBuilder sql = new StringBuilder("SELECT ");
            boolean first = true;
            for (ComputedOutputSpec computed : analysis.computedOutputs()) {
                if (!first) {
                    sql.append(", ");
                }
                sql.append(computed.mergeExpressionSql());
                sql.append(" AS ").append(quote(computed.outputName()));
                first = false;
            }
            return sql.append(" FROM ").append(TEMP_TABLE).toString();
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        boolean first = true;
        for (AggregateSpec aggregate : analysis.aggregates()) {
            if (aggregate.part() == AggregateSpec.AggregatePart.AVG_COUNT) {
                continue;
            }
            if (!first) {
                sql.append(", ");
            }
            if (aggregate.part() == AggregateSpec.AggregatePart.AVG_SUM) {
                sql.append(avgMergeExpression(aggregate, nextAvgCount(analysis, aggregate)));
            } else {
                sql.append(mergeExpression(aggregate));
            }
            sql.append(" AS ").append(quote(aggregate.outputName()));
            first = false;
        }
        return sql.append(" FROM ").append(TEMP_TABLE).toString();
    }

    public static String buildTopKMerge(QueryAnalysis analysis, TopKSpec topK) {
        return buildTopKMerge(analysis, topK, analysis.outputColumnNames());
    }

    public static String buildTopKMerge(QueryAnalysis analysis, TopKSpec topK, List<String> outputColumns) {
        StringBuilder sql = new StringBuilder("SELECT ");
        boolean first = true;
        for (String column : outputColumns) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(quote(column));
            first = false;
        }
        sql.append(" FROM ").append(TEMP_TABLE);
        appendOrderByAndLimit(sql, topK, analysis, false);
        return sql.toString();
    }

    private static void appendOrderByAndLimit(StringBuilder sql, TopKSpec topK, QueryAnalysis analysis, boolean unused) {
        if (topK.hasOrderBy()) {
            sql.append(" ORDER BY ");
            for (int i = 0; i < topK.orderBy().size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                OrderByClause clause = topK.orderBy().get(i);
                if (analysis != null && hasAggregatesOrComputed(analysis)) {
                    sql.append(TopKResolver.mergeOrderExpression(clause, analysis));
                } else {
                    sql.append(quote(clause.column()));
                }
                if (clause.descending()) {
                    sql.append(" DESC");
                }
            }
        }
        if (topK.hasLimit()) {
            sql.append(" LIMIT ").append(topK.limit());
        }
    }

    private static boolean hasAggregatesOrComputed(QueryAnalysis analysis) {
        return !analysis.aggregates().isEmpty() || !analysis.computedOutputs().isEmpty();
    }

    public static List<String> tempTableColumns(QueryAnalysis analysis) {
        if (!analysis.aggregates().isEmpty() || !analysis.groupByColumns().isEmpty()) {
            java.util.ArrayList<String> columns = new java.util.ArrayList<>();
            columns.addAll(analysis.groupByColumns());
            for (AggregateSpec aggregate : analysis.aggregates()) {
                columns.add(aggregate.mergeColumnName());
            }
            return columns;
        }
        return new java.util.ArrayList<>(analysis.outputColumnNames());
    }

    private static String mergeExpression(AggregateSpec aggregate) {
        String column = quote(aggregate.mergeColumnName());
        if (aggregate.part() == AggregateSpec.AggregatePart.DISTINCT_COUNT) {
            return "COUNT(DISTINCT " + column + ")";
        }
        return switch (aggregate.function()) {
            case COUNT, SUM -> "SUM(" + column + ")";
            case MIN -> "MIN(" + column + ")";
            case MAX -> "MAX(" + column + ")";
            case AVG -> throw new IllegalStateException("AVG must be decomposed before merge");
        };
    }

    private static AggregateSpec nextAvgCount(QueryAnalysis analysis, AggregateSpec sumPart) {
        int index = analysis.aggregates().indexOf(sumPart);
        if (index < 0 || index + 1 >= analysis.aggregates().size()) {
            throw new IllegalStateException("Missing AVG count companion for " + sumPart.outputName());
        }
        AggregateSpec countPart = analysis.aggregates().get(index + 1);
        if (countPart.part() != AggregateSpec.AggregatePart.AVG_COUNT) {
            throw new IllegalStateException("Missing AVG count companion for " + sumPart.outputName());
        }
        return countPart;
    }

    private static String avgMergeExpression(AggregateSpec sumPart, AggregateSpec countPart) {
        return "CAST(SUM(" + quote(sumPart.mergeColumnName()) + ") AS DOUBLE) / NULLIF(SUM("
                + quote(countPart.mergeColumnName()) + "), 0)";
    }

    private static String resolveOutputAlias(List<String> outputAliases, int outputIndex, String defaultName) {
        if (outputAliases != null && outputIndex < outputAliases.size()) {
            return outputAliases.get(outputIndex);
        }
        return defaultName;
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
