package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.ComputedOutputSpec;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TopKSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;

/** Resolves ORDER BY columns against SELECT aliases and merge metadata. */
final class TopKResolver {
    private TopKResolver() {}

    static TopKSpec resolve(TopKSpec topK, QueryAnalysis analysis) {
        if (!topK.hasOrderBy()) {
            return topK;
        }
        List<OrderByClause> resolved = new ArrayList<>(topK.orderBy().size());
        for (OrderByClause clause : topK.orderBy()) {
            resolved.add(new OrderByClause(resolveColumn(clause.column(), analysis), clause.descending()));
        }
        return new TopKSpec(resolved, topK.limit());
    }

    static TopKSpec extractAndResolve(SqlNode parsed, SqlSelect select, QueryAnalysis analysis) {
        TopKSpec raw = TopKExtractor.extract(parsed, select);
        return resolve(raw, analysis);
    }

    static String mergeOrderExpression(OrderByClause clause, QueryAnalysis analysis) {
        String column = clause.column();
        for (AggregateSpec aggregate : analysis.aggregates()) {
            if (aggregate.part() == AggregateSpec.AggregatePart.AVG_COUNT) {
                continue;
            }
            if (aggregate.outputName().equals(column)) {
                return mergeExpression(aggregate, analysis);
            }
        }
        for (ComputedOutputSpec computed : analysis.computedOutputs()) {
            if (computed.outputName().equals(column)) {
                return computed.mergeExpressionSql();
            }
        }
        return quote(column);
    }

    private static String resolveColumn(String orderColumn, QueryAnalysis analysis) {
        if (analysis.outputColumnNames().contains(orderColumn)) {
            return orderColumn;
        }
        if (analysis.groupByColumns().contains(orderColumn)) {
            return orderColumn;
        }
        for (AggregateSpec aggregate : analysis.aggregates()) {
            if (aggregate.outputName().equals(orderColumn)) {
                return orderColumn;
            }
        }
        for (ComputedOutputSpec computed : analysis.computedOutputs()) {
            if (computed.outputName().equals(orderColumn)) {
                return orderColumn;
            }
        }
        return orderColumn;
    }

    static String resolveFromSelectList(String orderColumn, SqlNodeList selectList) {
        for (SqlNode item : selectList) {
            if (item instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
                SqlNode aliasNode = call.operand(1);
                if (aliasNode instanceof SqlIdentifier alias && alias.getSimple().equalsIgnoreCase(orderColumn)) {
                    return alias.getSimple();
                }
            }
        }
        return orderColumn;
    }

    private static String mergeExpression(AggregateSpec aggregate, QueryAnalysis analysis) {
        if (aggregate.part() == AggregateSpec.AggregatePart.AVG_SUM) {
            return avgMergeExpression(aggregate, nextAvgCount(analysis, aggregate));
        }
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

    private static String avgMergeExpression(AggregateSpec sumPart, AggregateSpec countPart) {
        return "CAST(SUM(" + quote(sumPart.mergeColumnName()) + ") AS DOUBLE) / NULLIF(SUM("
                + quote(countPart.mergeColumnName()) + "), 0)";
    }

    private static AggregateSpec nextAvgCount(QueryAnalysis analysis, AggregateSpec sumPart) {
        int index = analysis.aggregates().indexOf(sumPart);
        AggregateSpec countPart = analysis.aggregates().get(index + 1);
        if (countPart.part() != AggregateSpec.AggregatePart.AVG_COUNT) {
            throw new IllegalStateException("Missing AVG count companion for " + sumPart.outputName());
        }
        return countPart;
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
