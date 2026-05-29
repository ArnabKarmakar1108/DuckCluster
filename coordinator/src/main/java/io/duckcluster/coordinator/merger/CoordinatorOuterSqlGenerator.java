package io.duckcluster.coordinator.merger;

import io.duckcluster.common.model.NestedDerivedTableSpec;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.TopKSpec;

final class CoordinatorOuterSqlGenerator {
    private static final String TEMP_TABLE = "__merge_temp";

    private CoordinatorOuterSqlGenerator() {}

    static String generate(NestedDerivedTableSpec nested) {
        var analysis = nested.outerAnalysis();
        StringBuilder sql = new StringBuilder("SELECT ");
        boolean first = true;
        for (String outputColumn : analysis.outputColumnNames()) {
            if (!first) {
                sql.append(", ");
            }
            if (analysis.groupByColumns().contains(outputColumn)) {
                sql.append(quote(outputColumn));
            } else {
                sql.append(aggregateExpression(analysis, outputColumn));
                sql.append(" AS ").append(quote(outputColumn));
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
        appendOrderByAndLimit(sql, nested.outerTopK());
        return sql.toString();
    }

    private static String aggregateExpression(io.duckcluster.common.model.QueryAnalysis analysis, String outputColumn) {
        for (var aggregate : analysis.aggregates()) {
            if (aggregate.outputName().equals(outputColumn)) {
                String operand = aggregate.inputColumn() == null ? "*" : quote(aggregate.inputColumn());
                return switch (aggregate.function()) {
                    case COUNT -> operand.equals("*") ? "COUNT(*)" : "COUNT(" + operand + ")";
                    case SUM -> "SUM(" + operand + ")";
                    case MIN -> "MIN(" + operand + ")";
                    case MAX -> "MAX(" + operand + ")";
                    case AVG -> "AVG(" + operand + ")";
                };
            }
        }
        throw new IllegalStateException("Missing aggregate metadata for output column: " + outputColumn);
    }

    private static void appendOrderByAndLimit(StringBuilder sql, TopKSpec topK) {
        if (topK.hasOrderBy()) {
            sql.append(" ORDER BY ");
            for (int i = 0; i < topK.orderBy().size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                OrderByClause clause = topK.orderBy().get(i);
                sql.append(quote(clause.column()));
                if (clause.descending()) {
                    sql.append(" DESC");
                }
            }
        }
        if (topK.hasLimit()) {
            sql.append(" LIMIT ").append(topK.limit());
        }
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
