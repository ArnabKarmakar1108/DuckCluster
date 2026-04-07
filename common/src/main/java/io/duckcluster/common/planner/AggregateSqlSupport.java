package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

public final class AggregateSqlSupport {
    private AggregateSqlSupport() {}

    public static boolean isAggregateExpression(SqlNode node) {
        return unwrapAlias(node) instanceof SqlCall call && isAggregateCall(call);
    }

    public static boolean isAggregateCall(SqlCall call) {
        if (call.getKind().belongsTo(SqlKind.AGGREGATE)) {
            return true;
        }
        return isKnownAggregateName(call.getOperator().getName());
    }

    public static AggregateFunction toAggregateFunction(SqlCall call) {
        if (call.getKind() == SqlKind.COUNT || "COUNT".equalsIgnoreCase(call.getOperator().getName())) {
            return AggregateFunction.COUNT;
        }
        if (call.getKind() == SqlKind.SUM || "SUM".equalsIgnoreCase(call.getOperator().getName())) {
            return AggregateFunction.SUM;
        }
        if (call.getKind() == SqlKind.MIN || "MIN".equalsIgnoreCase(call.getOperator().getName())) {
            return AggregateFunction.MIN;
        }
        if (call.getKind() == SqlKind.MAX || "MAX".equalsIgnoreCase(call.getOperator().getName())) {
            return AggregateFunction.MAX;
        }
        if (call.getKind() == SqlKind.AVG || "AVG".equalsIgnoreCase(call.getOperator().getName())) {
            return AggregateFunction.AVG;
        }
        throw new IllegalArgumentException("Unsupported aggregate: " + call);
    }

    private static boolean isKnownAggregateName(String name) {
        return switch (name.toUpperCase()) {
            case "COUNT", "SUM", "MIN", "MAX", "AVG" -> true;
            default -> false;
        };
    }

    static SqlNode unwrapAlias(SqlNode item) {
        if (item instanceof SqlCall call && call.getOperator().getKind() == SqlKind.AS) {
            return call.operand(0);
        }
        return item;
    }
}
