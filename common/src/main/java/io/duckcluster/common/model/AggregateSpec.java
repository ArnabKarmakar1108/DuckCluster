package io.duckcluster.common.model;

import org.apache.calcite.sql.SqlNode;

public record AggregateSpec(
        String outputName,
        String mergeColumnName,
        AggregateFunction function,
        String inputColumn,
        SqlNode inputExpression,
        AggregatePart part) {

    public AggregateSpec(
            String outputName,
            String mergeColumnName,
            AggregateFunction function,
            String inputColumn) {
        this(outputName, mergeColumnName, function, inputColumn, null, AggregatePart.WHOLE);
    }

    public AggregateSpec(
            String outputName,
            String mergeColumnName,
            AggregateFunction function,
            String inputColumn,
            SqlNode inputExpression) {
        this(outputName, mergeColumnName, function, inputColumn, inputExpression, AggregatePart.WHOLE);
    }

    public AggregateSpec(
            String outputName,
            String mergeColumnName,
            AggregateFunction function,
            String inputColumn,
            AggregatePart part) {
        this(outputName, mergeColumnName, function, inputColumn, null, part);
    }

    public boolean hasInputExpression() {
        return inputExpression != null;
    }

    public enum AggregatePart {
        WHOLE,
        AVG_SUM,
        AVG_COUNT
    }
}
