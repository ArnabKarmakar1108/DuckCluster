package io.duckcluster.common.model;

public record AggregateSpec(
        String outputName,
        String mergeColumnName,
        AggregateFunction function,
        String inputColumn,
        AggregatePart part) {

    public AggregateSpec(String outputName, String mergeColumnName, AggregateFunction function, String inputColumn) {
        this(outputName, mergeColumnName, function, inputColumn, AggregatePart.WHOLE);
    }

    public enum AggregatePart {
        WHOLE,
        AVG_SUM,
        AVG_COUNT
    }
}
