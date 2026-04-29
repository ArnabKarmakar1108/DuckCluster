package io.duckcluster.common.model;

import java.util.List;

public record TopKSpec(List<OrderByClause> orderBy, int limit) {

    public static TopKSpec none() {
        return new TopKSpec(List.of(), -1);
    }

    public boolean hasOrderBy() {
        return !orderBy.isEmpty();
    }

    public boolean hasLimit() {
        return limit > 0;
    }

    public boolean hasTopK() {
        return hasOrderBy() || hasLimit();
    }
}
