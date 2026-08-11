package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.PlannedQuery;

public interface QueryPlanner {
    default PlannedQuery plan(String sql, ClusterCatalog catalog) {
        return plan(sql, catalog, null);
    }

    PlannedQuery plan(String sql, ClusterCatalog catalog, String queryId);
}
