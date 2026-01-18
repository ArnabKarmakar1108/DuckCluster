package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.PlannedQuery;

public interface QueryPlanner {
    PlannedQuery plan(String sql, ClusterCatalog catalog);
}
