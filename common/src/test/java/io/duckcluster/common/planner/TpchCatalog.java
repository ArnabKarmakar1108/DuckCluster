package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.TableShardConfig;

import java.util.Map;

/** TPC-H table layout used in planner and integration tests (SF0.01, 6 lineitem/orders shards). */
public final class TpchCatalog {
    private TpchCatalog() {}

    public static ClusterCatalog create() {
        return ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 6, "l_orderkey"),
                "orders", new TableShardConfig("orders", 6, "o_orderkey"),
                "customer", new TableShardConfig("customer", 1, "c_custkey"),
                "supplier", new TableShardConfig("supplier", 1, "s_suppkey"),
                "nation", new TableShardConfig("nation", 1, "n_nationkey"),
                "region", new TableShardConfig("region", 1, "r_regionkey"),
                "part", new TableShardConfig("part", 1, "p_partkey"),
                "partsupp", new TableShardConfig("partsupp", 1, "ps_partkey")));
    }
}
