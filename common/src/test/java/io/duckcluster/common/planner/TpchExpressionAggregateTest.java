package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchExpressionAggregateTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final ClusterCatalog tpchCatalog = TpchCatalog.create();

    @Test
    void plansSumOnExpressionWithoutException() {
        PlannedQuery planned = planner.plan(
                "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem",
                tpchCatalog);

        assertEquals(MergeStrategyType.PARTIAL_AGG, planned.mergeStrategy());
        assertEquals(1, planned.analysis().aggregates().size());
        assertNull(planned.analysis().aggregates().get(0).inputColumn());
        assertNotNull(planned.analysis().aggregates().get(0).inputExpression());
        assertTrue(planned.fragments().get(0).sql().contains("__dc_agg_0"));
        assertTrue(planned.fragments().get(0).sql().contains("l_extendedprice"));
    }

    @Test
    void plansQ06StylePartialAggOnExpression() {
        PlannedQuery planned = planner.plan(
                """
                SELECT sum(l_extendedprice * l_discount) AS revenue
                FROM lineitem
                WHERE l_shipdate >= CAST('1994-01-01' AS date)
                  AND l_discount BETWEEN 0.05 AND 0.07
                """,
                tpchCatalog);

        assertEquals(MergeStrategyType.PARTIAL_AGG, planned.mergeStrategy());
        assertEquals("revenue", planned.analysis().aggregates().get(0).outputName());
        assertTrue(planned.fragments().get(0).sql().contains("lineitem_shard0"));
    }

    @Test
    void plansQ01StyleGroupByWithExpressionAggregates() {
        String sql = """
                SELECT
                    l_returnflag,
                    l_linestatus,
                    sum(l_quantity) AS sum_qty,
                    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                    avg(l_quantity) AS avg_qty
                FROM lineitem
                WHERE l_shipdate <= CAST('1998-09-02' AS date)
                GROUP BY l_returnflag, l_linestatus
                ORDER BY l_returnflag, l_linestatus
                """;

        PlannedQuery planned = planner.plan(sql, tpchCatalog);

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals(4, planned.analysis().aggregates().size());
        QueryAnalysis analysis = planned.analysis();
        assertEquals("sum_qty", analysis.aggregates().get(0).outputName());
        assertEquals("l_quantity", analysis.aggregates().get(0).inputColumn());
        assertNull(analysis.aggregates().get(1).inputColumn());
        assertNotNull(analysis.aggregates().get(1).inputExpression());
        assertEquals("sum_disc_price", analysis.aggregates().get(1).outputName());
        assertEquals(6, planned.fragments().size());
    }

    @Test
    void plansCaseExpressionInsideSum() {
        PlannedQuery planned = planner.plan(
                """
                SELECT l_shipmode,
                       sum(CASE WHEN o_orderpriority = '1-URGENT' THEN 1 ELSE 0 END) AS high_line_count
                FROM orders, lineitem
                WHERE o_orderkey = l_orderkey
                GROUP BY l_shipmode
                """,
                tpchCatalog);

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertNull(planned.analysis().aggregates().get(0).inputColumn());
        assertNotNull(planned.analysis().aggregates().get(0).inputExpression());
    }

    @Test
    void extractsExpressionOperandInAnalysis() {
        SqlSelect select = (SqlSelect) planner.parse(
                "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem");
        QueryAnalysis analysis = QueryAnalysisExtractor.extract(select, MergeStrategyType.PARTIAL_AGG);

        assertEquals(1, analysis.aggregates().size());
        assertNull(analysis.aggregates().get(0).inputColumn());
        assertNotNull(analysis.aggregates().get(0).inputExpression());
        assertEquals("revenue", analysis.aggregates().get(0).outputName());
    }
}
