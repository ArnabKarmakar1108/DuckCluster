package io.duckcluster.common.planner;

import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TpchFragmentSnapshotTest {
    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpch = TpchCatalog.create();

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "q09", "q10", "q11",
            "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22"
    })
    void allQueriesProduceValidFragments(String queryId) throws Exception {
        PlannedQuery planned = planner.plan(load(queryId), tpch);
        assertNotNull(planned.mergeStrategy());
        assertFalse(planned.fragments().isEmpty(), queryId + " must produce fragments");

        for (FragmentSpec fragment : planned.fragments()) {
            assertDoesNotThrow(
                    () -> FragmentSqlValidator.validate(fragment.sql(), planned, fragment.shardId(), tpch),
                    () -> queryId + " fragment " + fragment.shardId() + ": " + fragment.sql());
        }

        assertDoesNotThrow(() -> buildMergeSql(planned), queryId + " merge SQL");
    }

    private static void buildMergeSql(PlannedQuery planned) {
        switch (planned.mergeStrategy()) {
            case GROUP_BY_MERGE ->
                    MergeSqlBuilder.buildGroupByMerge(planned.analysis(), planned.topK());
            case PARTIAL_AGG -> MergeSqlBuilder.buildPartialAggMerge(planned.analysis());
            case TOP_K -> MergeSqlBuilder.buildTopKMerge(planned.analysis(), planned.topK());
            case NESTED_GROUP_BY_MERGE -> {
                if (planned.hasNestedDerivedTable()) {
                    MergeSqlBuilder.buildGroupByMerge(
                            planned.analysis(),
                            io.duckcluster.common.model.TopKSpec.none(),
                            planned.nestedDerivedTable().derivedColumnNames());
                }
            }
            case WITH_CTE_MERGE -> {
                if (planned.hasWithCte()) {
                    MergeSqlBuilder.buildGroupByMerge(
                            planned.analysis(),
                            io.duckcluster.common.model.TopKSpec.none(),
                            planned.withCte().innerColumnNames());
                }
            }
            case CONCATENATE -> { /* no merge SQL */ }
            default -> throw new IllegalStateException("Unknown merge strategy: " + planned.mergeStrategy());
        }
    }

    private String load(String queryId) throws Exception {
        Path path = Path.of("..", "tests", "integration", "queries", "tpch", queryId + ".sql");
        if (!Files.exists(path)) {
            path = Path.of("tests", "integration", "queries", "tpch", queryId + ".sql");
        }
        String sql = Files.readString(path);
        return sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
    }
}
