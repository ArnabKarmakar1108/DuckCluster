package io.duckcluster.common.planner;

import io.duckcluster.common.model.BroadcastTable;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.NestedDerivedTableSpec;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.common.model.TopKSpec;
import org.apache.calcite.config.Lex;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import io.duckcluster.common.model.WithCteSpec;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Set;

public final class CalciteQueryPlanner implements QueryPlanner {
    static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
            .withLex(Lex.JAVA)
            .withQuoting(Quoting.DOUBLE_QUOTE)
            .withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    @Override
    public PlannedQuery plan(String sql, ClusterCatalog catalog) {
        SqlNode parsed = parse(sql);
        SqlWith with = extractWith(parsed);
        if (with != null) {
            return planWithCte(sql, parsed, with, catalog);
        }
        SqlSelect select = asSelect(parsed);

        Optional<NestedDerivedTableDetector.Match> nested = NestedDerivedTableDetector.detect(select, parsed);
        if (nested.isPresent()) {
            return planNestedDerivedTable(sql, parsed, nested.get(), catalog);
        }

        TableClassification classification = classifyTables(select, catalog);
        int shardCount = classification.shardCount();

        MergeStrategyType mergeStrategy = detectMergeStrategy(parsed);
        QueryAnalysis analysis = QueryAnalysisExtractor.withMergeColumnNames(
                QueryAnalysisExtractor.extract(select, mergeStrategy));
        TopKSpec topK = TopKResolver.resolve(TopKExtractor.extract(parsed, select), analysis);

        String drivingTable = classification.drivingTable();
        Map<String, Integer> broadcastShardCounts = new HashMap<>();
        for (BroadcastTable bt : classification.broadcastTables()) {
            broadcastShardCounts.put(bt.tableName().toLowerCase(), bt.shardCount());
        }
        Map<String, Integer> catalogTableShardCounts = catalogTableShardCounts(catalog);
        List<BroadcastTable> subqueryBroadcasts = subqueryBroadcastTables(
                select, catalog, classification.broadcastTables(), drivingTable);
        List<String> correlatedCoPartition = correlatedCoPartitionTables(
                select, catalog, classification.broadcastTables(), subqueryBroadcasts, drivingTable);
        Set<String> materializedBroadcastTables = materializedBroadcastTables(
                classification.broadcastTables(), subqueryBroadcasts);

        List<FragmentSpec> fragments = new ArrayList<>(shardCount);
        for (int shardId = 0; shardId < shardCount; shardId++) {
        TopKSpec fragmentTopK = MergePushdownPlanner.fragmentTopK(analysis, topK, shardCount);
            String fragmentSql = FragmentSqlGenerator.generate(
                    select, shardId, analysis, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, fragmentTopK, materializedBroadcastTables);
            fragments.add(new FragmentSpec(shardId, shardId, fragmentSql, mergeStrategy));
        }

        return new PlannedQuery(sql, List.of(drivingTable), classification.broadcastTables(),
                fragments, mergeStrategy, analysis, List.of(), topK, null, null, subqueryBroadcasts,
                correlatedCoPartition);
    }

    private PlannedQuery planWithCte(String sql, SqlNode parsed, SqlWith with, ClusterCatalog catalog) {
        SqlSelect cteSelect = WithClauseExpander.firstCteSelect(with);
        if (cteSelect.getGroup() == null || cteSelect.getGroup().isEmpty()) {
            throw new IllegalArgumentException("WITH CTE body must include GROUP BY for distributed planning");
        }

        String cteName = WithClauseExpander.firstCteName(with);
        SqlNode outerNode = outerQueryNode(parsed, with);
        SqlSelect outerSelect = asSelect(outerNode);

        TableClassification classification = classifyTables(cteSelect, catalog);
        int shardCount = classification.shardCount();

        QueryAnalysis innerAnalysis = QueryAnalysisExtractor.withMergeColumnNames(
                QueryAnalysisExtractor.extract(cteSelect, MergeStrategyType.GROUP_BY_MERGE));
        TopKSpec outerTopK = TopKResolver.resolve(
                TopKExtractor.extract(outerNode, outerSelect),
                QueryAnalysisExtractor.withMergeColumnNames(
                        QueryAnalysisExtractor.extract(outerSelect, MergeStrategyType.GROUP_BY_MERGE)));

        String drivingTable = classification.drivingTable();
        Map<String, Integer> broadcastShardCounts = new HashMap<>();
        for (BroadcastTable bt : classification.broadcastTables()) {
            broadcastShardCounts.put(bt.tableName().toLowerCase(), bt.shardCount());
        }
        Map<String, Integer> catalogTableShardCounts = catalogTableShardCounts(catalog);

        List<String> coordinatorTables = WithClauseExpander.coordinatorDimensionTables(
                outerSelect, WithClauseExpander.cteNames(with), catalog);
        String outerSql = WithCteOuterSqlGenerator.generate(outerNode, cteName);
        WithCteSpec withCteSpec = new WithCteSpec(
                innerAnalysis,
                innerAnalysis.outputColumnNames(),
                outerSql,
                coordinatorTables,
                outerTopK);

        List<FragmentSpec> fragments = new ArrayList<>(shardCount);
        Set<String> materializedBroadcastTables = materializedBroadcastTables(classification.broadcastTables());
        for (int shardId = 0; shardId < shardCount; shardId++) {
            String fragmentSql = FragmentSqlGenerator.generate(
                    cteSelect, shardId, innerAnalysis, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, TopKSpec.none(), materializedBroadcastTables);
            fragments.add(new FragmentSpec(shardId, shardId, fragmentSql, MergeStrategyType.WITH_CTE_MERGE));
        }

        return new PlannedQuery(sql, List.of(drivingTable), classification.broadcastTables(),
                fragments, MergeStrategyType.WITH_CTE_MERGE, innerAnalysis, List.of(), TopKSpec.none(),
                null, withCteSpec, List.of(), List.of());
    }

    private PlannedQuery planNestedDerivedTable(
            String sql,
            SqlNode parsed,
            NestedDerivedTableDetector.Match nested,
            ClusterCatalog catalog) {
        SqlSelect innerSelect = nested.innerSelect();

        TableClassification classification = classifyTables(innerSelect, catalog);
        int shardCount = classification.shardCount();

        QueryAnalysis innerAnalysis = QueryAnalysisExtractor.withMergeColumnNames(
                QueryAnalysisExtractor.extract(innerSelect, MergeStrategyType.GROUP_BY_MERGE));
        QueryAnalysis outerAnalysis = QueryAnalysisExtractor.withMergeColumnNames(
                QueryAnalysisExtractor.extract(asSelect(parsed), MergeStrategyType.GROUP_BY_MERGE));

        String drivingTable = classification.drivingTable();
        Map<String, Integer> broadcastShardCounts = new HashMap<>();
        for (BroadcastTable bt : classification.broadcastTables()) {
            broadcastShardCounts.put(bt.tableName().toLowerCase(), bt.shardCount());
        }
        Map<String, Integer> catalogTableShardCounts = catalogTableShardCounts(catalog);

        MergeStrategyType mergeStrategy = MergeStrategyType.NESTED_GROUP_BY_MERGE;
        NestedDerivedTableSpec nestedSpec = new NestedDerivedTableSpec(
                outerAnalysis, derivedColumnNames(nested, innerAnalysis), nested.outerTopK());

        List<BroadcastTable> subqueryBroadcasts = subqueryBroadcastTables(
                innerSelect, catalog, classification.broadcastTables(), drivingTable);
        List<String> correlatedCoPartition = correlatedCoPartitionTables(
                innerSelect, catalog, classification.broadcastTables(), subqueryBroadcasts, drivingTable);
        Set<String> materializedBroadcastTables = materializedBroadcastTables(
                classification.broadcastTables(), subqueryBroadcasts);

        List<FragmentSpec> fragments = new ArrayList<>(shardCount);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            String fragmentSql = FragmentSqlGenerator.generate(
                    innerSelect, shardId, innerAnalysis, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, TopKSpec.none(), materializedBroadcastTables);
            fragments.add(new FragmentSpec(shardId, shardId, fragmentSql, mergeStrategy));
        }

        return new PlannedQuery(sql, List.of(drivingTable), classification.broadcastTables(),
                fragments, mergeStrategy, innerAnalysis, List.of(), TopKSpec.none(),
                nestedSpec, null, subqueryBroadcasts, correlatedCoPartition);
    }

    private static List<String> derivedColumnNames(
            NestedDerivedTableDetector.Match nested, QueryAnalysis innerAnalysis) {
        if (!nested.derivedColumnNames().isEmpty()) {
            return nested.derivedColumnNames();
        }
        return innerAnalysis.outputColumnNames();
    }

    public SqlNode parse(String sql) {
        SqlParser parser = SqlParser.create(stripTrailingSemicolon(SqlNormalizer.normalize(sql)), PARSER_CONFIG);
        try {
            return parser.parseStmt();
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("Invalid SQL: " + e.getMessage(), e);
        }
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.strip();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1).strip();
        }
        return trimmed;
    }

    public MergeStrategyType detectMergeStrategy(SqlNode parsed) {
        SqlSelect select = asSelect(parsed);
        if (select.getGroup() != null && !select.getGroup().isEmpty()) {
            return MergeStrategyType.GROUP_BY_MERGE;
        }
        if (containsAggregate(select.getSelectList())) {
            return MergeStrategyType.PARTIAL_AGG;
        }
        if (hasTopK(parsed, select)) {
            return MergeStrategyType.TOP_K;
        }
        return MergeStrategyType.CONCATENATE;
    }

    static TableClassification classifyTables(SqlSelect select, ClusterCatalog catalog) {
        List<String> allTables = collectTableNames(select.getFrom());
        if (allTables.isEmpty()) {
            throw new IllegalArgumentException("No tables found in FROM clause");
        }

        for (String table : allTables) {
            if (!catalog.hasTable(table)) {
                throw new IllegalArgumentException("Unknown table: " + table
                        + ". Known tables: " + catalog.getShardedTableNames());
            }
        }

        String drivingTable = allTables.get(0);
        int maxShards = catalog.table(drivingTable).shardCount();
        for (String t : allTables) {
            int count = catalog.table(t).shardCount();
            if (count >= maxShards) {
                drivingTable = t;
                maxShards = count;
            }
        }

        List<BroadcastTable> broadcast = new ArrayList<>();
        for (String t : allTables) {
            if (!t.equalsIgnoreCase(drivingTable)) {
                broadcast.add(new BroadcastTable(t, catalog.table(t).shardCount()));
            }
        }

        return new TableClassification(drivingTable, broadcast, maxShards);
    }

    static List<String> collectTableNames(SqlNode from) {
        Set<String> tables = new LinkedHashSet<>();
        collectTableNamesRecursive(from, tables);
        return List.copyOf(tables);
    }

    private static void collectTableNamesRecursive(SqlNode node, Set<String> tables) {
        String tableName = TableNameSupport.baseTableName(node);
        if (tableName != null) {
            tables.add(tableName);
            return;
        }
        if (node instanceof SqlSelect select) {
            if (select.getFrom() != null) {
                collectTableNamesRecursive(select.getFrom(), tables);
            }
        } else if (node instanceof SqlJoin join) {
            collectTableNamesRecursive(join.getLeft(), tables);
            collectTableNamesRecursive(join.getRight(), tables);
        } else if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
            collectTableNamesRecursive(call.operand(0), tables);
        }
    }

    private static SqlNode outerQueryNode(SqlNode parsed, SqlWith with) {
        if (parsed instanceof SqlOrderBy orderBy && orderBy.query instanceof SqlWith) {
            return new SqlOrderBy(
                    orderBy.getParserPosition(),
                    with.body,
                    orderBy.orderList,
                    orderBy.offset,
                    orderBy.fetch);
        }
        return with.body;
    }

    private static SqlWith extractWith(SqlNode parsed) {
        if (parsed instanceof SqlWith with) {
            return with;
        }
        if (parsed instanceof SqlOrderBy orderBy && orderBy.query instanceof SqlWith with) {
            return with;
        }
        return null;
    }

    private static SqlSelect asSelect(SqlNode parsed) {
        if (parsed instanceof SqlWith with) {
            return asSelect(with.body);
        }
        if (parsed instanceof SqlOrderBy orderBy) {
            if (orderBy.query instanceof SqlSelect select) {
                return select;
            }
            throw new IllegalArgumentException("Only SELECT queries are supported");
        }
        if (parsed instanceof SqlSelect select) {
            return select;
        }
        throw new IllegalArgumentException("Only SELECT queries are supported");
    }

    private static boolean hasTopK(SqlNode parsed, SqlSelect select) {
        if (parsed instanceof SqlOrderBy orderBy) {
            return (orderBy.orderList != null && !orderBy.orderList.isEmpty()) || orderBy.fetch != null;
        }
        return (select.getOrderList() != null && !select.getOrderList().isEmpty()) || select.getFetch() != null;
    }

    private static boolean containsAggregate(SqlNode node) {
        if (node == null) {
            return false;
        }
        AggregateDetector detector = new AggregateDetector();
        node.accept(detector);
        return detector.foundAggregate();
    }

    private static final class AggregateDetector extends SqlShuttle {
        private boolean aggregate;

        @Override
        public SqlNode visit(SqlCall call) {
            if (AggregateSqlSupport.isAggregateCall(call)) {
                aggregate = true;
            }
            return super.visit(call);
        }

        boolean foundAggregate() {
            return aggregate;
        }
    }

    record TableClassification(String drivingTable, List<BroadcastTable> broadcastTables, int shardCount) {}

    private static Map<String, Integer> catalogTableShardCounts(ClusterCatalog catalog) {
        Map<String, Integer> counts = new HashMap<>();
        for (String tableName : catalog.getShardedTableNames()) {
            counts.put(tableName.toLowerCase(), catalog.table(tableName).shardCount());
        }
        return counts;
    }

    private static List<BroadcastTable> subqueryBroadcastTables(
            SqlSelect select,
            ClusterCatalog catalog,
            List<BroadcastTable> mainBroadcastTables,
            String drivingTable) {
        Set<String> alreadyBroadcast = new LinkedHashSet<>();
        for (BroadcastTable broadcastTable : mainBroadcastTables) {
            alreadyBroadcast.add(broadcastTable.tableName().toLowerCase());
        }
        List<BroadcastTable> subqueryBroadcasts = new ArrayList<>();
        for (String tableName : SubqueryAnalyzer.uncorrelatedMultiShardTables(select, catalog, drivingTable)) {
            if (!alreadyBroadcast.contains(tableName)) {
                subqueryBroadcasts.add(new BroadcastTable(tableName, catalog.table(tableName).shardCount()));
            }
        }
        return subqueryBroadcasts;
    }

    private static List<String> correlatedCoPartitionTables(
            SqlSelect select,
            ClusterCatalog catalog,
            List<BroadcastTable> mainBroadcastTables,
            List<BroadcastTable> subqueryBroadcastTables,
            String drivingTable) {
        Set<String> alreadyCovered = new LinkedHashSet<>();
        for (BroadcastTable broadcastTable : mainBroadcastTables) {
            alreadyCovered.add(broadcastTable.tableName().toLowerCase());
        }
        for (BroadcastTable broadcastTable : subqueryBroadcastTables) {
            alreadyCovered.add(broadcastTable.tableName().toLowerCase());
        }
        List<String> tables = new ArrayList<>();
        for (String tableName : SubqueryAnalyzer.correlatedCoPartitionTables(select, catalog, drivingTable)) {
            if (!alreadyCovered.contains(tableName)) {
                tables.add(tableName);
            }
        }
        return tables;
    }

    @SafeVarargs
    private static Set<String> materializedBroadcastTables(List<BroadcastTable>... broadcastLists) {
        Set<String> tables = new HashSet<>();
        for (List<BroadcastTable> broadcastTables : broadcastLists) {
            for (BroadcastTable broadcastTable : broadcastTables) {
                if (broadcastTable.shardCount() > 1) {
                    tables.add(broadcastTable.tableName().toLowerCase());
                }
            }
        }
        return tables;
    }
}
