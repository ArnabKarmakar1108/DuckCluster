package io.duckcluster.common.planner;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;

import io.duckcluster.common.model.ClusterCatalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Detects predicate subqueries and TPC-H-style column-prefix correlation. */
public final class SubqueryAnalyzer {
    private static final Map<String, String> TABLE_PREFIXES = Map.ofEntries(
            Map.entry("lineitem", "l_"),
            Map.entry("orders", "o_"),
            Map.entry("customer", "c_"),
            Map.entry("part", "p_"),
            Map.entry("partsupp", "ps_"),
            Map.entry("supplier", "s_"),
            Map.entry("nation", "n_"),
            Map.entry("region", "r_"));

    private static final List<String> PREFIXES_LONGEST_FIRST = List.of(
            "ps_", "l_", "o_", "c_", "p_", "s_", "n_", "r_");

    private SubqueryAnalyzer() {}

    public static boolean isPredicateSubquery(SqlCall call) {
        return switch (call.getKind()) {
            case IN, NOT_IN, EXISTS -> operandSubquery(call) != null;
            default -> containsOperandSubquery(call);
        };
    }

    public static SqlSelect operandSubquery(SqlCall call) {
        return switch (call.getKind()) {
            case IN, NOT_IN -> subqueryOperand(call, 1);
            case EXISTS -> subqueryOperand(call, 0);
            default -> findOperandSubquery(call);
        };
    }

    public static boolean isCorrelated(SqlSelect subquery, Collection<String> enclosingTableNames) {
        return isCorrelated(subquery, FromScope.ofTables(enclosingTableNames));
    }

    public static boolean isCorrelated(SqlSelect subquery, FromScope enclosing) {
        FromScope inner = FromScope.of(subquery.getFrom());
        Set<String> outerPrefixes = prefixesForTables(enclosing.tableNames());
        Set<String> innerPrefixes = prefixesForTables(inner.tableNames());
        if (outerPrefixes.isEmpty()) {
            return false;
        }
        CorrelationDetector detector = new CorrelationDetector(enclosing, inner, outerPrefixes, innerPrefixes);
        if (subquery.getSelectList() != null) {
            subquery.getSelectList().accept(detector);
        }
        if (subquery.getWhere() != null) {
            subquery.getWhere().accept(detector);
        }
        if (subquery.getHaving() != null) {
            subquery.getHaving().accept(detector);
        }
        return detector.correlated;
    }

    /** Multi-shard tables referenced by predicate subqueries that use global UNION ALL rewrite. */
    public static Set<String> uncorrelatedMultiShardTables(
            SqlSelect select, ClusterCatalog catalog, String drivingTable) {
        FromScope outer = FromScope.of(select.getFrom());
        Set<String> tables = new LinkedHashSet<>();
        collectFromEmbeddedSelects(select.getFrom(), embedded -> {
            FromScope embeddedScope = outer.union(FromScope.of(embedded.getFrom()));
            if (embedded.getWhere() != null) {
                collectGlobalSubqueryTables(embedded.getWhere(), embeddedScope, drivingTable, catalog, tables);
            }
            if (embedded.getHaving() != null) {
                collectGlobalSubqueryTables(embedded.getHaving(), embeddedScope, drivingTable, catalog, tables);
            }
        });
        if (select.getWhere() != null) {
            collectGlobalSubqueryTables(select.getWhere(), outer, drivingTable, catalog, tables);
        }
        if (select.getHaving() != null) {
            collectGlobalSubqueryTables(select.getHaving(), outer, drivingTable, catalog, tables);
        }
        return tables;
    }

    /**
     * Multi-shard inner tables from correlated EXISTS that must be co-located with the driving shard.
     */
    public static Set<String> correlatedCoPartitionTables(
            SqlSelect select, ClusterCatalog catalog, String drivingTable) {
        FromScope outer = FromScope.of(select.getFrom());
        Set<String> tables = new LinkedHashSet<>();
        collectFromEmbeddedSelects(select.getFrom(), embedded -> {
            FromScope embeddedScope = outer.union(FromScope.of(embedded.getFrom()));
            if (embedded.getWhere() != null) {
                collectCorrelatedCoPartitionTables(
                        embedded.getWhere(), embeddedScope, drivingTable, catalog, tables);
            }
            if (embedded.getHaving() != null) {
                collectCorrelatedCoPartitionTables(
                        embedded.getHaving(), embeddedScope, drivingTable, catalog, tables);
            }
        });
        if (select.getWhere() != null) {
            collectCorrelatedCoPartitionTables(select.getWhere(), outer, drivingTable, catalog, tables);
        }
        if (select.getHaving() != null) {
            collectCorrelatedCoPartitionTables(select.getHaving(), outer, drivingTable, catalog, tables);
        }
        return tables;
    }

    static boolean isExistsSubquery(SqlCall call) {
        return call.getKind() == SqlKind.EXISTS;
    }

    /** Co-located EXISTS rewrite is valid only when driving and inner tables share shard parallelism. */
    static boolean canCoLocateSubquery(SqlSelect subquery, String drivingTable, Map<String, Integer> catalogTableShardCounts) {
        int drivingShards = catalogTableShardCounts.getOrDefault(drivingTable.toLowerCase(), 1);
        if (drivingShards > 1) {
            return true;
        }
        for (String tableName : CalciteQueryPlanner.collectTableNames(subquery.getFrom())) {
            int innerShards = catalogTableShardCounts.getOrDefault(tableName.toLowerCase(), 1);
            if (innerShards > 1) {
                return false;
            }
        }
        return true;
    }

    public static SqlCall replaceSubqueryOperand(SqlCall call, SqlSelect rewritten) {
        return switch (call.getKind()) {
            case IN, NOT_IN -> call.getOperator().createCall(call.getParserPosition(), call.operand(0), rewritten);
            case EXISTS -> call.getOperator().createCall(call.getParserPosition(), rewritten);
            default -> replaceFirstSubqueryOperand(call, rewritten);
        };
    }

    private static void collectGlobalSubqueryTables(
            SqlNode node,
            FromScope enclosing,
            String drivingTable,
            ClusterCatalog catalog,
            Set<String> tables) {
        Map<String, Integer> shardCounts = catalogShardCounts(catalog);
        node.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                if (isPredicateSubquery(call)) {
                    SqlSelect subquery = operandSubquery(call);
                    boolean coLocated = isCorrelated(subquery, enclosing)
                            && isExistsSubquery(call)
                            && canCoLocateSubquery(subquery, drivingTable, shardCounts);
                    if (!coLocated) {
                        for (String tableName : CalciteQueryPlanner.collectTableNames(subquery.getFrom())) {
                            if (catalog.hasTable(tableName) && catalog.table(tableName).shardCount() > 1) {
                                tables.add(tableName.toLowerCase());
                            }
                        }
                    }
                    FromScope subqueryScope = enclosing.union(FromScope.of(subquery.getFrom()));
                    if (subquery.getWhere() != null) {
                        collectGlobalSubqueryTables(subquery.getWhere(), subqueryScope, drivingTable, catalog, tables);
                    }
                    if (subquery.getHaving() != null) {
                        collectGlobalSubqueryTables(subquery.getHaving(), subqueryScope, drivingTable, catalog, tables);
                    }
                    return call;
                }
                return super.visit(call);
            }
        });
    }

    private static void collectUncorrelatedMultiShardTables(
            SqlNode node,
            Set<String> enclosingTableNames,
            ClusterCatalog catalog,
            Set<String> tables) {
        collectGlobalSubqueryTables(node, FromScope.ofTables(enclosingTableNames), "", catalog, tables);
    }

    private static void collectCorrelatedCoPartitionTables(
            SqlNode node,
            FromScope enclosing,
            String drivingTable,
            ClusterCatalog catalog,
            Set<String> tables) {
        Map<String, Integer> shardCounts = catalogShardCounts(catalog);
        node.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                if (isPredicateSubquery(call)) {
                    SqlSelect subquery = operandSubquery(call);
                    if (isExistsSubquery(call)
                            && isCorrelated(subquery, enclosing)
                            && canCoLocateSubquery(subquery, drivingTable, shardCounts)) {
                        for (String tableName : CalciteQueryPlanner.collectTableNames(subquery.getFrom())) {
                            if (catalog.hasTable(tableName) && catalog.table(tableName).shardCount() > 1) {
                                tables.add(tableName.toLowerCase());
                            }
                        }
                    }
                    FromScope subqueryScope = enclosing.union(FromScope.of(subquery.getFrom()));
                    if (subquery.getWhere() != null) {
                        collectCorrelatedCoPartitionTables(
                                subquery.getWhere(), subqueryScope, drivingTable, catalog, tables);
                    }
                    if (subquery.getHaving() != null) {
                        collectCorrelatedCoPartitionTables(
                                subquery.getHaving(), subqueryScope, drivingTable, catalog, tables);
                    }
                    return call;
                }
                return super.visit(call);
            }
        });
    }

    private static Map<String, Integer> catalogShardCounts(ClusterCatalog catalog) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String tableName : catalog.getShardedTableNames()) {
            counts.put(tableName.toLowerCase(), catalog.table(tableName).shardCount());
        }
        return counts;
    }

    private static void collectFromEmbeddedSelects(SqlNode from, java.util.function.Consumer<SqlSelect> visitor) {
        if (from == null) {
            return;
        }
        if (from instanceof SqlSelect select) {
            visitor.accept(select);
            return;
        }
        if (from instanceof SqlJoin join) {
            collectFromEmbeddedSelects(join.getLeft(), visitor);
            collectFromEmbeddedSelects(join.getRight(), visitor);
            return;
        }
        if (from instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
            collectFromEmbeddedSelects(call.operand(0), visitor);
        }
    }

    static String columnPrefix(String columnName) {
        String lower = columnName.toLowerCase();
        for (String prefix : PREFIXES_LONGEST_FIRST) {
            if (lower.startsWith(prefix)) {
                return prefix;
            }
        }
        return "";
    }

    private static Set<String> prefixesForTables(Collection<String> tableNames) {
        Set<String> prefixes = new LinkedHashSet<>();
        for (String tableName : tableNames) {
            String prefix = TABLE_PREFIXES.get(tableName.toLowerCase());
            if (prefix != null) {
                prefixes.add(prefix);
            }
        }
        return prefixes;
    }

    private static SqlSelect subqueryOperand(SqlCall call, int index) {
        if (call.operandCount() <= index) {
            return null;
        }
        SqlNode operand = call.operand(index);
        return operand instanceof SqlSelect select ? select : null;
    }

    private static boolean containsOperandSubquery(SqlCall call) {
        return findOperandSubquery(call) != null;
    }

    private static SqlSelect findOperandSubquery(SqlCall call) {
        for (SqlNode operand : call.getOperandList()) {
            if (operand instanceof SqlSelect select) {
                return select;
            }
        }
        return null;
    }

    private static SqlCall replaceFirstSubqueryOperand(SqlCall call, SqlSelect rewritten) {
        List<SqlNode> operands = new ArrayList<>(call.getOperandList());
        for (int i = 0; i < operands.size(); i++) {
            if (operands.get(i) instanceof SqlSelect) {
                operands.set(i, rewritten);
                break;
            }
        }
        return (SqlCall) call.getOperator().createCall(call.getParserPosition(), operands);
    }

    private static final class CorrelationDetector extends SqlShuttle {
        private final FromScope enclosing;
        private final FromScope inner;
        private final Set<String> outerPrefixes;
        private final Set<String> innerPrefixes;
        private boolean correlated;

        private CorrelationDetector(
                FromScope enclosing,
                FromScope inner,
                Set<String> outerPrefixes,
                Set<String> innerPrefixes) {
            this.enclosing = enclosing;
            this.inner = inner;
            this.outerPrefixes = outerPrefixes;
            this.innerPrefixes = innerPrefixes;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (SubqueryAnalyzer.isPredicateSubquery(call)) {
                return call;
            }
            return super.visit(call);
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (id.isStar()) {
                return id;
            }
            if (enclosing.referencesOuterAlias(id, inner)) {
                correlated = true;
                return id;
            }
            String columnName = id.names.get(id.names.size() - 1);
            String prefix = columnPrefix(columnName);
            if (!prefix.isEmpty() && outerPrefixes.contains(prefix) && !innerPrefixes.contains(prefix)) {
                correlated = true;
            }
            return id;
        }
    }
}
