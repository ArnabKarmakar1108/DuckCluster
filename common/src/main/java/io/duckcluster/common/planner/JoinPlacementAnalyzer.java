package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.ShuffleJoinTable;
import io.duckcluster.common.model.TableColumn;
import io.duckcluster.common.model.TableShardConfig;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Decides how each non-driving multi-shard table in FROM is joined to the driving table (4.3):
 *
 * <ul>
 *   <li><b>Co-partitioned</b>: both tables declare shard keys, have the same shard count, and every
 *       occurrence of the table is equi-joined (directly or transitively) key-to-key with the driving
 *       table. Fragment {@code i} joins shard {@code i} of both sides locally.</li>
 *   <li><b>Shuffle</b>: the driving side joins on its own shard key but the other table is partitioned
 *       differently. The other table is re-bucketed with the driving table's hash so bucket {@code i}
 *       meets driving shard {@code i}; each row moves at most once instead of to every worker.</li>
 *   <li>Otherwise <b>broadcast</b> (the planner's default).</li>
 * </ul>
 *
 * Fragment rewriting is per table name, not per alias, so a table qualifies only if all of its
 * occurrences qualify.
 */
final class JoinPlacementAnalyzer {

    record Placement(Set<String> coPartitioned, List<ShuffleJoinTable> shuffled) {
        static final Placement NONE = new Placement(Set.of(), List.of());

        Set<String> shuffledTableNames() {
            Set<String> names = new LinkedHashSet<>();
            for (ShuffleJoinTable table : shuffled) {
                names.add(table.tableName());
            }
            return names;
        }
    }

    private JoinPlacementAnalyzer() {}

    /**
     * A multi-shard table referenced more than once in the same FROM scope must equi-join every alias on
     * the declared shard key. Otherwise each fragment would only see within-shard matches and results would
     * be wrong.
     */
    static void validateSelfJoins(SqlSelect select, ClusterCatalog catalog) {
        for (Scope scope : scopesOf(select)) {
            Map<String, List<String>> aliasesByTable = new LinkedHashMap<>();
            for (Map.Entry<String, String> occurrence : scope.aliasToTable().entrySet()) {
                aliasesByTable.computeIfAbsent(occurrence.getValue(), ignored -> new ArrayList<>())
                        .add(occurrence.getKey());
            }
            for (Map.Entry<String, List<String>> entry : aliasesByTable.entrySet()) {
                String table = entry.getKey();
                List<String> aliases = entry.getValue();
                if (aliases.size() < 2 || !catalog.hasTable(table) || catalog.table(table).shardCount() <= 1) {
                    continue;
                }
                TableShardConfig config = catalog.table(table);
                if (!config.hasDeclaredShardKey()) {
                    throw new IllegalArgumentException(
                            "Self-join on multi-shard table '" + table
                                    + "' requires a declared shard key in the catalog");
                }
                ColumnClasses classes = ColumnClasses.of(
                        scope.equalities(), List.of(scope.aliasToTable()), catalog);
                String shardKey = config.shardKey();
                for (String alias : aliases) {
                    if (!classes.connectedToAny(alias, shardKey, aliases, shardKey)) {
                        throw new IllegalArgumentException(
                                "Self-join on '" + table + "' must equi-join every alias on shard key '"
                                        + shardKey + "' (non-key joins are not supported)");
                    }
                }
            }
        }
    }

    static Placement analyze(
            SqlSelect select, ClusterCatalog catalog, String drivingTable, PlannerOptions options) {
        if (select.getFrom() == null || (!options.coPartitionJoins() && !options.shuffleJoins())) {
            return Placement.NONE;
        }
        String driving = drivingTable.toLowerCase(Locale.ROOT);
        TableShardConfig drivingConfig = catalog.table(driving);
        if (!drivingConfig.hasDeclaredShardKey() || drivingConfig.shardCount() <= 1) {
            return Placement.NONE;
        }
        Optional<String> drivingKeyType = columnType(catalog, driving, drivingConfig.shardKey());

        Map<String, Boolean> coPartitionOk = new LinkedHashMap<>();
        Map<String, Set<String>> shuffleColumns = new LinkedHashMap<>();
        Set<String> shuffleBlocked = new LinkedHashSet<>();

        for (Scope scope : scopesOf(select)) {
            List<Map<String, String>> chain = List.of(scope.aliasToTable());
            ColumnClasses classes = ColumnClasses.of(scope.equalities(), chain, catalog);
            List<String> drivingAliases = scope.aliasesFor(driving);

            for (Map.Entry<String, String> occurrence : scope.aliasToTable().entrySet()) {
                String alias = occurrence.getKey();
                String table = occurrence.getValue();
                if (table.equals(driving) || !catalog.hasTable(table) || catalog.table(table).shardCount() <= 1) {
                    continue;
                }
                TableShardConfig config = catalog.table(table);
                boolean coPartitioned = options.coPartitionJoins()
                        && config.hasDeclaredShardKey()
                        && config.shardCount() == drivingConfig.shardCount()
                        && typesCompatible(catalog, table, config.shardKey(), driving, drivingConfig.shardKey())
                        && classes.connectedToAny(alias, config.shardKey(), drivingAliases, drivingConfig.shardKey());
                coPartitionOk.merge(table, coPartitioned, Boolean::logicalAnd);

                Optional<String> joinColumn = classes.columnOfAliasConnectedTo(
                        alias, drivingAliases, drivingConfig.shardKey());
                if (joinColumn.isPresent()) {
                    shuffleColumns.computeIfAbsent(table, ignored -> new LinkedHashSet<>())
                            .add(joinColumn.get().toLowerCase(Locale.ROOT));
                } else {
                    shuffleBlocked.add(table);
                }
            }
        }

        Set<String> coPartitioned = new LinkedHashSet<>();
        coPartitionOk.forEach((table, ok) -> {
            if (ok) {
                coPartitioned.add(table);
            }
        });

        List<ShuffleJoinTable> shuffled = new ArrayList<>();
        if (options.shuffleJoins() && drivingKeyType.isPresent()) {
            for (Map.Entry<String, Set<String>> entry : shuffleColumns.entrySet()) {
                String table = entry.getKey();
                if (coPartitioned.contains(table) || shuffleBlocked.contains(table) || entry.getValue().size() != 1) {
                    continue;
                }
                if (catalog.estimatedRowCount(table) < options.shuffleMinRows()) {
                    continue;
                }
                shuffled.add(new ShuffleJoinTable(
                        table,
                        catalog.table(table).shardCount(),
                        entry.getValue().iterator().next(),
                        drivingKeyType.get(),
                        drivingConfig.shardCount()));
            }
        }
        return new Placement(Set.copyOf(coPartitioned), List.copyOf(shuffled));
    }

    /**
     * A correlated EXISTS subquery may scan only the driving shard's slice of its inner tables when
     * every inner multi-shard table is equated, on its shard key, with the shard key of an outer
     * table that is itself sliced by shard (the driving table or one co-partitioned with it).
     */
    static boolean canCoLocateSubquery(
            SqlSelect subquery,
            FromScope enclosing,
            String drivingTable,
            ClusterCatalog catalog,
            Set<String> coPartitionedTables) {
        Scope inner = Scope.direct(subquery);
        List<String> innerMulti = new ArrayList<>();
        for (Map.Entry<String, String> occurrence : inner.aliasToTable().entrySet()) {
            String table = occurrence.getValue();
            if (catalog != null && catalog.hasTable(table) && catalog.table(table).shardCount() > 1) {
                innerMulti.add(occurrence.getKey());
            }
        }
        if (innerMulti.isEmpty()) {
            return true;
        }
        if (catalog == null || !catalog.hasTable(drivingTable)) {
            return false;
        }
        String driving = drivingTable.toLowerCase(Locale.ROOT);
        TableShardConfig drivingConfig = catalog.table(driving);
        if (!drivingConfig.hasDeclaredShardKey() || drivingConfig.shardCount() <= 1) {
            return false;
        }
        Map<String, String> outerAliases = enclosing.aliasToTableName();
        Map<String, String> slicedOuterKeys = new LinkedHashMap<>();
        outerAliases.forEach((alias, table) -> {
            boolean sliced = table.equals(driving) || coPartitionedTables.contains(table);
            if (sliced && !inner.aliasToTable().containsKey(alias) && catalog.hasTable(table)) {
                slicedOuterKeys.put(alias, catalog.table(table).shardKey());
            }
        });
        ColumnClasses classes = ColumnClasses.of(
                inner.equalities(), List.of(inner.aliasToTable(), outerAliases), catalog);
        for (String alias : innerMulti) {
            String table = inner.aliasToTable().get(alias);
            TableShardConfig config = catalog.table(table);
            if (!config.hasDeclaredShardKey()
                    || config.shardCount() != drivingConfig.shardCount()
                    || !typesCompatible(catalog, table, config.shardKey(), driving, drivingConfig.shardKey())
                    || !classes.connectedToAnyKey(alias, config.shardKey(), slicedOuterKeys)) {
                return false;
            }
        }
        return true;
    }

    private static boolean typesCompatible(
            ClusterCatalog catalog, String leftTable, String leftColumn, String rightTable, String rightColumn) {
        Optional<String> left = columnType(catalog, leftTable, leftColumn);
        Optional<String> right = columnType(catalog, rightTable, rightColumn);
        return left.isEmpty() || right.isEmpty() || left.get().equalsIgnoreCase(right.get());
    }

    private static Optional<String> columnType(ClusterCatalog catalog, String table, String column) {
        for (TableColumn tableColumn : catalog.tableColumns(table)) {
            if (tableColumn.name().equalsIgnoreCase(column)) {
                return "UNKNOWN".equalsIgnoreCase(tableColumn.type())
                        ? Optional.empty()
                        : Optional.of(tableColumn.type());
            }
        }
        return Optional.empty();
    }

    private static boolean tableHasColumn(ClusterCatalog catalog, String table, String column) {
        if (!catalog.hasTable(table)) {
            return false;
        }
        List<TableColumn> columns = catalog.tableColumns(table);
        if (columns.isEmpty()) {
            return catalog.table(table).shardKey().equalsIgnoreCase(column);
        }
        for (TableColumn tableColumn : columns) {
            if (tableColumn.name().equalsIgnoreCase(column)) {
                return true;
            }
        }
        return false;
    }

    /** All SELECT scopes reachable through FROM (the query itself plus derived tables). */
    private static List<Scope> scopesOf(SqlSelect select) {
        List<Scope> scopes = new ArrayList<>();
        collectScopes(select, scopes);
        return scopes;
    }

    private static void collectScopes(SqlSelect select, List<Scope> scopes) {
        Scope scope = Scope.direct(select);
        scopes.add(scope);
        for (SqlSelect derived : scope.derivedSelects()) {
            collectScopes(derived, scopes);
        }
    }

    /** Base tables directly in one SELECT's FROM (derived tables are separate scopes) plus its equi-join conjuncts. */
    private record Scope(Map<String, String> aliasToTable, List<SqlBasicCall> equalities, List<SqlSelect> derivedSelects) {

        static Scope direct(SqlSelect select) {
            Map<String, String> aliases = new LinkedHashMap<>();
            List<SqlBasicCall> equalities = new ArrayList<>();
            List<SqlSelect> derived = new ArrayList<>();
            if (select.getFrom() != null) {
                collectFrom(select.getFrom(), aliases, equalities, derived);
            }
            if (select.getWhere() != null) {
                collectEqualities(select.getWhere(), equalities);
            }
            return new Scope(aliases, equalities, derived);
        }

        List<String> aliasesFor(String table) {
            List<String> aliases = new ArrayList<>();
            aliasToTable.forEach((alias, aliasTable) -> {
                if (aliasTable.equals(table)) {
                    aliases.add(alias);
                }
            });
            return aliases;
        }

        private static void collectFrom(
                SqlNode node, Map<String, String> aliases, List<SqlBasicCall> equalities, List<SqlSelect> derived) {
            if (node instanceof SqlJoin join) {
                collectFrom(join.getLeft(), aliases, equalities, derived);
                collectFrom(join.getRight(), aliases, equalities, derived);
                if (join.getCondition() != null) {
                    collectEqualities(join.getCondition(), equalities);
                }
                return;
            }
            if (node instanceof SqlSelect select) {
                derived.add(select);
                return;
            }
            if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
                SqlNode inner = call.operand(0);
                if (inner instanceof SqlSelect select) {
                    derived.add(select);
                    return;
                }
                String table = TableNameSupport.baseTableName(inner);
                if (table != null && call.operand(1) instanceof SqlIdentifier alias) {
                    aliases.put(alias.getSimple().toLowerCase(Locale.ROOT), table.toLowerCase(Locale.ROOT));
                }
                return;
            }
            String table = TableNameSupport.baseTableName(node);
            if (table != null) {
                aliases.put(table.toLowerCase(Locale.ROOT), table.toLowerCase(Locale.ROOT));
            }
        }

        /** Only top-level AND conjuncts: an equality under OR/NOT does not constrain every row. */
        private static void collectEqualities(SqlNode condition, List<SqlBasicCall> equalities) {
            if (condition instanceof SqlBasicCall call) {
                if (call.getKind() == SqlKind.AND) {
                    for (SqlNode operand : call.getOperandList()) {
                        collectEqualities(operand, equalities);
                    }
                } else if (call.getKind() == SqlKind.EQUALS
                        && call.operand(0) instanceof SqlIdentifier
                        && call.operand(1) instanceof SqlIdentifier) {
                    equalities.add(call);
                }
            }
        }
    }

    /** Union-find over {@code alias.column} references linked by equi-join conjuncts. */
    private static final class ColumnClasses {
        private final Map<String, String> parent = new HashMap<>();

        static ColumnClasses of(
                List<SqlBasicCall> equalities, List<Map<String, String>> scopeChain, ClusterCatalog catalog) {
            ColumnClasses classes = new ColumnClasses();
            for (SqlBasicCall equality : equalities) {
                String left = resolve((SqlIdentifier) equality.operand(0), scopeChain, catalog);
                String right = resolve((SqlIdentifier) equality.operand(1), scopeChain, catalog);
                if (left != null && right != null) {
                    classes.union(left, right);
                }
            }
            return classes;
        }

        boolean connectedToAny(String alias, String column, List<String> targetAliases, String targetColumn) {
            String node = key(alias, column);
            for (String target : targetAliases) {
                if (connected(node, key(target, targetColumn))) {
                    return true;
                }
            }
            return false;
        }

        boolean connectedToAnyKey(String alias, String column, Map<String, String> targetKeyByAlias) {
            String node = key(alias, column);
            for (Map.Entry<String, String> target : targetKeyByAlias.entrySet()) {
                if (connected(node, key(target.getKey(), target.getValue()))) {
                    return true;
                }
            }
            return false;
        }

        Optional<String> columnOfAliasConnectedTo(String alias, List<String> targetAliases, String targetColumn) {
            String prefix = alias.toLowerCase(Locale.ROOT) + ".";
            for (String target : targetAliases) {
                String root = find(key(target, targetColumn));
                for (String node : parent.keySet()) {
                    if (node.startsWith(prefix) && find(node).equals(root)) {
                        return Optional.of(node.substring(prefix.length()));
                    }
                }
            }
            return Optional.empty();
        }

        private boolean connected(String left, String right) {
            return parent.containsKey(left) && parent.containsKey(right) && find(left).equals(find(right));
        }

        private void union(String left, String right) {
            String leftRoot = find(left);
            String rightRoot = find(right);
            if (!leftRoot.equals(rightRoot)) {
                parent.put(leftRoot, rightRoot);
            }
        }

        private String find(String node) {
            parent.putIfAbsent(node, node);
            String root = node;
            while (!parent.get(root).equals(root)) {
                root = parent.get(root);
            }
            parent.put(node, root);
            return root;
        }

        private static String key(String alias, String column) {
            return alias.toLowerCase(Locale.ROOT) + "." + column.toLowerCase(Locale.ROOT);
        }

        /**
         * Qualified names resolve by alias; unqualified names resolve to the single table in the
         * innermost scope that has the column (catalog schema, or the shard key when no schema is known).
         */
        private static String resolve(SqlIdentifier identifier, List<Map<String, String>> scopeChain, ClusterCatalog catalog) {
            if (identifier.names.size() >= 2) {
                String qualifier = identifier.names.get(identifier.names.size() - 2).toLowerCase(Locale.ROOT);
                String column = identifier.names.get(identifier.names.size() - 1);
                for (Map<String, String> scope : scopeChain) {
                    if (scope.containsKey(qualifier)) {
                        return key(qualifier, column);
                    }
                }
                return null;
            }
            String column = identifier.getSimple();
            for (Map<String, String> scope : scopeChain) {
                String match = null;
                for (Map.Entry<String, String> entry : scope.entrySet()) {
                    if (tableHasColumn(catalog, entry.getValue(), column)) {
                        if (match != null) {
                            return null;
                        }
                        match = entry.getKey();
                    }
                }
                if (match != null) {
                    return key(match, column);
                }
            }
            return null;
        }
    }
}
