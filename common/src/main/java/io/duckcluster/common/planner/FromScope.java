package io.duckcluster.common.planner;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Table names and aliases visible in a FROM clause. */
final class FromScope {
    private final Set<String> tableNames;
    private final Map<String, String> aliasToTableName;

    FromScope(Set<String> tableNames, Map<String, String> aliasToTableName) {
        this.tableNames = tableNames;
        this.aliasToTableName = aliasToTableName;
    }

    static FromScope of(SqlNode from) {
        Map<String, String> aliases = new LinkedHashMap<>();
        Set<String> tables = new LinkedHashSet<>();
        if (from != null) {
            collect(from, aliases, tables);
        }
        return new FromScope(Set.copyOf(tables), Map.copyOf(aliases));
    }

    static FromScope ofTables(Collection<String> tableNames) {
        Map<String, String> aliases = new LinkedHashMap<>();
        Set<String> tables = new LinkedHashSet<>();
        for (String tableName : tableNames) {
            String lower = tableName.toLowerCase();
            tables.add(lower);
            aliases.put(lower, lower);
        }
        return new FromScope(Set.copyOf(tables), Map.copyOf(aliases));
    }

    Set<String> tableNames() {
        return tableNames;
    }

    boolean hasAlias(String alias) {
        return aliasToTableName.containsKey(alias.toLowerCase());
    }

    boolean referencesOuterAlias(SqlIdentifier identifier, FromScope inner) {
        if (identifier.names.size() < 2) {
            return false;
        }
        String qualifier = identifier.names.get(0).toLowerCase();
        return aliasToTableName.containsKey(qualifier) && !inner.aliasToTableName.containsKey(qualifier);
    }

    FromScope union(FromScope other) {
        Map<String, String> mergedAliases = new LinkedHashMap<>(aliasToTableName);
        mergedAliases.putAll(other.aliasToTableName);
        Set<String> mergedTables = new LinkedHashSet<>(tableNames);
        mergedTables.addAll(other.tableNames);
        return new FromScope(Set.copyOf(mergedTables), Map.copyOf(mergedAliases));
    }

    private static void collect(SqlNode node, Map<String, String> aliases, Set<String> tables) {
        String tableName = TableNameSupport.baseTableName(node);
        if (tableName != null) {
            String lower = tableName.toLowerCase();
            tables.add(lower);
            aliases.put(aliasOf(node).toLowerCase(), lower);
            return;
        }
        if (node instanceof SqlJoin join) {
            collect(join.getLeft(), aliases, tables);
            collect(join.getRight(), aliases, tables);
            return;
        }
        if (node instanceof org.apache.calcite.sql.SqlSelect select) {
            if (select.getFrom() != null) {
                collect(select.getFrom(), aliases, tables);
            }
            return;
        }
        if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
            collect(call.operand(0), aliases, tables);
            String innerTable = TableNameSupport.baseTableName(call.operand(0));
            if (innerTable != null && call.operand(1) instanceof SqlIdentifier aliasNode) {
                aliases.put(aliasNode.getSimple().toLowerCase(), innerTable.toLowerCase());
            }
        }
    }

    private static String aliasOf(SqlNode node) {
        if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
            if (call.operand(1) instanceof SqlIdentifier alias) {
                return alias.getSimple();
            }
        }
        String tableName = TableNameSupport.baseTableName(node);
        return tableName != null ? tableName : "unknown";
    }
}
