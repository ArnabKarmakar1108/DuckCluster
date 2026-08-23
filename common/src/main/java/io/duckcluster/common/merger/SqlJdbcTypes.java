package io.duckcluster.common.merger;

import io.duckcluster.proto.v1.SqlValueType;

import java.sql.Types;
import java.util.Locale;
import java.util.Set;

/** Maps JDBC column types to merge-time SQL type names and proto enums. */
public final class SqlJdbcTypes {
    /** DuckDB reports these as {@link Types#JAVA_OBJECT}; only the type name identifies them. */
    private static final Set<String> DUCKDB_INTEGER_TYPE_NAMES = Set.of(
            "HUGEINT", "UHUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT");

    private SqlJdbcTypes() {}

    public static SqlValueType toProtoType(int jdbcType) {
        return toProtoType(jdbcType, null);
    }

    public static SqlValueType toProtoType(int jdbcType, String typeName) {
        return switch (jdbcType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> SqlValueType.SQL_BIGINT;
            case Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> SqlValueType.SQL_DOUBLE;
            case Types.BOOLEAN, Types.BIT -> SqlValueType.SQL_BOOLEAN;
            default -> typeName != null && DUCKDB_INTEGER_TYPE_NAMES.contains(typeName.toUpperCase(Locale.ROOT))
                    ? SqlValueType.SQL_BIGINT
                    : SqlValueType.SQL_VARCHAR;
        };
    }

    public static String fromProtoType(SqlValueType type) {
        if (type == null || type == SqlValueType.UNRECOGNIZED || type == SqlValueType.SQL_VALUE_TYPE_UNSPECIFIED) {
            return "VARCHAR";
        }
        return switch (type) {
            case SQL_BIGINT -> "BIGINT";
            case SQL_DOUBLE -> "DOUBLE";
            case SQL_BOOLEAN -> "BOOLEAN";
            default -> "VARCHAR";
        };
    }
}
