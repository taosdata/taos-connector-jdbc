package com.taosdata.jdbc.enums;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.sql.Types;

import static com.taosdata.jdbc.TSDBConstants.*;

public enum DataType {
    NULL("NULL", Types.NULL, TSDB_DATA_TYPE_NULL),
    BOOL("BOOL", Types.BOOLEAN, TSDB_DATA_TYPE_BOOL),
    TINYINT("TINYINT", Types.TINYINT, TSDB_DATA_TYPE_TINYINT),
    UTINYINT("UTINYINT", Types.TINYINT, TSDB_DATA_TYPE_UTINYINT),
    USMALLINT("USMALLINT", Types.SMALLINT, TSDB_DATA_TYPE_USMALLINT),
    SMALLINT("SMALLINT", Types.SMALLINT, TSDB_DATA_TYPE_SMALLINT),
    UINT("UINT", Types.INTEGER, TSDB_DATA_TYPE_UINT),
    INT("INT", Types.INTEGER, TSDB_DATA_TYPE_INT),
    UBIGINT("UBIGINT", Types.BIGINT, TSDB_DATA_TYPE_UBIGINT),
    BIGINT("BIGINT", Types.BIGINT, TSDB_DATA_TYPE_BIGINT),
    FLOAT("FLOAT", Types.FLOAT, TSDB_DATA_TYPE_FLOAT),
    DOUBLE("DOUBLE", Types.DOUBLE, TSDB_DATA_TYPE_DOUBLE),
    BINARY("BINARY", Types.BINARY, TSDB_DATA_TYPE_BINARY),
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, TSDB_DATA_TYPE_TIMESTAMP),
    NCHAR("NCHAR", Types.NCHAR, TSDB_DATA_TYPE_NCHAR),
    JSON("JSON", Types.OTHER, TSDB_DATA_TYPE_JSON),
    ;

    private final String typeName;
    private final int taosTypeValue;
    private final int jdbcTypeValue;

    DataType(String typeName, int taosTypeValue, int jdbcTypeValue) {
        this.typeName = typeName;
        this.taosTypeValue = taosTypeValue;
        this.jdbcTypeValue = jdbcTypeValue;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getTaosTypeValue() {
        return taosTypeValue;
    }

    public int getJdbcTypeValue() {
        return jdbcTypeValue;
    }

    public static DataType getDataType(String name) {
        if (StringUtils.isEmpty(name))
            return NULL;

        name = name.trim().toUpperCase();
        for (DataType type : DataType.values()) {
            if (name.equals(type.getTypeName())) {
                return type;
            }
        }
        return NULL;
    }

    public static DataType convertJDBC2DataType(int jdbcType) throws SQLException {
        for (DataType type : DataType.values()) {
            if (jdbcType == type.getJdbcTypeValue()) {
                return type;
            }
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE, "unknown sql type: " + jdbcType + " in tdengine");
    }

    public static DataType convertTaosType2DataType(int taosType) throws SQLException {
        for (DataType type : DataType.values()) {
            if (taosType == type.getTaosTypeValue()) {
                return type;
            }
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_TAOS_TYPE, "unknown taos type: " + taosType + " in tdengine");
    }
}
