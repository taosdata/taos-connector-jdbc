package com.taosdata.jdbc.enums;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.sql.Types;

import static com.taosdata.jdbc.TSDBConstants.*;

public enum DataType {
    NULL("NULL", Types.NULL, TSDB_DATA_TYPE_NULL, 0),
    BOOL("BOOL", Types.BOOLEAN, TSDB_DATA_TYPE_BOOL, BOOLEAN_PRECISION),
    TINYINT("TINYINT", Types.TINYINT, TSDB_DATA_TYPE_TINYINT, TINYINT_PRECISION),
    UTINYINT("TINYINT UNSIGNED", Types.TINYINT, TSDB_DATA_TYPE_UTINYINT, TINYINT_PRECISION),
    USMALLINT("SMALLINT UNSIGNED", Types.SMALLINT, TSDB_DATA_TYPE_USMALLINT, SMALLINT_PRECISION),
    SMALLINT("SMALLINT", Types.SMALLINT, TSDB_DATA_TYPE_SMALLINT, SMALLINT_PRECISION),
    UINT("INT UNSIGNED", Types.INTEGER, TSDB_DATA_TYPE_UINT, INT_PRECISION),
    INT("INT", Types.INTEGER, TSDB_DATA_TYPE_INT, INT_PRECISION),
    UBIGINT("BIGINT UNSIGNED", Types.BIGINT, TSDB_DATA_TYPE_UBIGINT, BIGINT_PRECISION),
    BIGINT("BIGINT", Types.BIGINT, TSDB_DATA_TYPE_BIGINT, BIGINT_PRECISION),
    FLOAT("FLOAT", Types.FLOAT, TSDB_DATA_TYPE_FLOAT, FLOAT_PRECISION),
    DOUBLE("DOUBLE", Types.DOUBLE, TSDB_DATA_TYPE_DOUBLE, DOUBLE_PRECISION),
    BINARY("BINARY", Types.BINARY, TSDB_DATA_TYPE_BINARY, 0),
    VARCHAR("VARCHAR", Types.VARCHAR, TSDB_DATA_TYPE_VARCHAR, 0),
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, TSDB_DATA_TYPE_TIMESTAMP, 0),
    NCHAR("NCHAR", Types.NCHAR, TSDB_DATA_TYPE_NCHAR, 0),
    JSON("JSON", Types.OTHER, TSDB_DATA_TYPE_JSON, 0),
    VARBINARY("VARBINARY", Types.VARBINARY, TSDB_DATA_TYPE_VARBINARY, 0),
    ;

    private final String typeName;
    private final int jdbcTypeValue;
    private final int taosTypeValue;
    private final int size;

    DataType(String typeName, int jdbcTypeValue, int taosTypeValue, int size) {
        this.typeName = typeName;
        this.jdbcTypeValue = jdbcTypeValue;
        this.taosTypeValue = taosTypeValue;
        this.size = size;
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

    public int getSize() {
        return size;
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

    public static int calculateColumnSize(String typeName, String precisionType, int length) {
        if (StringUtils.isEmpty(typeName))
            return -1;
        DataType tmp = null;
        typeName = typeName.trim().toUpperCase();
        for (DataType type : DataType.values()) {
            if (typeName.equals(type.getTypeName())) {
                tmp = type;
            }
        }
        if (null == tmp) {
            return -1;
        }
        if (0 == tmp.getSize()) {
            if (tmp == TIMESTAMP) {
                return precisionType.equals("ms") ? TIMESTAMP_MS_PRECISION : TIMESTAMP_US_PRECISION;
            } else if (tmp == NCHAR || tmp == BINARY || tmp == VARCHAR || tmp == VARBINARY) {
                return length;
            }
        }
        return tmp.getSize();
    }

    public static Integer calculateDecimalDigits(String typeName) {
        switch (typeName) {
            case "TINYINT":
            case "TINYINT UNSIGNED":
            case "SMALLINT":
            case "SMALLINT UNSIGNED":
            case "INT":
            case "INT UNSIGNED":
            case "BIGINT":
            case "BIGINT UNSIGNED":
                return 0;
            case "FLOAT":
                return 5;
            case "DOUBLE":
                return 16;
            default:
                return null;
        }
    }
}
