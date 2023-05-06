package com.taosdata.jdbc.enums;

public enum DataLength {
    TSDB_DATA_TYPE_NULL(1),
    TSDB_DATA_TYPE_BOOL(1),
    TSDB_DATA_TYPE_TINYINT(1),
    TSDB_DATA_TYPE_SMALLINT(2),
    TSDB_DATA_TYPE_INT(4),
    TSDB_DATA_TYPE_BIGINT(8),
    TSDB_DATA_TYPE_FLOAT(4),
    TSDB_DATA_TYPE_DOUBLE(8),
    TSDB_DATA_TYPE_TIMESTAMP(8),
    TSDB_DATA_TYPE_UTINYINT(1),
    TSDB_DATA_TYPE_USMALLINT(2),
    TSDB_DATA_TYPE_UINT(4),
    TSDB_DATA_TYPE_UBIGINT(8),
    ;

    private final int length;

    DataLength(int length) {
        this.length = length;
    }

    public int getLength() {
        return length;
    }
}
