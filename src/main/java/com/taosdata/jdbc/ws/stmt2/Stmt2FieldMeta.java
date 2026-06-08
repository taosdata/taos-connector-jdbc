package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.ws.stmt2.entity.Field;

import static com.taosdata.jdbc.TSDBConstants.*;

/**
 * Cached per-field descriptor shared by the columnar serializer and future producers.
 * Captures bind type, TDengine data type, precision, and fixed vs variable-width rules.
 */
public final class Stmt2FieldMeta {

    private final byte bindType;
    private final byte fieldType;
    private final byte precision;
    private final boolean variableWidth;

    private Stmt2FieldMeta(byte bindType, byte fieldType, byte precision) {
        this.bindType = bindType;
        this.fieldType = fieldType;
        this.precision = precision;
        this.variableWidth = isVariableWidth(fieldType);
    }

    public static Stmt2FieldMeta fromField(Field field) {
        return new Stmt2FieldMeta(field.getBindType(), field.getFieldType(), field.getPrecision());
    }

    public static Stmt2FieldMeta of(byte bindType, byte fieldType, byte precision) {
        return new Stmt2FieldMeta(bindType, fieldType, precision);
    }

    /**
     * Returns true for types that carry per-row byte lengths in the column block.
     * Matches the adapter's needLength() function.
     */
    public static boolean isVariableWidth(int fieldType) {
        switch (fieldType) {
            case TSDB_DATA_TYPE_VARCHAR:   // BINARY = 8
            case TSDB_DATA_TYPE_NCHAR:     // 10
            case TSDB_DATA_TYPE_JSON:      // 15
            case TSDB_DATA_TYPE_VARBINARY: // 16
            case TSDB_DATA_TYPE_DECIMAL128:// 17 (DECIMAL in adapter)
            case TSDB_DATA_TYPE_BLOB:      // 18
            case TSDB_DATA_TYPE_GEOMETRY:  // 20
            case TSDB_DATA_TYPE_DECIMAL64: // 21
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns the fixed byte width for numeric/timestamp types.
     * Returns 0 for variable-width types.
     */
    public int fixedWidth() {
        switch (fieldType) {
            case TSDB_DATA_TYPE_BOOL:
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                return 1;
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                return 2;
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_FLOAT:
                return 4;
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
            case TSDB_DATA_TYPE_DOUBLE:
            case TSDB_DATA_TYPE_TIMESTAMP:
                return 8;
            default:
                return 0;
        }
    }

    public byte getBindType() {
        return bindType;
    }

    public byte getFieldType() {
        return fieldType;
    }

    public byte getPrecision() {
        return precision;
    }

    public boolean isVariableWidth() {
        return variableWidth;
    }
}
