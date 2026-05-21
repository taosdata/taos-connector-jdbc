package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;

import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.*;

final class Stmt2RowFlushPlan {

    private enum SlotKind {
        UTF8_VAR,
        BYTES_VAR,
        BOOL,
        TINYINT,
        UTINYINT,
        SMALLINT,
        USMALLINT,
        INT,
        UINT,
        FLOAT,
        BIGINT,
        UBIGINT,
        DOUBLE,
        TIMESTAMP
    }

    private final int[] utf8VarIndexes;
    private final int[] bytesVarIndexes;
    private final int[] boolIndexes;
    private final int[] tinyIntIndexes;
    private final int[] uTinyIntIndexes;
    private final int[] smallIntIndexes;
    private final int[] uSmallIntIndexes;
    private final int[] intIndexes;
    private final int[] uintIndexes;
    private final int[] floatIndexes;
    private final int[] bigIntIndexes;
    private final int[] uBigIntIndexes;
    private final int[] doubleIndexes;
    private final int[] timestampIndexes;

    Stmt2RowFlushPlan(Stmt2FieldMeta[] fieldMetas) {
        int[] counts = new int[SlotKind.values().length];
        for (Stmt2FieldMeta meta : fieldMetas) {
            counts[slotKind(meta).ordinal()]++;
        }

        int[][] indexesByKind = new int[SlotKind.values().length][];
        for (SlotKind kind : SlotKind.values()) {
            indexesByKind[kind.ordinal()] = new int[counts[kind.ordinal()]];
        }

        int[] positions = new int[SlotKind.values().length];
        for (int i = 0; i < fieldMetas.length; i++) {
            SlotKind kind = slotKind(fieldMetas[i]);
            indexesByKind[kind.ordinal()][positions[kind.ordinal()]++] = i;
        }

        this.utf8VarIndexes = indexesByKind[SlotKind.UTF8_VAR.ordinal()];
        this.bytesVarIndexes = indexesByKind[SlotKind.BYTES_VAR.ordinal()];
        this.boolIndexes = indexesByKind[SlotKind.BOOL.ordinal()];
        this.tinyIntIndexes = indexesByKind[SlotKind.TINYINT.ordinal()];
        this.uTinyIntIndexes = indexesByKind[SlotKind.UTINYINT.ordinal()];
        this.smallIntIndexes = indexesByKind[SlotKind.SMALLINT.ordinal()];
        this.uSmallIntIndexes = indexesByKind[SlotKind.USMALLINT.ordinal()];
        this.intIndexes = indexesByKind[SlotKind.INT.ordinal()];
        this.uintIndexes = indexesByKind[SlotKind.UINT.ordinal()];
        this.floatIndexes = indexesByKind[SlotKind.FLOAT.ordinal()];
        this.bigIntIndexes = indexesByKind[SlotKind.BIGINT.ordinal()];
        this.uBigIntIndexes = indexesByKind[SlotKind.UBIGINT.ordinal()];
        this.doubleIndexes = indexesByKind[SlotKind.DOUBLE.ordinal()];
        this.timestampIndexes = indexesByKind[SlotKind.TIMESTAMP.ordinal()];
    }

    void flushRow(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        flushUtf8VariableWidth(rowState, columnBuffers, utf8VarIndexes);
        flushBytesVariableWidth(rowState, columnBuffers, bytesVarIndexes);
        flushBool(rowState, columnBuffers);
        flushTinyInt(rowState, columnBuffers);
        flushUTinyInt(rowState, columnBuffers);
        flushSmallInt(rowState, columnBuffers);
        flushUSmallInt(rowState, columnBuffers);
        flushInt(rowState, columnBuffers);
        flushUInt(rowState, columnBuffers);
        flushFloat(rowState, columnBuffers);
        flushBigInt(rowState, columnBuffers);
        flushUBigInt(rowState, columnBuffers);
        flushDouble(rowState, columnBuffers);
        flushTimestamp(rowState, columnBuffers);
    }

    private static SlotKind slotKind(Stmt2FieldMeta meta) {
        switch (meta.getFieldType() & 0xFF) {
            case TSDB_DATA_TYPE_VARCHAR:
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
                return SlotKind.UTF8_VAR;
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_DECIMAL128:
            case TSDB_DATA_TYPE_BLOB:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_DECIMAL64:
                return SlotKind.BYTES_VAR;
            case TSDB_DATA_TYPE_BOOL:
                return SlotKind.BOOL;
            case TSDB_DATA_TYPE_TINYINT:
                return SlotKind.TINYINT;
            case TSDB_DATA_TYPE_UTINYINT:
                return SlotKind.UTINYINT;
            case TSDB_DATA_TYPE_SMALLINT:
                return SlotKind.SMALLINT;
            case TSDB_DATA_TYPE_USMALLINT:
                return SlotKind.USMALLINT;
            case TSDB_DATA_TYPE_INT:
                return SlotKind.INT;
            case TSDB_DATA_TYPE_UINT:
                return SlotKind.UINT;
            case TSDB_DATA_TYPE_FLOAT:
                return SlotKind.FLOAT;
            case TSDB_DATA_TYPE_BIGINT:
                return SlotKind.BIGINT;
            case TSDB_DATA_TYPE_UBIGINT:
                return SlotKind.UBIGINT;
            case TSDB_DATA_TYPE_DOUBLE:
                return SlotKind.DOUBLE;
            case TSDB_DATA_TYPE_TIMESTAMP:
                return SlotKind.TIMESTAMP;
            default:
                throw new IllegalArgumentException("Unsupported field type for flush plan: " + meta.getFieldType());
        }
    }

    private static void flushUtf8VariableWidth(
            Stmt2CurrentRowState rowState,
            Stmt2ColumnFieldBuffer[] columnBuffers,
            int[] indexes) throws SQLException {
        for (int index : indexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
                continue;
            }
            String stringValue = rowState.stringValue(index);
            if (stringValue != null) {
                buffer.appendString(stringValue);
            } else {
                buffer.appendEncodedVar(rowState.varValue(index), rowState.varLength(index));
            }
        }
    }

    private static void flushBytesVariableWidth(
            Stmt2CurrentRowState rowState,
            Stmt2ColumnFieldBuffer[] columnBuffers,
            int[] indexes) throws SQLException {
        for (int index : indexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
                continue;
            }
            buffer.appendEncodedVar(rowState.varValue(index), rowState.varLength(index));
        }
    }

    private void flushBool(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : boolIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendBool(rowState.fixed1Value(index) != 0);
            }
        }
    }

    private void flushTinyInt(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : tinyIntIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendTinyInt(rowState.fixed1Value(index));
            }
        }
    }

    private void flushUTinyInt(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : uTinyIntIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendUTinyInt(rowState.fixed1Value(index));
            }
        }
    }

    private void flushSmallInt(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : smallIntIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendSmallInt(rowState.fixed2Value(index));
            }
        }
    }

    private void flushUSmallInt(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : uSmallIntIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendUSmallInt(rowState.fixed2Value(index));
            }
        }
    }

    private void flushInt(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : intIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendInt(rowState.fixed4Value(index));
            }
        }
    }

    private void flushUInt(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : uintIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendUInt(rowState.fixed4Value(index));
            }
        }
    }

    private void flushFloat(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : floatIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendFixed4Raw(rowState.fixed4Value(index));
            }
        }
    }

    private void flushBigInt(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : bigIntIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendBigInt(rowState.fixed8Value(index));
            }
        }
    }

    private void flushUBigInt(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : uBigIntIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendUBigInt(rowState.fixed8Value(index));
            }
        }
    }

    private void flushDouble(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : doubleIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendFixed8Raw(rowState.fixed8Value(index));
            }
        }
    }

    private void flushTimestamp(Stmt2CurrentRowState rowState, Stmt2ColumnFieldBuffer[] columnBuffers) throws SQLException {
        for (int index : timestampIndexes) {
            Stmt2ColumnFieldBuffer buffer = columnBuffers[index];
            if (rowState.isNull(index)) {
                buffer.appendNull();
            } else {
                buffer.appendTimestamp(rowState.fixed8Value(index));
            }
        }
    }
}
