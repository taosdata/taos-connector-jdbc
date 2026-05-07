package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;

import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.*;

final class Stmt2ColumnBatchState {

    private final Stmt2FieldMeta[] fieldMetas;
    private final int tbNameFieldIdx;
    private Stmt2ColumnFieldBuffer[] columnBuffers;
    private int expectedRowCount;

    Stmt2ColumnBatchState(Stmt2FieldMeta[] fieldMetas) {
        this.fieldMetas = fieldMetas;
        int tbIdx = -1;
        for (int i = 0; i < fieldMetas.length; i++) {
            if (fieldMetas[i].getBindType() == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
                tbIdx = i;
            }
        }
        this.tbNameFieldIdx = tbIdx;
        this.columnBuffers = allocateColumnBuffers();
    }

    void flushRow(Stmt2CurrentRowState rowState) throws SQLException {
        validateRowForFlush(rowState);
        for (int i = 0; i < fieldMetas.length; i++) {
            Stmt2FieldMeta meta = fieldMetas[i];
            Stmt2ColumnFieldBuffer buffer = columnBuffers[i];
            if (meta.getBindType() == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
                buffer.appendTbName(rowState.tableName());
                continue;
            }
            if (rowState.isNull(i)) {
                buffer.appendNull();
            } else if (meta.isVariableWidth()) {
                buffer.appendBytes(rowState.varValue(i));
            } else {
                appendFixedValue(buffer, meta, rowState.fixedValue(i));
            }
        }
        expectedRowCount++;
        rowState.clear();
    }

    private void validateRowForFlush(Stmt2CurrentRowState rowState) throws SQLException {
        if (tbNameFieldIdx >= 0) {
            String tableName = rowState.tableName();
            if (tableName == null || tableName.isEmpty()) {
                throw new SQLException("Table name not set for row; call setString on the tbname parameter");
            }
        }
    }

    void checkRowCounts() throws SQLException {
        for (int i = 0; i < columnBuffers.length; i++) {
            int actual = columnBuffers[i].getRowCount();
            if (actual != expectedRowCount) {
                throw new SQLException(
                        "row count mismatch at column " + i + ": expected " + expectedRowCount + ", got " + actual);
            }
        }
    }

    byte[] buildPayload() throws SQLException {
        return Stmt2ColumnBindSerializer.serialize(columnBuffers);
    }

    void reset() {
        expectedRowCount = 0;
        columnBuffers = allocateColumnBuffers();
    }

    int getExpectedRowCount() {
        return expectedRowCount;
    }

    int getTbNameFieldIdx() {
        return tbNameFieldIdx;
    }

    Stmt2ColumnFieldBuffer getColumnBuffer(int index) {
        return columnBuffers[index];
    }

    private Stmt2ColumnFieldBuffer[] allocateColumnBuffers() {
        Stmt2ColumnFieldBuffer[] buffers = new Stmt2ColumnFieldBuffer[fieldMetas.length];
        for (int i = 0; i < fieldMetas.length; i++) {
            buffers[i] = new Stmt2ColumnFieldBuffer(fieldMetas[i]);
        }
        return buffers;
    }

    private static void appendFixedValue(
            Stmt2ColumnFieldBuffer buffer,
            Stmt2FieldMeta meta,
            long value) throws SQLException {
        switch (meta.getFieldType() & 0xFF) {
            case TSDB_DATA_TYPE_BOOL:
                buffer.appendBool(value != 0);
                break;
            case TSDB_DATA_TYPE_TINYINT:
                buffer.appendTinyInt((byte) value);
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                buffer.appendUTinyInt((byte) value);
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                buffer.appendSmallInt((short) value);
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                buffer.appendUSmallInt((short) value);
                break;
            case TSDB_DATA_TYPE_INT:
                buffer.appendInt((int) value);
                break;
            case TSDB_DATA_TYPE_UINT:
                buffer.appendUInt((int) value);
                break;
            case TSDB_DATA_TYPE_BIGINT:
                buffer.appendBigInt(value);
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                buffer.appendUBigInt(value);
                break;
            case TSDB_DATA_TYPE_FLOAT:
                buffer.appendFloat(Float.intBitsToFloat((int) value));
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                buffer.appendDouble(Double.longBitsToDouble(value));
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                buffer.appendTimestamp(value);
                break;
            default:
                throw new SQLException("Unexpected fixed-width field type: " + meta.getFieldType());
        }
    }
}
