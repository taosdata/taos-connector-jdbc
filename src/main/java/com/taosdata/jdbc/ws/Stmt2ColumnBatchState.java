package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.sql.SQLException;
import java.util.Arrays;

final class Stmt2ColumnBatchState {

    private final Stmt2FieldMeta[] fieldMetas;
    private final int tbNameFieldIdx;
    private final Stmt2RowFlushPlan rowFlushPlan;
    private Stmt2ColumnFieldBuffer[] columnBuffers;
    private Stmt2ColumnFieldBuffer.BufferSizeHints[] bufferSizeHints;
    private int expectedRowCount;
    private int tableCount;
    private String lastTableName;
    private byte[] lastTableNameBytes;
    private int lastTableNameLength;

    Stmt2ColumnBatchState(Stmt2FieldMeta[] fieldMetas) {
        this.fieldMetas = fieldMetas;
        int tbIdx = -1;
        for (int i = 0; i < fieldMetas.length; i++) {
            if (fieldMetas[i].getBindType() == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
                tbIdx = i;
            }
        }
        this.tbNameFieldIdx = tbIdx;
        this.rowFlushPlan = new Stmt2RowFlushPlan(fieldMetas);
        this.columnBuffers = allocateColumnBuffers();
    }

    void flushRow(Stmt2CurrentRowState rowState) throws SQLException {
        validateRowForFlush(rowState);
        if (tbNameFieldIdx >= 0) {
            String tableName = resolveTableNameString(rowState);
            if (tableName != null) {
                updateTableCount(tableName);
            } else {
                updateTableCount(resolveTableNameBytes(rowState), resolveTableNameLength(rowState));
            }
        }
        rowFlushPlan.flushRow(rowState, columnBuffers);
        expectedRowCount++;
        rowState.clear();
    }

    private void validateRowForFlush(Stmt2CurrentRowState rowState) throws SQLException {
        if (tbNameFieldIdx >= 0) {
            String tableName = resolveTableNameString(rowState);
            if (tableName != null) {
                if (tableName.isEmpty()) {
                    throw new SQLException("Table name not set for row; call setString on the tbname parameter");
                }
                return;
            }
            byte[] tableNameBytes = resolveTableNameBytes(rowState);
            if (tableNameBytes == null || resolveTableNameLength(rowState) <= 0) {
                throw new SQLException("Table name not set for row; call setString on the tbname parameter");
            }
        }
    }

    private String resolveTableNameString(Stmt2CurrentRowState rowState) {
        return tbNameFieldIdx >= 0 ? rowState.stringValue(tbNameFieldIdx) : null;
    }

    private byte[] resolveTableNameBytes(Stmt2CurrentRowState rowState) {
        return tbNameFieldIdx >= 0 ? rowState.varValue(tbNameFieldIdx) : null;
    }

    private int resolveTableNameLength(Stmt2CurrentRowState rowState) {
        return tbNameFieldIdx >= 0 ? rowState.varLength(tbNameFieldIdx) : 0;
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
        ByteBuf payload = buildPayloadBuffer();
        try {
            return ByteBufUtil.getBytes(payload);
        } finally {
            Utils.releaseByteBuf(payload);
        }
    }

    ByteBuf buildPayloadBuffer() throws SQLException {
        return Stmt2ColumnBindSerializer.serializeBuffer(columnBuffers, resolvedTableCount());
    }

    void reset() {
        bufferSizeHints = snapshotBufferSizeHints();
        releaseColumnBuffers();
        expectedRowCount = 0;
        tableCount = 0;
        lastTableName = null;
        lastTableNameBytes = null;
        lastTableNameLength = 0;
        columnBuffers = allocateColumnBuffers();
    }

    void release() {
        releaseColumnBuffers();
        expectedRowCount = 0;
        tableCount = 0;
        lastTableName = null;
        lastTableNameBytes = null;
        lastTableNameLength = 0;
        columnBuffers = null;
        bufferSizeHints = null;
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
            buffers[i] = new Stmt2ColumnFieldBuffer(
                    fieldMetas[i],
                    bufferSizeHints == null ? null : bufferSizeHints[i]);
        }
        return buffers;
    }

    private int resolvedTableCount() {
        return tbNameFieldIdx >= 0 ? tableCount : 1;
    }

    private void updateTableCount(String tableName) {
        if (!tableName.equals(lastTableName)) {
            tableCount++;
            lastTableName = tableName;
            lastTableNameBytes = null;
            lastTableNameLength = 0;
        }
    }

    private void updateTableCount(byte[] tableNameBytes, int tableNameLength) {
        if (!sameBytes(lastTableNameBytes, lastTableNameLength, tableNameBytes, tableNameLength)) {
            tableCount++;
            lastTableNameBytes = copyPrefix(tableNameBytes, tableNameLength);
            lastTableNameLength = tableNameLength;
            lastTableName = null;
        }
    }

    private void releaseColumnBuffers() {
        if (columnBuffers == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer columnBuffer : columnBuffers) {
            if (columnBuffer != null) {
                columnBuffer.release();
            }
        }
    }

    private Stmt2ColumnFieldBuffer.BufferSizeHints[] snapshotBufferSizeHints() {
        if (columnBuffers == null) {
            return null;
        }
        Stmt2ColumnFieldBuffer.BufferSizeHints[] hints =
                new Stmt2ColumnFieldBuffer.BufferSizeHints[columnBuffers.length];
        for (int i = 0; i < columnBuffers.length; i++) {
            hints[i] = columnBuffers[i].snapshotUsage();
        }
        return hints;
    }

    private static byte[] copyPrefix(byte[] src, int len) {
        return Arrays.copyOf(src, len);
    }

    private static boolean sameBytes(byte[] left, int leftLength, byte[] right, int rightLength) {
        if (left == null || right == null) {
            return false;
        }
        if (leftLength != rightLength) {
            return false;
        }
        for (int i = 0; i < leftLength; i++) {
            if (left[i] != right[i]) {
                return false;
            }
        }
        return true;
    }
}
