package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BLOB;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARBINARY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARCHAR;
import static org.junit.Assert.*;

public class Stmt2ColumnBatchStateTest {

    @Test
    public void flushRow_appendsValues_and_advancesExpectedRowCount() throws Exception {
        Stmt2FieldMeta[] fieldMetas = new Stmt2FieldMeta[]{
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0),
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT, (byte) 0),
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0)
        };
        Stmt2ColumnBatchState state = new Stmt2ColumnBatchState(fieldMetas);
        Stmt2CurrentRowState rowState = new Stmt2CurrentRowState(fieldMetas.length);

        assertEquals(0, state.getExpectedRowCount());
        assertEquals(0, state.getTbNameFieldIdx());

        rowState.stageString(0, "meters");
        rowState.stageFixed4(1, 42);
        rowState.stageString(2, "alpha");

        state.flushRow(rowState);

        assertEquals(1, state.getExpectedRowCount());
        assertEquals(1, state.getColumnBuffer(0).getRowCount());
        assertEquals(1, state.getColumnBuffer(1).getRowCount());
        assertEquals(1, state.getColumnBuffer(2).getRowCount());
        assertFalse(rowState.hasPendingValues());
        assertNull(rowState.stringValue(0));
        assertTrue(rowState.isNull(1));
        assertTrue(rowState.isNull(2));

        rowState.stageString(0, "meters");
        rowState.stageNull(1);
        rowState.stageVar(2, null);

        state.flushRow(rowState);
        state.checkRowCounts();

        assertEquals(2, state.getExpectedRowCount());
        assertEquals(1, state.getColumnBuffer(0).computeTableCount());
        assertArrayEquals(
                Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{
                        state.getColumnBuffer(0),
                        state.getColumnBuffer(1),
                        state.getColumnBuffer(2)
                }),
                state.buildPayload());

        Stmt2ColumnFieldBuffer bufferBeforeReset = state.getColumnBuffer(1);

        state.reset();

        assertEquals(0, state.getExpectedRowCount());
        assertNotSame(bufferBeforeReset, state.getColumnBuffer(1));
        assertEquals(0, state.getColumnBuffer(0).getRowCount());
        assertEquals(0, state.getColumnBuffer(1).getRowCount());
        assertEquals(0, state.getColumnBuffer(2).getRowCount());
    }

    @Test
    public void checkRowCounts_throwsWhenAnyColumnLags() throws Exception {
        Stmt2FieldMeta[] fieldMetas = new Stmt2FieldMeta[]{
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT, (byte) 0),
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0)
        };
        Stmt2ColumnBatchState state = new Stmt2ColumnBatchState(fieldMetas);
        Stmt2CurrentRowState rowState = new Stmt2CurrentRowState(fieldMetas.length);

        rowState.stageFixed4(0, 7);
        rowState.stageVar(1, "x".getBytes(StandardCharsets.UTF_8));
        state.flushRow(rowState);

        replaceColumnBuffer(state, 1, new Stmt2ColumnFieldBuffer(fieldMetas[1]));

        SQLException ex = assertThrows(SQLException.class, state::checkRowCounts);
        assertEquals("row count mismatch at column 1: expected 1, got 0", ex.getMessage());
    }

    @Test
    public void flushRow_missingTbName_doesNotPartiallyAppendBuffers() {
        Stmt2FieldMeta[] fieldMetas = new Stmt2FieldMeta[]{
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT, (byte) 0),
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0)
        };
        Stmt2ColumnBatchState state = new Stmt2ColumnBatchState(fieldMetas);
        Stmt2CurrentRowState rowState = new Stmt2CurrentRowState(fieldMetas.length);

        assertEquals(-1, new Stmt2ColumnBatchState(new Stmt2FieldMeta[]{
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT, (byte) 0)
        }).getTbNameFieldIdx());

        rowState.stageFixed4(0, 9);

        SQLException ex = assertThrows(SQLException.class, () -> state.flushRow(rowState));

        assertEquals("Table name not set for row; call setString on the tbname parameter", ex.getMessage());
        assertEquals(0, state.getExpectedRowCount());
        assertEquals(0, state.getColumnBuffer(0).getRowCount());
        assertEquals(0, state.getColumnBuffer(1).getRowCount());
        assertTrue(rowState.hasPendingValues());
    }

    @Test
    public void flushRow_emptyTbName_doesNotPartiallyAppendBuffers() {
        Stmt2FieldMeta[] fieldMetas = new Stmt2FieldMeta[]{
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT, (byte) 0),
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0)
        };
        Stmt2ColumnBatchState state = new Stmt2ColumnBatchState(fieldMetas);
        Stmt2CurrentRowState rowState = new Stmt2CurrentRowState(fieldMetas.length);

        rowState.stageFixed4(0, 11);
        rowState.stageString(1, "");

        assertThrows(SQLException.class, () -> state.flushRow(rowState));
        assertEquals(0, state.getExpectedRowCount());
        assertEquals(0, state.getColumnBuffer(0).getRowCount());
        assertEquals(0, state.getColumnBuffer(1).getRowCount());
        assertTrue(rowState.hasPendingValues());
    }

    @Test
    public void reset_reusesPreviousValueBufferSizeAsNextInitialSize() throws Exception {
        Stmt2FieldMeta[] fieldMetas = new Stmt2FieldMeta[]{
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0)
        };
        Stmt2ColumnBatchState state = new Stmt2ColumnBatchState(fieldMetas);
        Stmt2CurrentRowState rowState = new Stmt2CurrentRowState(fieldMetas.length);
        String largeValue = String.join("", Collections.nCopies(20_000, "a"));

        rowState.stageString(0, largeValue);
        state.flushRow(rowState);

        int usedValueBytes = getAutoExpandingBufferReadableBytes(state.getColumnBuffer(0), "valueBuffer");
        assertTrue(usedValueBytes >= 20_000);

        state.reset();

        int nextInitialSize = getAutoExpandingBufferBufferSize(state.getColumnBuffer(0), "valueBuffer");
        assertTrue("reset should carry previous batch usage into next initial size", nextInitialSize >= usedValueBytes);
    }

    @Test
    public void flushRow_usesEncodedVarBytesForBytesBackedColumns() throws Exception {
        Stmt2FieldMeta[] fieldMetas = new Stmt2FieldMeta[]{
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_VARBINARY, (byte) 0),
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_BLOB, (byte) 0)
        };
        Stmt2ColumnBatchState state = new Stmt2ColumnBatchState(fieldMetas);
        Stmt2CurrentRowState rowState = new Stmt2CurrentRowState(fieldMetas.length);

        byte[] tb = "meters".getBytes(StandardCharsets.UTF_8);
        byte[] value = "alpha".getBytes(StandardCharsets.UTF_8);
        rowState.stageVar(0, tb, tb.length);
        rowState.stageVar(1, value, value.length);

        state.flushRow(rowState);

        assertEquals(1, state.getExpectedRowCount());
        assertEquals(1, state.getColumnBuffer(0).getRowCount());
        assertEquals(1, state.getColumnBuffer(1).getRowCount());
    }

    private static void replaceColumnBuffer(
            Stmt2ColumnBatchState state,
            int index,
            Stmt2ColumnFieldBuffer buffer) throws Exception {
        Field field = Stmt2ColumnBatchState.class.getDeclaredField("columnBuffers");
        field.setAccessible(true);
        Stmt2ColumnFieldBuffer[] columnBuffers = (Stmt2ColumnFieldBuffer[]) field.get(state);
        columnBuffers[index] = buffer;
    }

    private static int getAutoExpandingBufferReadableBytes(
            Stmt2ColumnFieldBuffer buffer,
            String fieldName) throws Exception {
        Object autoBuffer = getDeclaredField(buffer, fieldName);
        return (Integer) autoBuffer.getClass().getDeclaredMethod("readableBytes").invoke(autoBuffer);
    }

    private static int getAutoExpandingBufferBufferSize(
            Stmt2ColumnFieldBuffer buffer,
            String fieldName) throws Exception {
        Object autoBuffer = getDeclaredField(buffer, fieldName);
        Field bufferSize = autoBuffer.getClass().getDeclaredField("bufferSize");
        bufferSize.setAccessible(true);
        return bufferSize.getInt(autoBuffer);
    }

    private static Object getDeclaredField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
