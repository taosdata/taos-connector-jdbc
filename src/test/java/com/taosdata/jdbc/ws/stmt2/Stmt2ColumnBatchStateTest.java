package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
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

        rowState.stageTbName("meters");
        rowState.stageFixed(1, 42L);
        rowState.stageVar(2, "alpha".getBytes(StandardCharsets.UTF_8));

        state.flushRow(rowState);

        assertEquals(1, state.getExpectedRowCount());
        assertEquals(1, state.getColumnBuffer(0).getRowCount());
        assertEquals(1, state.getColumnBuffer(1).getRowCount());
        assertEquals(1, state.getColumnBuffer(2).getRowCount());
        assertFalse(rowState.hasPendingValues());
        assertNull(rowState.tableName());
        assertTrue(rowState.isNull(1));
        assertTrue(rowState.isNull(2));

        rowState.stageTbName("meters");
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

        rowState.stageFixed(0, 7L);
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

        rowState.stageFixed(0, 9L);

        SQLException ex = assertThrows(SQLException.class, () -> state.flushRow(rowState));

        assertEquals("Table name not set for row; call setString on the tbname parameter", ex.getMessage());
        assertEquals(0, state.getExpectedRowCount());
        assertEquals(0, state.getColumnBuffer(0).getRowCount());
        assertEquals(0, state.getColumnBuffer(1).getRowCount());
        assertTrue(rowState.hasPendingValues());
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
}
