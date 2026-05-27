package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import com.taosdata.jdbc.ws.stmt2.WSEWChunkSizingUtil;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UTINYINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WSColumnFastPreparedStatementTest {

    private Transport transport;
    private ConnectionParam param;
    private WSConnection connection;

    @Before
    public void setUp() {
        transport = Mockito.mock(Transport.class);
        param = Mockito.mock(ConnectionParam.class);
        connection = Mockito.mock(WSConnection.class);

        Mockito.when(transport.getConnectionParam()).thenReturn(param);
        Mockito.when(transport.getReconnectCount()).thenReturn(0);
        Mockito.when(param.getRequestTimeout()).thenReturn(30_000);
        Mockito.when(param.getZoneId()).thenReturn(null);
        Mockito.when(connection.supportsStmt2BindExec()).thenReturn(true);
    }

    @Test
    public void standaloneFastStatement_extendsWSRetryableStmtDirectly() {
        assertEquals(WSRetryableStmt.class, WSColumnFastPreparedStatement.class.getSuperclass());
    }

    @Test
    public void executeBatch_rowCountMismatch_resetsStateAndThrows() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_VARCHAR));

        stmt.setInt(1, 7);
        stmt.addBatch();

        SQLException ex = assertSqlException(stmt::executeBatch);
        assertTrue(ex.getMessage().contains("row count mismatch"));
        assertEquals(0, stmt.getExpectedRowCount());
        assertEquals(0, stmt.getColumnBuffer(0).getRowCount());
        assertEquals(0, stmt.getColumnBuffer(1).getRowCount());
    }

    @Test
    public void executeUpdate_requiresExactlyOneRow() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(java.util.Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        stmt.setInt(1, 1);
        stmt.setInt(1, 2);

        SQLException ex = assertSqlException(stmt::executeUpdate);
        assertTrue(ex.getMessage().contains("row count mismatch"));
    }

    @Test
    public void executeBatch_bindExecTransportConsumesRequestBuffer_withoutDoubleRelease() throws Exception {
        stubBindExecTransportConsumesRequestBuffer(1);
        WSColumnFastPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_INT));

        stmt.setString(1, "value");
        stmt.setInt(2, 7);
        stmt.addBatch();
        assertTrue(getBatchStats(stmt)[0].getObservedValueBytes() > 0);

        int[] result = stmt.executeBatch();

        assertEquals(1, result.length);
        assertEquals(java.sql.Statement.SUCCESS_NO_INFO, result[0]);
        assertEquals(0, getBatchStats(stmt)[0].getObservedValueBytes());
        assertEquals(0, getBatchStats(stmt)[0].getRowsWritten());
    }

    @Test
    public void constructor_enablesBindExecWritePathForCapableConnection() {
        WSColumnFastPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        assertTrue(stmt.isUsingBindExec());
    }

    @Test
    public void clearBatch_reusesColumnBuffersAndResetsCounter() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(java.util.Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        stmt.setInt(1, 1);
        stmt.addBatch();
        Stmt2ColumnFieldBuffer[] beforeClear = getColumnBuffers(stmt);
        Stmt2ColumnFieldBuffer beforeBuffer = beforeClear[0];

        stmt.clearBatch();

        assertEquals(0, stmt.getExpectedRowCount());
        Stmt2ColumnFieldBuffer[] afterClear = getColumnBuffers(stmt);
        assertEquals(beforeClear.length, afterClear.length);
        assertEquals(beforeBuffer, afterClear[0]);
        assertEquals(0, afterClear[0].getRowCount());
    }

    @Test
    public void clearBatch_keepsVarWidthReusableBufferInstance() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));

        stmt.setString(1, "first-value");
        stmt.addBatch();

        Stmt2ColumnFieldBuffer[] beforeClear = getColumnBuffers(stmt);
        Stmt2ColumnFieldBuffer beforeBuffer = beforeClear[0];

        stmt.clearBatch();

        Stmt2ColumnFieldBuffer[] afterClear = getColumnBuffers(stmt);
        assertEquals(beforeBuffer, afterClear[0]);
        assertEquals(0, afterClear[0].getRowCount());
        assertTrue(afterClear[0].currentReusableSpec() != null);
    }

    @Test
    public void clearBatch_recreatesVarWidthBufferWhenSpecChanges() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));

        stmt.setString(1, "first-value");
        stmt.addBatch();

        Stmt2ColumnFieldBuffer beforeBuffer = getColumnBuffers(stmt)[0];
        setNextBufferSpec(stmt, 0, new WSEWChunkSizingUtil.BufferSpec(16 * 1024, 2));

        stmt.clearBatch();

        Stmt2ColumnFieldBuffer afterBuffer = getColumnBuffers(stmt)[0];
        assertTrue(beforeBuffer != afterBuffer);
        assertEquals(0, afterBuffer.getRowCount());
        assertEquals(16 * 1024, afterBuffer.currentReusableSpec().getChunkBytes());
        assertEquals(2, afterBuffer.currentReusableSpec().getReusableChunkCount());
    }

    @Test
    public void constructor_initializesBatchStatsScaffolding() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_VARCHAR));

        WSEWChunkSizingUtil.FieldBatchStats[] stats = getBatchStats(stmt);

        assertEquals(2, stats.length);
        assertTrue(stats[0] != null);
        assertTrue(stats[1] != null);
    }

    @Test
    public void executeBatch_varWidthOverflowUpdatesNextSpecForFollowingBatch() throws Exception {
        stubBindExecTransportConsumesRequestBuffer(1);
        WSColumnFastPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));
        String largeValue = String.join("", Collections.nCopies(50_000, "x"));

        stmt.setString(1, largeValue);
        stmt.addBatch();
        stmt.executeBatch();

        WSEWChunkSizingUtil.BufferSpec nextSpec = getNextBufferSpecs(stmt)[0];
        assertTrue(nextSpec.getChunkBytes() > 8 * 1024);
    }

    @Test
    public void updateNextBufferSpecsAfterSuccessfulBatch_ignoresSizingOverflowWithoutPartialStateUpdate()
            throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARCHAR));
        WSEWChunkSizingUtil.FieldBatchStats[] stats = getBatchStats(stmt);

        stats[0].recordValueBytes(50L * 1024, 1024, 1);
        stats[0].setActiveChunksUsed(9);
        stats[1].recordValueBytes(1L, 1, 1);
        setColumnBuffer(stmt, 1, newBufferWithoutReusableSpecButOverflowing());
        setNextBufferSpec(stmt, 1, new WSEWChunkSizingUtil.BufferSpec(Integer.MAX_VALUE, 1));
        setUnderuseStreak(stmt, 0, 11);
        setUnderuseStreak(stmt, 1, 13);

        invokeUpdateNextBufferSpecsAfterSuccessfulBatch(stmt);

        WSEWChunkSizingUtil.BufferSpec[] nextSpecs = getNextBufferSpecs(stmt);
        assertNull(nextSpecs[0]);
        assertEquals(Integer.MAX_VALUE, nextSpecs[1].getChunkBytes());
        assertEquals(11, getUnderuseStreak(stmt, 0));
        assertEquals(13, getUnderuseStreak(stmt, 1));
    }

    @Test
    public void clearBatch_resetsVarWidthBatchStats() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));

        stmt.setString(1, "first-value");
        stmt.addBatch();
        assertTrue(getBatchStats(stmt)[0].getObservedValueBytes() > 0);

        stmt.clearBatch();

        assertEquals(0, getBatchStats(stmt)[0].getObservedValueBytes());
        assertEquals(0, getBatchStats(stmt)[0].getRowsWritten());
    }

    @Test
    public void setString_tbnameRejectsNull() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException nullEx = assertSqlException(() -> stmt.setString(1, null));
        assertEquals(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, nullEx.getErrorCode());
        assertTrue(nullEx.getMessage().contains("table name can't be null"));
        assertTrue(nullEx.getMessage().startsWith("ERROR (0x2303)"));
    }

    @Test
    public void setString_tbnameRejectsEmpty() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException emptyEx = assertSqlException(() -> stmt.setString(1, ""));
        assertTrue(emptyEx.getMessage().contains("Table name not set for row"));
        assertTrue(emptyEx.getMessage().contains("tbname parameter"));
    }

    @Test
    public void setNull_tbnameRejectsNull() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setNull(1, java.sql.Types.VARCHAR));
        assertTrue(ex.getMessage().contains("Table name not set for row"));
        assertTrue(ex.getMessage().contains("tbname parameter"));
    }

    @Test
    public void setObject_tbnameRejectsNull() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setObject(1, null));
        assertTrue(ex.getMessage().contains("Table name not set for row"));
        assertTrue(ex.getMessage().contains("tbname parameter"));
    }

    @Test
    public void setBytes_tbnameAcceptsValidUtf8() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(tbNameAndColFields());

        stmt.setBytes(1, "d000000001".getBytes(StandardCharsets.UTF_8));

        assertEquals(1, stmt.getColumnBuffer(0).getRowCount());
        assertEquals(1, stmt.getColumnBuffer(0).computeTableCount());
    }

    @Test
    public void setBytes_tbnameRejectsInvalidUtf8() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setBytes(1, new byte[]{(byte) 0xC3, 0x28}));
        assertTrue(ex.getMessage().contains("tbname"));
        assertTrue(ex.getMessage().contains("UTF-8"));
    }

    @Test
    public void setShort_utinyintFieldAcceptsUnsignedByteRange() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(java.util.Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_UTINYINT)));

        stmt.setShort(1, (short) 255);

        assertEquals(1, stmt.getColumnBuffer(0).getRowCount());
    }

    @Test
    public void setShort_utinyintFieldRejectsOutOfRangeValue() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(java.util.Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_UTINYINT)));

        SQLException ex = assertSqlException(() -> stmt.setShort(1, (short) 256));
        assertTrue(ex.getMessage().contains("utinyint value is out of range"));
    }

    private WSColumnFastPreparedStatement buildStmt(List<Field> fields) {
        Stmt2PrepareResp resp = new Stmt2PrepareResp();
        resp.setInsert(true);
        resp.setStmtId(1L);
        resp.setFields(fields);
        return new WSColumnFastPreparedStatement(transport, param, "test_db",
                connection, "INSERT INTO t VALUES (?)", 1L, resp);
    }

    private void stubBindExecTransportConsumesRequestBuffer(int affectedRows) throws SQLException {
        Mockito.when(transport.send(
                        Mockito.anyString(),
                        Mockito.anyLong(),
                        Mockito.any(ByteBuf.class),
                        Mockito.eq(false),
                        Mockito.anyLong()))
                .thenAnswer(invocation -> {
                    ByteBuf request = invocation.getArgument(2);
                    request.release();
                    Stmt2ExecResp resp = new Stmt2ExecResp();
                    resp.setCode(0);
                    resp.setStmtId(1L);
                    resp.setAffected(affectedRows);
                    return resp;
                });
    }

    private static List<Field> twoFields(int firstType, int secondType) {
        List<Field> fields = new ArrayList<>();
        fields.add(field((byte) FieldBindType.TAOS_FIELD_COL.getValue(), firstType));
        fields.add(field((byte) FieldBindType.TAOS_FIELD_COL.getValue(), secondType));
        return fields;
    }

    private static List<Field> tbNameAndColFields() {
        List<Field> fields = new ArrayList<>();
        fields.add(field((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), TSDB_DATA_TYPE_VARCHAR));
        fields.add(field((byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        return fields;
    }

    private static Field field(byte bindType, int fieldType) {
        Field field = new Field();
        field.setBindType(bindType);
        field.setFieldType((byte) fieldType);
        field.setPrecision((byte) 0);
        return field;
    }

    private static Stmt2ColumnFieldBuffer[] getColumnBuffers(WSColumnFastPreparedStatement stmt) {
        try {
            java.lang.reflect.Field directField = WSColumnFastPreparedStatement.class.getDeclaredField("columnBuffers");
            directField.setAccessible(true);
            Stmt2ColumnFieldBuffer[] directBuffers = (Stmt2ColumnFieldBuffer[]) directField.get(stmt);
            if (directBuffers != null) {
                return directBuffers;
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
            // Fall back to the write-once batch state.
        }

        try {
            java.lang.reflect.Field batchStateField = WSColumnFastPreparedStatement.class.getDeclaredField("batchState");
            batchStateField.setAccessible(true);
            Object batchState = batchStateField.get(stmt);
            if (batchState == null) {
                return null;
            }

            java.lang.reflect.Field buffersField = batchState.getClass().getDeclaredField("columnBuffers");
            buffersField.setAccessible(true);
            return (Stmt2ColumnFieldBuffer[]) buffersField.get(batchState);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Unable to read column buffers from prepared statement", e);
        }
    }

    private static WSEWChunkSizingUtil.FieldBatchStats[] getBatchStats(WSColumnFastPreparedStatement stmt)
            throws Exception {
        java.lang.reflect.Field batchStatsField = WSColumnFastPreparedStatement.class.getDeclaredField("batchStats");
        batchStatsField.setAccessible(true);
        return (WSEWChunkSizingUtil.FieldBatchStats[]) batchStatsField.get(stmt);
    }

    private static void setNextBufferSpec(
            WSColumnFastPreparedStatement stmt,
            int index,
            WSEWChunkSizingUtil.BufferSpec spec) throws Exception {
        java.lang.reflect.Field nextSpecsField =
                WSColumnFastPreparedStatement.class.getDeclaredField("nextBufferSpecs");
        nextSpecsField.setAccessible(true);
        WSEWChunkSizingUtil.BufferSpec[] nextSpecs =
                (WSEWChunkSizingUtil.BufferSpec[]) nextSpecsField.get(stmt);
        nextSpecs[index] = spec;
    }

    private static WSEWChunkSizingUtil.BufferSpec[] getNextBufferSpecs(WSColumnFastPreparedStatement stmt)
            throws Exception {
        java.lang.reflect.Field nextSpecsField =
                WSColumnFastPreparedStatement.class.getDeclaredField("nextBufferSpecs");
        nextSpecsField.setAccessible(true);
        return (WSEWChunkSizingUtil.BufferSpec[]) nextSpecsField.get(stmt);
    }

    private static void setColumnBuffer(
            WSColumnFastPreparedStatement stmt,
            int index,
            Stmt2ColumnFieldBuffer buffer) throws Exception {
        Stmt2ColumnFieldBuffer[] buffers = getColumnBuffers(stmt);
        Stmt2ColumnFieldBuffer previous = buffers[index];
        if (previous != null) {
            previous.release();
        }
        buffers[index] = buffer;
    }

    private static void setUnderuseStreak(
            WSColumnFastPreparedStatement stmt,
            int index,
            int streak) throws Exception {
        java.lang.reflect.Field field = WSColumnFastPreparedStatement.class.getDeclaredField("underuseStreaks");
        field.setAccessible(true);
        int[] streaks = (int[]) field.get(stmt);
        streaks[index] = streak;
    }

    private static int getUnderuseStreak(WSColumnFastPreparedStatement stmt, int index) throws Exception {
        java.lang.reflect.Field field = WSColumnFastPreparedStatement.class.getDeclaredField("underuseStreaks");
        field.setAccessible(true);
        return ((int[]) field.get(stmt))[index];
    }

    private static void invokeUpdateNextBufferSpecsAfterSuccessfulBatch(WSColumnFastPreparedStatement stmt)
            throws Exception {
        java.lang.reflect.Method method = WSColumnFastPreparedStatement.class
                .getDeclaredMethod("updateNextBufferSpecsAfterSuccessfulBatch");
        method.setAccessible(true);
        method.invoke(stmt);
    }

    private static Stmt2ColumnFieldBuffer newBufferWithoutReusableSpecButOverflowing() throws Exception {
        Stmt2ColumnFieldBuffer buffer = new Stmt2ColumnFieldBuffer(Stmt2FieldMeta.fromField(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));
        java.lang.reflect.Field reusableField =
                Stmt2ColumnFieldBuffer.class.getDeclaredField("reusableValueBuffer");
        reusableField.setAccessible(true);
        Class<?> reusableClass = Class.forName("com.taosdata.jdbc.ws.stmt2.ReusableChunkedBuffer");
        java.lang.reflect.Constructor<?> ctor = reusableClass.getDeclaredConstructor(int.class, int.class, int.class);
        ctor.setAccessible(true);
        Object reusable = ctor.newInstance(8 * 1024, 4 * 1024, 1);
        java.lang.reflect.Field overflowField = reusableClass.getDeclaredField("overflowCount");
        overflowField.setAccessible(true);
        overflowField.setInt(reusable, 1);
        reusableField.set(buffer, reusable);
        return buffer;
    }

    private static SQLException assertSqlException(ThrowingRunnable runnable) throws Exception {
        try {
            runnable.run();
            fail("Expected SQLException");
            return null;
        } catch (SQLException ex) {
            return ex;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
