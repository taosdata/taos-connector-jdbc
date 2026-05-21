package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
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
import static org.junit.Assert.assertNotSame;
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
        WSColumnFastPreparedStatement stmt = buildStmt(tbNameAndColFields());

        stmt.setString(1, "d000000001");
        stmt.setInt(2, 7);
        stmt.addBatch();

        int[] result = stmt.executeBatch();

        assertEquals(1, result.length);
        assertEquals(java.sql.Statement.SUCCESS_NO_INFO, result[0]);
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
    public void clearBatch_recreatesVarWidthBuffersWithLearnedCapacity() throws Exception {
        WSColumnFastPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));
        String largeValue = String.join("", Collections.nCopies(20_000, "a"));

        stmt.setString(1, largeValue);
        stmt.addBatch();

        Stmt2ColumnFieldBuffer[] beforeClear = getColumnBuffers(stmt);
        Stmt2ColumnFieldBuffer beforeBuffer = beforeClear[0];
        int usedValueBytes = getAutoExpandingBufferReadableBytes(beforeClear[0], "valueBuffer");

        stmt.clearBatch();

        Stmt2ColumnFieldBuffer[] afterClear = getColumnBuffers(stmt);
        assertNotSame(beforeBuffer, afterClear[0]);
        int nextInitialSize = getAutoExpandingBufferBufferSize(afterClear[0], "valueBuffer");
        assertTrue(nextInitialSize >= usedValueBytes);
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
        java.lang.reflect.Field bufferSize = autoBuffer.getClass().getDeclaredField("bufferSize");
        bufferSize.setAccessible(true);
        return bufferSize.getInt(autoBuffer);
    }

    private static Object getDeclaredField(Object target, String fieldName) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
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
