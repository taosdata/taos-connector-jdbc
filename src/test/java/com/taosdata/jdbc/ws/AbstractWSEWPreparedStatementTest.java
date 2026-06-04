package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.EWBackendThreadInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWRawBlock;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.RecursiveAction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractWSEWPreparedStatementTest {
    private Transport transport;
    private ConnectionParam param;
    private WSConnection connection;

    @Before
    public void setUp() throws Exception {
        transport = Mockito.mock(Transport.class);
        param = Mockito.mock(ConnectionParam.class);
        connection = Mockito.mock(WSConnection.class);

        Mockito.when(param.getZoneId()).thenReturn(null);
        Mockito.when(param.getBackendWriteThreadNum()).thenReturn(1);
        Mockito.when(param.getCacheSizeByRow()).thenReturn(8);
        Mockito.when(param.getBatchSizeByRow()).thenReturn(1);
        Mockito.when(param.getRetryTimes()).thenReturn(1);
        Mockito.when(param.getRequestTimeout()).thenReturn(30_000);
        Mockito.when(param.isCopyData()).thenReturn(true);
        Mockito.when(param.isStrictCheck()).thenReturn(false);
        Mockito.when(param.isEnableAutoConnect()).thenReturn(false);
        Mockito.when(connection.supportsStmt2BindExec()).thenReturn(true);

        Mockito.when(transport.getConnectionParam()).thenReturn(param);
        Mockito.when(transport.getReconnectCount()).thenReturn(0);
        Mockito.when(transport.isConnected()).thenReturn(false);
        Stmt2Resp initResp = new Stmt2Resp();
        initResp.setCode(Code.SUCCESS.getCode());
        initResp.setStmtId(11L);
        Mockito.when(transport.send(Mockito.any(Request.class))).thenReturn(initResp);
        Mockito.when(transport.send(Mockito.any(Request.class), Mockito.eq(false), Mockito.anyLong()))
                .thenReturn(prepareResp(true));
        Mockito.when(transport.send(Mockito.anyString(),
                        Mockito.anyLong(),
                        Mockito.any(ByteBuf.class),
                        Mockito.eq(false),
                        Mockito.anyLong()))
                .thenAnswer(invocation -> {
                    Stmt2ExecResp execResp = new Stmt2ExecResp();
                    execResp.setCode(Code.SUCCESS.getCode());
                    execResp.setStmtId(11L);
                    execResp.setAffected(1);
                    return execResp;
                });
    }

    @After
    public void tearDown() throws Exception {
        Mockito.validateMockitoUsage();
    }

    @Test
    public void writeQueueSkew_isDetectedWhenSingleQueueIsHotAndOthersAreBelowBatchSize() {
        AbstractWSEWPreparedStatement.QueueSkewStats stats =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{12, 1, 1, 1}, 4);

        assertTrue(stats.skewed);
        assertEquals(0, stats.maxQueueIndex);
        assertEquals(12, stats.maxQueueSize);
        assertEquals(1.0, stats.otherAverageQueueSize, 0.001);
    }

    @Test
    public void writeQueueSkew_isIgnoredWhenOtherQueuesAreAlsoBacklogged() {
        AbstractWSEWPreparedStatement.QueueSkewStats stats =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{40, 12, 12, 12}, 4);

        assertFalse(stats.skewed);
        assertEquals(12.0, stats.otherAverageQueueSize, 0.001);
    }

    @Test
    public void writeQueueSkew_requiresHotQueueToReachOneBatchWhenOtherQueuesAreEmpty() {
        AbstractWSEWPreparedStatement.QueueSkewStats belowBatch =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{3, 0, 0, 0}, 4);
        AbstractWSEWPreparedStatement.QueueSkewStats atBatch =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{4, 0, 0, 0}, 4);

        assertFalse(belowBatch.skewed);
        assertTrue(atBatch.skewed);
    }

    @Test
    public void writeQueueSkew_handlesInvalidInputsAndDefensivelyCopiesQueueSizes() {
        assertEquals(0, AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(null, 4).queueSizes.length);
        assertFalse(AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{10}, 4).skewed);
        assertFalse(AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{10, 0}, 0).skewed);

        int[] sizes = new int[]{1, 9, 2};
        AbstractWSEWPreparedStatement.QueueSkewStats stats =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(sizes, 3);
        sizes[1] = 100;

        assertArrayEquals(new int[]{1, 9, 2}, stats.queueSizes);
        assertEquals(1, stats.maxQueueIndex);
        assertEquals(9, stats.maxQueueSize);
        assertEquals(1.5, stats.otherAverageQueueSize, 0.001);
        assertTrue(stats.skewed);
    }

    @Test
    public void standardJdbcBatch_dispatchesRowsAndExecuteUpdateReturnsAffectedRows() throws Exception {
        TestWSEWPreparedStatement stmt = newStatement(true, false);
        try {
            stmt.setBytes(1, "t0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            stmt.setInt(2, 7);
            stmt.addBatch();

            assertArrayEquals(new int[]{Statement.SUCCESS_NO_INFO}, stmt.executeBatch());
            assertEquals(1, stmt.executeUpdate());
            assertEquals(1, stmt.dispatched);
        } finally {
            stmt.close();
        }
    }

    @Test
    public void executeOnInsertDelegatesToExecuteUpdateAndReturnsFalse() throws Exception {
        TestWSEWPreparedStatement stmt = newStatement(true, false);
        try {
            stmt.setString(1, "t0");
            stmt.setInt(2, 7);
            stmt.addBatch();

            assertFalse(stmt.execute());
        } finally {
            stmt.close();
        }
    }

    @Test
    public void progressiveTriggerSerializesPartialQueueAndCloseIsIdempotent() throws Exception {
        TestWSEWPreparedStatement stmt = newStatement(true, false);
        EWBackendThreadInfo info = new EWBackendThreadInfo(4, 4);
        try {
            info.getWriteQueue().put(row("t0", 7));

            stmt.triggerProgressiveForTest(info);

            EWRawBlock block = info.getSerialQueue().poll();
            try {
                assertNotNull(block);
                assertEquals(1, stmt.dispatched);
                assertEquals(1, block.getRowCount());
            } finally {
                if (block != null && block.getByteBuf() != null) {
                    block.getByteBuf().release();
                }
            }
            stmt.close();
            stmt.close();
        } finally {
            info.releaseReusableColumnBuffers();
        }
    }

    @Test
    public void executeRejectsNonInsertAndClosedStatements() throws Exception {
        TestWSEWPreparedStatement queryStmt = newStatement(false, false);
        try {
            SQLException ex = assertSqlException(queryStmt::execute);
            assertTrue(ex.getMessage().contains("Only support insert"));
        } finally {
            queryStmt.close();
        }

        TestWSEWPreparedStatement closedStmt = newStatement(true, false);
        closedStmt.close();
        assertEquals(com.taosdata.jdbc.TSDBErrorNumbers.ERROR_STATEMENT_CLOSED,
                assertSqlException(closedStmt::execute).getErrorCode());
        assertEquals(com.taosdata.jdbc.TSDBErrorNumbers.ERROR_STATEMENT_CLOSED,
                assertSqlException(closedStmt::executeUpdate).getErrorCode());
    }

    @Test
    public void addBatchRejectsPartialBindingsInvalidTbnameAndStrictLengthOverflow() throws Exception {
        TestWSEWPreparedStatement partial = newStatement(true, false);
        try {
            partial.setBytes(1, "t0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            SQLException ex = assertSqlException(partial::addBatch);
            assertTrue(ex.getMessage().contains("Only support standard jdbc bind api"));
        } finally {
            partial.close();
        }

        TestWSEWPreparedStatement invalidTbname = newStatement(true, false);
        try {
            invalidTbname.setInt(1, 1);
            invalidTbname.setInt(2, 2);
            SQLException ex = assertSqlException(invalidTbname::addBatch);
            assertTrue(ex.getMessage().contains("error type tbname"));
        } finally {
            invalidTbname.close();
        }

        TestWSEWPreparedStatement strict = newStatement(true, true);
        try {
            strict.setString(1, "t0");
            strict.setString(2, "abcd");
            SQLException ex = assertSqlException(strict::addBatch);
            assertTrue(ex.getMessage().contains("data length is too long"));
        } finally {
            strict.close();
        }
    }

    @Test
    public void strictLengthCheckRejectsOversizedStringColumnData() throws Exception {
        TestWSEWPreparedStatement strict = newStatement(true, true);
        try {
            strict.bindRawColumn(1, new Column("t0", com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY, 1));
            strict.bindRawColumn(2, new Column("abcd", com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY, 2));

            SQLException ex = assertSqlException(strict::addBatch);

            assertTrue(ex.getMessage().contains("data length is too long"));
        } finally {
            strict.close();
        }
    }

    @Test
    public void executeUpdateSurfacesWorkerErrorWithInsertedRowsContext() throws Exception {
        TestWSEWPreparedStatement stmt = newStatement(true, false);
        stmt.nextSerializationError = new SQLException("injected write failure");
        try {
            stmt.setString(1, "t0");
            stmt.setInt(2, 7);
            stmt.addBatch();

            SQLException ex = assertSqlException(stmt::executeUpdate);
            assertEquals(com.taosdata.jdbc.TSDBErrorNumbers.ERROR_FW_WRITE_ERROR, ex.getErrorCode());
            assertTrue(ex.getMessage().contains("InsertedRows"));
            assertTrue(ex.getMessage().contains("injected write failure"));
        } finally {
            stmt.close();
        }
    }

    @Test
    public void unsupportedMetadataAndColumnDataExtensionMethodsThrowExplicitErrors() throws Exception {
        TestWSEWPreparedStatement stmt = newStatement(true, false);
        try {
            assertSqlException(stmt::executeQuery);
            SQLException metaEx = assertSqlException(() -> {
                ResultSetMetaData ignored = stmt.getMetaData();
            });
            assertTrue(metaEx.getMessage().contains("Fast write mode only support insert"));
            assertEquals(com.taosdata.jdbc.TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD,
                    assertSqlException(stmt::columnDataAddBatch).getErrorCode());
            assertEquals(com.taosdata.jdbc.TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD,
                    assertSqlException(stmt::columnDataExecuteBatch).getErrorCode());
            assertEquals(com.taosdata.jdbc.TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD,
                    assertSqlException(stmt::columnDataCloseBatch).getErrorCode());
        } finally {
            stmt.close();
        }
    }

    @Test
    public void constructorWrapsWorkerInitFailure() throws Exception {
        Transport failingTransport = Mockito.mock(Transport.class);
        Mockito.when(failingTransport.getReconnectCount()).thenReturn(0);
        Mockito.when(failingTransport.getConnectionParam()).thenReturn(param);
        Mockito.when(failingTransport.isConnected()).thenReturn(false);
        Mockito.when(failingTransport.send(Mockito.any(Request.class)))
                .thenThrow(new SQLException("init failed"));

        SQLException ex = assertSqlException(() -> new TestWSEWPreparedStatement(
                failingTransport,
                param,
                "test_db",
                connection,
                "INSERT INTO ? VALUES(?)",
                1L,
                prepareResp(true)));
        assertTrue(ex.getMessage().contains("Failed to initialize prepared statement"));
        assertTrue(ex.getMessage().contains("init failed"));
    }

    private TestWSEWPreparedStatement newStatement(boolean insert, boolean strictCheck) throws SQLException {
        Mockito.when(param.isStrictCheck()).thenReturn(strictCheck);
        return new TestWSEWPreparedStatement(
                transport,
                param,
                "test_db",
                connection,
                "INSERT INTO ? VALUES(?)",
                1L,
                prepareResp(insert));
    }

    private static Stmt2PrepareResp prepareResp(boolean insert) {
        Stmt2PrepareResp resp = new Stmt2PrepareResp();
        resp.setInsert(insert);
        resp.setStmtId(11L);
        resp.setFields(Arrays.asList(field(
                (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(),
                com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY,
                8),
                field((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                        com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY,
                        2)));
        return resp;
    }

    private static Field field(byte bindType, int type, int bytes) {
        Field field = new Field();
        field.setBindType(bindType);
        field.setFieldType((byte) type);
        field.setBytes(bytes);
        return field;
    }

    private static Map<Integer, Column> row(String tbname, int value) {
        Map<Integer, Column> row = new java.util.HashMap<>();
        row.put(1, new Column(tbname, com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY, 1));
        row.put(2, new Column(value, com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT, 2));
        return row;
    }

    private static SQLException assertSqlException(ThrowingSqlRunnable runnable) {
        try {
            runnable.run();
            fail("Expected SQLException");
            return null;
        } catch (SQLException e) {
            return e;
        }
    }

    @FunctionalInterface
    private interface ThrowingSqlRunnable {
        void run() throws SQLException;
    }

    private static final class TestWSEWPreparedStatement extends AbstractWSEWPreparedStatement {
        private int dispatched;
        private SQLException nextSerializationError;

        private TestWSEWPreparedStatement(Transport transport,
                                          ConnectionParam param,
                                          String database,
                                          AbstractConnection connection,
                                          String sql,
                                          Long instanceId,
                                          Stmt2PrepareResp prepareResp) throws SQLException {
            super(transport, param, database, connection, sql, instanceId, prepareResp);
        }

        @Override
        protected RecursiveAction newSerializationTask(EWBackendThreadInfo backendThreadInfo,
                                                       int batchSize,
                                                       boolean isProgressive) {
            return new RecursiveAction() {
                @Override
                protected void compute() {
                    int rows = 0;
                    try {
                        for (int i = 0; i < batchSize; i++) {
                            Map<Integer, Column> row = backendThreadInfo.getWriteQueue().poll();
                            if (row == null) {
                                break;
                            }
                            rows++;
                        }
                        ByteBuf rawBlock = null;
                        if (nextSerializationError == null) {
                            rawBlock = Unpooled.buffer(16);
                            rawBlock.writeZero(16);
                        }
                        backendThreadInfo.getSerialQueue().put(new EWRawBlock(
                                rawBlock,
                                rows,
                                nextSerializationError));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        backendThreadInfo.getSerializeRunning().set(false);
                    }
                }
            };
        }

        @Override
        protected void dispatchSerializationTask(RecursiveAction task) {
            dispatched++;
            task.invoke();
        }

        private void triggerProgressiveForTest(EWBackendThreadInfo backendThreadInfo) {
            triggerSerializeProgressive(backendThreadInfo);
        }

        private void bindRawColumn(int index, Column column) {
            colOrderedMap.put(index, column);
        }
    }
}
