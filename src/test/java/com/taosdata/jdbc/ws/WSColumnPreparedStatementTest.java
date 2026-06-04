package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.TaosPrepareStatement;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import com.taosdata.jdbc.ws.stmt2.Stmt2VariableWidthReuseHelper;
import com.taosdata.jdbc.ws.stmt2.Stmt2ChunkSizingUtil;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BLOB;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UTINYINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WSColumnPreparedStatementTest {

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
        Mockito.when(connection.isSupportBlob()).thenReturn(true);
    }

    @Test
    public void standaloneFastStatement_extendsWSRetryableStmtDirectly() {
        assertEquals(WSRetryableStmt.class, WSColumnPreparedStatement.class.getSuperclass());
    }

    @Test
    public void standaloneFastStatement_implementsTaosPrepareStatement() {
        assertTrue(TaosPrepareStatement.class.isAssignableFrom(WSColumnPreparedStatement.class));
    }

    @Test
    public void executeBatch_rowCountMismatch_resetsStateAndThrows() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_VARCHAR));

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
        WSColumnPreparedStatement stmt = buildStmt(java.util.Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        stmt.setInt(1, 1);
        stmt.setInt(1, 2);

        SQLException ex = assertSqlException(stmt::executeUpdate);
        assertTrue(ex.getMessage().contains("row count mismatch"));
    }

    @Test
    public void executeBatch_bindExecTransportConsumesRequestBuffer_withoutDoubleRelease() throws Exception {
        stubBindExecTransportConsumesRequestBuffer(1);
        WSColumnPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_INT));

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
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        assertTrue(stmt.isUsingBindExec());
    }

    @Test
    public void clearBatch_reusesColumnBuffersAndResetsCounter() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(java.util.Collections.singletonList(field(
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
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
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
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));

        stmt.setString(1, "first-value");
        stmt.addBatch();

        Stmt2ColumnFieldBuffer beforeBuffer = getColumnBuffers(stmt)[0];
        setNextBufferSpec(stmt, 0, new Stmt2ChunkSizingUtil.BufferSpec(16 * 1024, 2));

        stmt.clearBatch();

        Stmt2ColumnFieldBuffer afterBuffer = getColumnBuffers(stmt)[0];
        assertTrue(beforeBuffer != afterBuffer);
        assertEquals(0, afterBuffer.getRowCount());
        assertEquals(16 * 1024, afterBuffer.currentReusableSpec().getChunkBytes());
        assertEquals(2, afterBuffer.currentReusableSpec().getReusableChunkCount());
    }

    @Test
    public void constructor_initializesBatchStatsScaffolding() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_VARCHAR));

        Stmt2ChunkSizingUtil.FieldBatchStats[] stats = getBatchStats(stmt);

        assertEquals(2, stats.length);
        assertTrue(stats[0] != null);
        assertTrue(stats[1] != null);
    }

    @Test
    public void constructor_releasesPreviouslyAllocatedBuffersWhenALaterAllocationFails() throws Exception {
        Stmt2PrepareResp resp = new Stmt2PrepareResp();
        resp.setInsert(true);
        resp.setStmtId(1L);
        resp.setFields(twoFields(TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARCHAR));

        Stmt2ColumnFieldBuffer firstBuffer = Stmt2ColumnFieldBuffer.forReusableValueBuffer(
                Stmt2FieldMeta.fromField(field((byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)),
                null,
                8 * 1024,
                4 * 1024,
                1);
        Stmt2ColumnFieldBuffer spyBuffer = Mockito.spy(firstBuffer);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();

        try (MockedStatic<Stmt2VariableWidthReuseHelper> mocked = Mockito.mockStatic(
                Stmt2VariableWidthReuseHelper.class, Mockito.CALLS_REAL_METHODS)) {
            mocked.when(() -> Stmt2VariableWidthReuseHelper.resolveBufferSpec(Mockito.any(), Mockito.anyInt()))
                    .thenReturn(new Stmt2ChunkSizingUtil.BufferSpec(8 * 1024, 1));
            mocked.when(() -> Stmt2VariableWidthReuseHelper.createReusableVariableWidthBuffer(
                    Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        if (calls.getAndIncrement() == 0) {
                            return spyBuffer;
                        }
                        throw new OutOfMemoryError("boom");
                    });

            try {
                new WSColumnPreparedStatement(transport, param, "test_db", connection,
                        "INSERT INTO t VALUES (?, ?)", 1L, resp);
                fail("expected OutOfMemoryError");
            } catch (OutOfMemoryError expected) {
                // expected
            }
        }

        assertEquals(0, cachedChunkCount(spyBuffer));
    }

    @Test
    public void resetFastState_nullsReleasedSlotWhenReallocationFails() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));
        Stmt2ColumnFieldBuffer original = getColumnBuffers(stmt)[0];
        setNextBufferSpec(stmt, 0, new Stmt2ChunkSizingUtil.BufferSpec(16 * 1024, 2));
        java.lang.reflect.Method resetFastStateMethod = WSColumnPreparedStatement.class
                .getDeclaredMethod("resetFastState");
        resetFastStateMethod.setAccessible(true);

        try (MockedStatic<Stmt2VariableWidthReuseHelper> mocked = Mockito.mockStatic(
                Stmt2VariableWidthReuseHelper.class, Mockito.CALLS_REAL_METHODS)) {
            mocked.when(() -> Stmt2VariableWidthReuseHelper.createReusableVariableWidthBuffer(
                    Mockito.any(), Mockito.any()))
                    .thenThrow(new OutOfMemoryError("boom"));

            try {
                resetFastStateMethod.invoke(stmt);
                fail("expected OutOfMemoryError");
            } catch (java.lang.reflect.InvocationTargetException expected) {
                assertTrue(expected.getCause() instanceof OutOfMemoryError);
            }
        }

        java.lang.reflect.Field columnBuffersField = WSColumnPreparedStatement.class
                .getDeclaredField("columnBuffers");
        columnBuffersField.setAccessible(true);
        assertNull(columnBuffersField.get(stmt));

        resetFastStateMethod.invoke(stmt);
        Stmt2ColumnFieldBuffer[] rebuilt = (Stmt2ColumnFieldBuffer[]) columnBuffersField.get(stmt);
        assertTrue(rebuilt != null);
        assertEquals(1, rebuilt.length);
        assertEquals(0, cachedChunkCount(original));
    }

    @Test
    public void executeBatch_varWidthOverflowUpdatesNextSpecForFollowingBatch() throws Exception {
        stubBindExecTransportConsumesRequestBuffer(1);
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));
        String largeValue = String.join("", Collections.nCopies(50_000, "x"));

        stmt.setString(1, largeValue);
        stmt.addBatch();
        stmt.executeBatch();

        Stmt2ChunkSizingUtil.BufferSpec nextSpec = getNextBufferSpecs(stmt)[0];
        assertTrue(nextSpec.getChunkBytes() > 8 * 1024);
    }

    @Test
    public void updateNextBufferSpecsAfterSuccessfulBatch_keepsOtherSizingUpdatesWhenOneFieldOverflows()
            throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_VARCHAR));
        Stmt2ChunkSizingUtil.FieldBatchStats[] stats = getBatchStats(stmt);

        stats[0].recordValueBytes(50L * 1024, 1024, 1);
        stats[0].setActiveChunksUsed(9);
        stats[1].recordValueBytes(1L, 1, 1);
        setColumnBuffer(stmt, 1, newBufferWithoutReusableSpecButOverflowing());
        setNextBufferSpec(stmt, 1, new Stmt2ChunkSizingUtil.BufferSpec(Integer.MAX_VALUE, 1));
        setUnderuseStreak(stmt, 0, 11);
        setUnderuseStreak(stmt, 1, 13);

        invokeUpdateNextBufferSpecsAfterSuccessfulBatch(stmt);

        Stmt2ChunkSizingUtil.BufferSpec[] nextSpecs = getNextBufferSpecs(stmt);
        assertTrue(nextSpecs[0].getChunkBytes() > 8 * 1024);
        assertEquals(0, getUnderuseStreak(stmt, 0));
        assertEquals(Integer.MAX_VALUE, nextSpecs[1].getChunkBytes());
        assertEquals(1, nextSpecs[1].getReusableChunkCount());
        assertEquals(0, getUnderuseStreak(stmt, 1));
    }

    @Test
    public void clearBatch_resetsVarWidthBatchStats() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
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
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException nullEx = assertSqlException(() -> stmt.setString(1, null));
        assertEquals(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, nullEx.getErrorCode());
        assertTrue(nullEx.getMessage().contains("table name can't be null"));
        assertTrue(nullEx.getMessage().startsWith("ERROR (0x2303)"));
    }

    @Test
    public void setString_tbnameRejectsEmpty() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException emptyEx = assertSqlException(() -> stmt.setString(1, ""));
        assertTrue(emptyEx.getMessage().contains("Table name not set for row"));
        assertTrue(emptyEx.getMessage().contains("tbname parameter"));
    }

    @Test
    public void setNull_tbnameRejectsNull() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setNull(1, java.sql.Types.VARCHAR));
        assertTrue(ex.getMessage().contains("Table name not set for row"));
        assertTrue(ex.getMessage().contains("tbname parameter"));
    }

    @Test
    public void setObject_tbnameRejectsNull() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setObject(1, null));
        assertTrue(ex.getMessage().contains("Table name not set for row"));
        assertTrue(ex.getMessage().contains("tbname parameter"));
    }

    @Test
    public void setBytes_tbnameAcceptsValidUtf8() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        stmt.setBytes(1, "d000000001".getBytes(StandardCharsets.UTF_8));

        assertEquals(1, stmt.getColumnBuffer(0).getRowCount());
        assertEquals(1, stmt.getColumnBuffer(0).computeTableCount());
    }

    @Test
    public void setBytes_tbnameUsesRawTbNameAppend() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());
        Stmt2ColumnFieldBuffer spyBuffer = Mockito.spy(new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.fromField(field((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), TSDB_DATA_TYPE_VARCHAR))));
        setColumnBuffer(stmt, 0, spyBuffer);
        byte[] tbname = "d000000001".getBytes(StandardCharsets.UTF_8);

        stmt.setBytes(1, tbname);

        Mockito.verify(spyBuffer).appendTbNameBytes(tbname, tbname.length);
        Mockito.verify(spyBuffer, Mockito.never()).appendEncodedVar(Mockito.any(byte[].class));
    }

    @Test
    public void setBytes_tbnameRejectsNull() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setBytes(1, null));

        assertEquals(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("table name can't be null"));
    }

    @Test
    public void setNString_tbnameRejectsNull() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setNString(1, null));

        assertEquals(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("table name can't be null"));
    }

    @Test
    public void fixedWidthSetter_rejectsTbnameParameter() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setInt(1, 7));

        assertTrue(ex.getMessage().contains("Table name not set for row"));
    }

    @Test
    public void setString_tbnameAcceptsValidUtf8() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        stmt.setString(1, "涛思_01");

        assertEquals(1, stmt.getColumnBuffer(0).getRowCount());
        assertEquals(1, stmt.getColumnBuffer(0).computeTableCount());
    }

    @Test
    public void setTableName_usesTbNameField() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        stmt.setTableName("t_01");
        stmt.setInt(0, Collections.singletonList(7));
        stmt.columnDataAddBatch();

        assertEquals(1, stmt.getColumnBuffer(0).getRowCount());
        assertEquals(1, stmt.getColumnBuffer(0).computeTableCount());
        assertEquals(1, stmt.getExpectedRowCount());
    }

    @Test
    public void setTagInt_usesTagOrdinal() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameTagTagAndColFields());
        Stmt2ColumnFieldBuffer tag0Buffer = Mockito.spy(new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.fromField(field((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), TSDB_DATA_TYPE_INT))));
        Stmt2ColumnFieldBuffer tag1Buffer = Mockito.spy(new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.fromField(field((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), TSDB_DATA_TYPE_VARCHAR))));
        setColumnBuffer(stmt, 1, tag0Buffer);
        setColumnBuffer(stmt, 2, tag1Buffer);

        stmt.setTableName("t_01");
        stmt.setTagInt(0, 42);
        stmt.setTagString(1, "loc");
        stmt.setInt(0, Collections.singletonList(7));
        stmt.columnDataAddBatch();

        assertEquals(1, stmt.getColumnBuffer(1).getRowCount());
        assertEquals(1, stmt.getColumnBuffer(2).getRowCount());
        Mockito.verify(tag0Buffer).appendFixed4Raw(42);
        Mockito.verify(tag1Buffer).appendString("loc", 3);
        assertEquals(1, stmt.getExpectedRowCount());
    }

    @Test
    public void setTagString_usesTagOrdinal() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameTagTagAndColFields());
        Stmt2ColumnFieldBuffer tag0Buffer = Mockito.spy(new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.fromField(field((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), TSDB_DATA_TYPE_INT))));
        Stmt2ColumnFieldBuffer tag1Buffer = Mockito.spy(new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.fromField(field((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), TSDB_DATA_TYPE_VARCHAR))));
        setColumnBuffer(stmt, 1, tag0Buffer);
        setColumnBuffer(stmt, 2, tag1Buffer);

        stmt.setTableName("t_01");
        stmt.setTagInt(0, 42);
        stmt.setTagString(1, "loc");
        stmt.setInt(0, Collections.singletonList(7));
        stmt.columnDataAddBatch();

        assertEquals(1, stmt.getColumnBuffer(1).getRowCount());
        assertEquals(1, stmt.getColumnBuffer(2).getRowCount());
        Mockito.verify(tag0Buffer).appendFixed4Raw(42);
        Mockito.verify(tag1Buffer).appendString("loc", 3);
        assertEquals(1, stmt.getExpectedRowCount());
    }

    @Test
    public void columnDataAddBatch_repeatsTableNameAndTagsForColumnRows() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameTagTagAndColFields());

        stmt.setTableName("t_01");
        stmt.setTagInt(0, 42);
        stmt.setTagString(1, "loc");
        stmt.setInt(0, java.util.Arrays.asList(7, 8, 9));
        stmt.columnDataAddBatch();

        assertEquals(3, stmt.getExpectedRowCount());
        assertEquals(3, stmt.getColumnBuffer(0).getRowCount());
        assertEquals(1, stmt.getColumnBuffer(0).computeTableCount());
        assertEquals(3, stmt.getColumnBuffer(1).getRowCount());
        assertEquals(3, stmt.getColumnBuffer(2).getRowCount());
        assertEquals(3, stmt.getColumnBuffer(3).getRowCount());
    }

    @Test
    public void columnDataExecuteBatch_usesBindExecAndResetsState() throws Exception {
        stubBindExecTransportConsumesRequestBuffer(2);
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        stmt.setTableName("t_01");
        stmt.setInt(0, java.util.Arrays.asList(7, 8));
        stmt.columnDataAddBatch();

        stmt.columnDataExecuteBatch();

        assertEquals(0, stmt.getExpectedRowCount());
        assertEquals(0, stmt.getColumnBuffer(0).getRowCount());
        assertEquals(0, stmt.getColumnBuffer(1).getRowCount());
    }

    @Test
    public void setString_usesDirectStringAppendForVarWidthColumn() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));
        Stmt2ColumnFieldBuffer spyBuffer = Mockito.spy(new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.fromField(field((byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR))));
        setColumnBuffer(stmt, 0, spyBuffer);

        stmt.setString(1, "alpha");

        Mockito.verify(spyBuffer).appendString("alpha", 5);
        Mockito.verify(spyBuffer, Mockito.never()).appendEncodedVar(Mockito.any(byte[].class));
    }

    @Test
    public void setString_tbnameUsesDirectTbNameAppend() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());
        Stmt2ColumnFieldBuffer spyBuffer = Mockito.spy(new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.fromField(field((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), TSDB_DATA_TYPE_VARCHAR))));
        setColumnBuffer(stmt, 0, spyBuffer);

        stmt.setString(1, "t_01");

        Mockito.verify(spyBuffer).appendTbName("t_01", 4);
        Mockito.verify(spyBuffer, Mockito.never()).appendEncodedVar(Mockito.any(byte[].class));
    }

    @Test
    public void setBytes_tbnameRejectsInvalidUtf8() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameAndColFields());

        SQLException ex = assertSqlException(() -> stmt.setBytes(1, new byte[]{(byte) 0xC3, 0x28}));
        assertTrue(ex.getMessage().contains("tbname"));
        assertTrue(ex.getMessage().contains("UTF-8"));
    }

    @Test
    public void setShort_utinyintFieldAcceptsUnsignedByteRange() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(java.util.Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_UTINYINT)));

        stmt.setShort(1, (short) 255);

        assertEquals(1, stmt.getColumnBuffer(0).getRowCount());
    }

    @Test
    public void setShort_utinyintFieldRejectsOutOfRangeValue() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(java.util.Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_UTINYINT)));

        SQLException ex = assertSqlException(() -> stmt.setShort(1, (short) 256));
        assertTrue(ex.getMessage().contains("utinyint value is out of range"));
    }

    @Test
    public void setTableName_rejectsStatementWithoutTbnameField() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        SQLException ex = assertSqlException(() -> stmt.setTableName("t_01"));

        assertTrue(ex.getMessage().contains("No table name field"));
    }

    @Test
    public void columnDataAddBatch_validatesExtensionInputsBeforeAppendingRows() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameTagTagAndColFields());

        SQLException noData = assertSqlException(stmt::columnDataAddBatch);
        assertTrue(noData.getMessage().contains("No column data bound"));

        SQLException badColumnIndex = assertSqlException(() -> stmt.setInt(1, Collections.singletonList(7)));
        assertEquals(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, badColumnIndex.getErrorCode());

        SQLException nullColumnList = assertSqlException(() -> stmt.setInt(0, null));
        assertTrue(nullColumnList.getMessage().contains("Column data list must not be null"));

        IllegalArgumentException badTagIndex = assertIllegalArgumentException(() -> stmt.setTagInt(2, 42));
        assertTrue(badTagIndex.getMessage().contains("Failed to bind tag at index 2"));

        stmt.setInt(0, Collections.singletonList(7));
        SQLException missingTableName = assertSqlException(stmt::columnDataAddBatch);
        assertTrue(missingTableName.getMessage().contains("Table name not set for row"));

        stmt.setTableName("t_01");
        SQLException missingTag = assertSqlException(stmt::columnDataAddBatch);
        assertTrue(missingTag.getMessage().contains("Tag value not set at index 0"));
    }

    @Test
    public void columnDataAddBatch_rejectsMismatchedExtensionColumnRowCounts() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(twoFields(TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_VARCHAR));

        stmt.setInt(0, java.util.Arrays.asList(1, 2));
        stmt.setString(1, Collections.singletonList("one"), 10);

        SQLException ex = assertSqlException(stmt::columnDataAddBatch);
        assertTrue(ex.getMessage().contains("column data row count mismatch"));
    }

    @Test
    public void columnDataAddBatch_acceptsNullColumnListValues() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(tbNameTagTagAndColFields());

        stmt.setTableName("t_01");
        stmt.setTagInt(0, 42);
        stmt.setTagString(1, "loc");
        stmt.setInt(0, java.util.Arrays.asList(7, null));
        stmt.columnDataAddBatch();

        assertEquals(2, stmt.getExpectedRowCount());
        assertEquals(2, stmt.getColumnBuffer(3).getRowCount());
    }

    @Test
    public void setObjectWithSqlType_rejectsInvalidJavaTypes() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        assertSetObjectTypeError(stmt, java.sql.Types.BOOLEAN, "bad", "Invalid type for boolean");
        assertSetObjectTypeError(stmt, java.sql.Types.TINYINT, "bad", "Invalid type for byte");
        assertSetObjectTypeError(stmt, java.sql.Types.SMALLINT, "bad", "Invalid type for short");
        assertSetObjectTypeError(stmt, java.sql.Types.INTEGER, "bad", "Invalid type for int");
        assertSetObjectTypeError(stmt, java.sql.Types.BIGINT, "bad", "Invalid type for long");
        assertSetObjectTypeError(stmt, java.sql.Types.FLOAT, "bad", "Invalid type for float");
        assertSetObjectTypeError(stmt, java.sql.Types.DOUBLE, "bad", "Invalid type for double");
        assertSetObjectTypeError(stmt, java.sql.Types.TIMESTAMP, "bad", "Invalid type for timestamp");
        assertSetObjectTypeError(stmt, java.sql.Types.VARCHAR, new Object(), "Invalid type for binary");
        assertSetObjectTypeError(stmt, java.sql.Types.BLOB, "bad", "Invalid type for blob");
        assertSetObjectTypeError(stmt, java.sql.Types.DECIMAL, "bad", "Invalid type for decimal");
        assertSetObjectTypeError(stmt, java.sql.Types.ARRAY, 1, "unsupported type");
    }

    @Test
    public void setObjectUnsupportedType_rejectsUnknownJavaType() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        SQLException ex = assertSqlException(() -> stmt.setObject(1, new Object()));

        assertTrue(ex.getMessage().contains("Unsupported data type"));
    }

    @Test
    public void temporalAndBlobSetters_handleNullsCalendarsAndStreams() throws Exception {
        WSColumnPreparedStatement timestampStmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_TIMESTAMP)));

        timestampStmt.setTimestamp(1, (java.sql.Timestamp) null);
        timestampStmt.setTimestamp(1, new java.sql.Timestamp(0), java.util.Calendar.getInstance());
        timestampStmt.setDate(1, null);
        timestampStmt.setDate(1, new java.sql.Date(1));
        timestampStmt.setTime(1, null);
        timestampStmt.setTime(1, new java.sql.Time(2));
        assertEquals(6, timestampStmt.getColumnBuffer(0).getRowCount());

        WSColumnPreparedStatement varcharStmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR)));
        varcharStmt.setBigDecimal(1, (java.math.BigDecimal) null);
        varcharStmt.setNull(1, java.sql.Types.VARCHAR, "varchar");
        assertEquals(2, varcharStmt.getColumnBuffer(0).getRowCount());

        WSColumnPreparedStatement blobStmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_BLOB)));
        byte[] bytes = new byte[]{1, 2, 3};
        blobStmt.setBlob(1, (java.sql.Blob) null);
        blobStmt.setBlob(1, new java.io.ByteArrayInputStream(bytes), bytes.length);
        blobStmt.setBlob(1, new java.io.ByteArrayInputStream(bytes));
        assertEquals(3, blobStmt.getColumnBuffer(0).getRowCount());
    }

    @Test
    public void setObject_coversTemporalAndVariableWidthSpecializations() throws Exception {
        WSColumnPreparedStatement timestampStmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_TIMESTAMP)));
        timestampStmt.setObject(1, null);
        timestampStmt.setObject(1, new java.sql.Date(1));
        timestampStmt.setObject(1, new java.sql.Time(2));
        timestampStmt.setObject(1, new java.sql.Timestamp(3));
        timestampStmt.setObject(1, java.time.LocalDateTime.of(1970, 1, 1, 0, 0));
        timestampStmt.setObject(1, java.time.Instant.ofEpochMilli(4));
        timestampStmt.setObject(1, java.time.ZonedDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(5), java.time.ZoneId.of("UTC")));
        timestampStmt.setObject(1, java.time.OffsetDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(6), java.time.ZoneId.of("UTC")));
        assertEquals(8, timestampStmt.getColumnBuffer(0).getRowCount());

        Mockito.when(param.getZoneId()).thenReturn(java.time.ZoneId.of("UTC"));
        WSColumnPreparedStatement zonedTimestampStmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_TIMESTAMP)));
        zonedTimestampStmt.setObject(1, java.time.LocalDateTime.of(1970, 1, 1, 0, 0));
        zonedTimestampStmt.setObject(1, java.time.LocalDateTime.of(1970, 1, 1, 0, 0), java.sql.Types.TIMESTAMP);
        assertEquals(2, zonedTimestampStmt.getColumnBuffer(0).getRowCount());

        WSColumnPreparedStatement blobStmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_BLOB)));
        byte[] bytes = new byte[]{1, 2, 3};
        blobStmt.setObject(1, bytes, java.sql.Types.BLOB);
        blobStmt.setObject(1, new com.taosdata.jdbc.common.TDBlob(bytes, true), java.sql.Types.BLOB);
        blobStmt.setObject(1, new com.taosdata.jdbc.common.TDBlob(bytes, true));
        assertEquals(3, blobStmt.getColumnBuffer(0).getRowCount());
    }

    @Test
    public void insertOnlyMethodsValidateClosedAndQueryStates() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));
        SQLException queryEx = assertSqlException(stmt::executeQuery);
        assertTrue(queryEx.getMessage().contains("only supports insert"));
        assertNull(stmt.getMetaData());

        WSColumnPreparedStatement nonInsert = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)), false);
        SQLException nonInsertEx = assertSqlException(nonInsert::executeUpdate);
        assertTrue(nonInsertEx.getMessage().contains("insert SQL must be prepared"));

        stmt.close();
        assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, assertSqlException(stmt::addBatch).getErrorCode());
        assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, assertSqlException(stmt::clearBatch).getErrorCode());
        assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, assertSqlException(stmt::executeBatch).getErrorCode());
        assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, assertSqlException(stmt::execute).getErrorCode());
        assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, assertSqlException(stmt::executeUpdate).getErrorCode());
        assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, assertSqlException(stmt::getParameterMetaData).getErrorCode());
        assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, assertSqlException(stmt::columnDataAddBatch).getErrorCode());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void unsupportedJdbcMethodsReturnExplicitUnsupportedErrors() throws Exception {
        WSColumnPreparedStatement stmt = buildStmt(Collections.singletonList(field(
                (byte) FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT)));

        assertUnsupportedMethod(() -> stmt.setObject(1, 1, java.sql.Types.INTEGER, 0));
        assertUnsupportedMethod(() -> stmt.setAsciiStream(1, new java.io.ByteArrayInputStream(new byte[0]), 0));
        assertUnsupportedMethod(() -> stmt.setUnicodeStream(1, new java.io.ByteArrayInputStream(new byte[0]), 0));
        assertUnsupportedMethod(() -> stmt.setBinaryStream(1, new java.io.ByteArrayInputStream(new byte[0]), 0));
        assertUnsupportedMethod(() -> stmt.setCharacterStream(1, new java.io.StringReader("x"), 1));
        assertUnsupportedMethod(() -> stmt.setAsciiStream(1, new java.io.ByteArrayInputStream(new byte[0]), 1L));
        assertUnsupportedMethod(() -> stmt.setBinaryStream(1, new java.io.ByteArrayInputStream(new byte[0]), 1L));
        assertUnsupportedMethod(() -> stmt.setAsciiStream(1, new java.io.ByteArrayInputStream(new byte[0])));
        assertUnsupportedMethod(() -> stmt.setBinaryStream(1, new java.io.ByteArrayInputStream(new byte[0])));
        assertUnsupportedMethod(() -> stmt.setCharacterStream(1, new java.io.StringReader("x"), 1L));
        assertUnsupportedMethod(() -> stmt.setCharacterStream(1, new java.io.StringReader("x")));
        assertUnsupportedMethod(() -> stmt.setNCharacterStream(1, new java.io.StringReader("x"), 1L));
        assertUnsupportedMethod(() -> stmt.setNCharacterStream(1, new java.io.StringReader("x")));
        assertUnsupportedMethod(() -> stmt.setClob(1, (java.sql.Clob) null));
        assertUnsupportedMethod(() -> stmt.setClob(1, new java.io.StringReader("x"), 1L));
        assertUnsupportedMethod(() -> stmt.setClob(1, new java.io.StringReader("x")));
        assertUnsupportedMethod(() -> stmt.setNClob(1, (java.sql.NClob) null));
        assertUnsupportedMethod(() -> stmt.setNClob(1, new java.io.StringReader("x"), 1L));
        assertUnsupportedMethod(() -> stmt.setNClob(1, new java.io.StringReader("x")));
        assertUnsupportedMethod(() -> stmt.setDate(1, new java.sql.Date(0), java.util.Calendar.getInstance()));
        assertUnsupportedMethod(() -> stmt.setTime(1, new java.sql.Time(0), java.util.Calendar.getInstance()));
        assertUnsupportedMethod(stmt::clearParameters);
        assertUnsupportedMethod(() -> stmt.setArray(1, null));
        assertUnsupportedMethod(() -> stmt.setRef(1, null));
        assertUnsupportedMethod(() -> stmt.setRowId(1, null));
        assertUnsupportedMethod(() -> stmt.setSQLXML(1, null));
        assertUnsupportedMethod(() -> stmt.setURL(1, null));
        assertUnsupportedMethod(() -> stmt.addBatch("insert into t values(1)"));

        stmt.close();
        SQLException ex = assertSqlException(() -> stmt.addBatch("insert into t values(1)"));
        assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, ex.getErrorCode());
    }

    private WSColumnPreparedStatement buildStmt(List<Field> fields) {
        return buildStmt(fields, true);
    }

    private WSColumnPreparedStatement buildStmt(List<Field> fields, boolean insert) {
        Stmt2PrepareResp resp = new Stmt2PrepareResp();
        resp.setInsert(insert);
        resp.setStmtId(1L);
        resp.setFields(fields);
        return new WSColumnPreparedStatement(transport, param, "test_db",
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

    private static List<Field> tbNameTagTagAndColFields() {
        List<Field> fields = new ArrayList<>();
        fields.add(field((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), TSDB_DATA_TYPE_VARCHAR));
        fields.add(field((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), TSDB_DATA_TYPE_INT));
        fields.add(field((byte) FieldBindType.TAOS_FIELD_TAG.getValue(), TSDB_DATA_TYPE_VARCHAR));
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

    private static Stmt2ColumnFieldBuffer[] getColumnBuffers(WSColumnPreparedStatement stmt) {
        try {
            java.lang.reflect.Field directField = WSColumnPreparedStatement.class.getDeclaredField("columnBuffers");
            directField.setAccessible(true);
            Stmt2ColumnFieldBuffer[] directBuffers = (Stmt2ColumnFieldBuffer[]) directField.get(stmt);
            if (directBuffers != null) {
                return directBuffers;
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
            // Fall back to the write-once batch state.
        }

        try {
            java.lang.reflect.Field batchStateField = WSColumnPreparedStatement.class.getDeclaredField("batchState");
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

    private static Stmt2ChunkSizingUtil.FieldBatchStats[] getBatchStats(WSColumnPreparedStatement stmt)
            throws Exception {
        java.lang.reflect.Field batchStatsField = WSColumnPreparedStatement.class.getDeclaredField("batchStats");
        batchStatsField.setAccessible(true);
        return (Stmt2ChunkSizingUtil.FieldBatchStats[]) batchStatsField.get(stmt);
    }

    private static void setNextBufferSpec(
            WSColumnPreparedStatement stmt,
            int index,
            Stmt2ChunkSizingUtil.BufferSpec spec) throws Exception {
        java.lang.reflect.Field nextSpecsField =
                WSColumnPreparedStatement.class.getDeclaredField("nextBufferSpecs");
        nextSpecsField.setAccessible(true);
        Stmt2ChunkSizingUtil.BufferSpec[] nextSpecs =
                (Stmt2ChunkSizingUtil.BufferSpec[]) nextSpecsField.get(stmt);
        nextSpecs[index] = spec;
    }

    private static Stmt2ChunkSizingUtil.BufferSpec[] getNextBufferSpecs(WSColumnPreparedStatement stmt)
            throws Exception {
        java.lang.reflect.Field nextSpecsField =
                WSColumnPreparedStatement.class.getDeclaredField("nextBufferSpecs");
        nextSpecsField.setAccessible(true);
        return (Stmt2ChunkSizingUtil.BufferSpec[]) nextSpecsField.get(stmt);
    }

    private static void setColumnBuffer(
            WSColumnPreparedStatement stmt,
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
            WSColumnPreparedStatement stmt,
            int index,
            int streak) throws Exception {
        java.lang.reflect.Field field = WSColumnPreparedStatement.class.getDeclaredField("underuseStreaks");
        field.setAccessible(true);
        int[] streaks = (int[]) field.get(stmt);
        streaks[index] = streak;
    }

    private static int getUnderuseStreak(WSColumnPreparedStatement stmt, int index) throws Exception {
        java.lang.reflect.Field field = WSColumnPreparedStatement.class.getDeclaredField("underuseStreaks");
        field.setAccessible(true);
        return ((int[]) field.get(stmt))[index];
    }

    private static void invokeUpdateNextBufferSpecsAfterSuccessfulBatch(WSColumnPreparedStatement stmt)
            throws Exception {
        java.lang.reflect.Method method = WSColumnPreparedStatement.class
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

    private static IllegalArgumentException assertIllegalArgumentException(ThrowingRunnable runnable) throws Exception {
        try {
            runnable.run();
            fail("Expected IllegalArgumentException");
            return null;
        } catch (IllegalArgumentException ex) {
            return ex;
        }
    }

    private static void assertSetObjectTypeError(WSColumnPreparedStatement stmt, int sqlType,
                                                 Object value, String message) throws Exception {
        SQLException ex = assertSqlException(() -> stmt.setObject(1, value, sqlType));
        assertTrue(ex.getMessage().contains(message));
    }

    private static void assertUnsupportedMethod(ThrowingRunnable runnable) throws Exception {
        SQLException ex = assertSqlException(runnable);
        assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, ex.getErrorCode());
    }

    private static int cachedChunkCount(Stmt2ColumnFieldBuffer buffer) throws Exception {
        java.lang.reflect.Field reusableField = Stmt2ColumnFieldBuffer.class.getDeclaredField("reusableValueBuffer");
        reusableField.setAccessible(true);
        Object reusable = reusableField.get(buffer);
        java.lang.reflect.Field cachedField = reusable.getClass().getDeclaredField("cachedStandardChunks");
        cachedField.setAccessible(true);
        return ((java.util.List<?>) cachedField.get(reusable)).size();
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
