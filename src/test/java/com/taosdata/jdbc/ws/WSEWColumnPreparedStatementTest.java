package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.WSEWChunkSizingUtil;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWBackendThreadInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWRawBlock;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class WSEWColumnPreparedStatementTest {

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }

    @Test
    public void buildColumnBuffersFromQueuedRows_preservesQueuedTableOrder() throws Exception {
        List<Map<Integer, Column>> rows = Arrays.asList(
                row("d0", 1000L, 11),
                row("d1", 2000L, 12),
                row("d0", 3000L, 13));

        Stmt2ColumnFieldBuffer[] buffers = null;
        try {
            buffers = WSEWColumnPreparedStatement.buildColumnBuffersFromQueuedRows(rows, stmtInfo());

            assertEquals(3, buffers[0].getRowCount());
            assertEquals(3, buffers[1].getRowCount());
            assertEquals(3, buffers[2].getRowCount());
            assertEquals(3, buffers[0].computeTableCount());
        } finally {
            if (buffers != null) {
                for (Stmt2ColumnFieldBuffer buffer : buffers) {
                    if (buffer != null) {
                        buffer.release();
                    }
                }
            }
        }
    }

    @Test(expected = SQLException.class)
    public void buildColumnBuffersFromQueuedRows_rejectsMissingTbname() throws Exception {
        Map<Integer, Column> row = new HashMap<>();
        row.put(2, new Column(1000L, TSDB_DATA_TYPE_TIMESTAMP, 2));
        row.put(3, new Column(11, TSDB_DATA_TYPE_INT, 3));
        WSEWColumnPreparedStatement.buildColumnBuffersFromQueuedRows(
                Collections.singletonList(row),
                stmtInfo());
    }

    @Test
    public void buildColumnBuffersFromQueuedRows_reusesProvidedBuffersAcrossBatches() throws Exception {
        Method method = WSEWColumnPreparedStatement.class.getDeclaredMethod(
                "buildColumnBuffersFromQueuedRows",
                List.class,
                StmtInfo.class,
                Stmt2ColumnFieldBuffer[].class);
        method.setAccessible(true);

        List<Map<Integer, Column>> firstRows = Arrays.asList(
                row("d0", 1000L, 11),
                row("d1", 2000L, 12));
        List<Map<Integer, Column>> secondRows = Collections.singletonList(
                row("d2", 3000L, 21));

        Stmt2ColumnFieldBuffer[] reused = null;
        Stmt2ColumnFieldBuffer[] expected = null;
        try {
            reused = (Stmt2ColumnFieldBuffer[]) method.invoke(null, firstRows, stmtInfo(), null);
            Object reusableValueBuffer = getDeclaredField(reused[0], "reusableValueBuffer");
            assertNotNull("tbname buffer should switch to reusable chunked storage in EW reuse path", reusableValueBuffer);

            Stmt2ColumnFieldBuffer[] second = (Stmt2ColumnFieldBuffer[]) method.invoke(null, secondRows, stmtInfo(), reused);
            assertSame(reused, second);
            assertSame(reused[0], second[0]);
            assertSame(reusableValueBuffer, getDeclaredField(second[0], "reusableValueBuffer"));

            expected = WSEWColumnPreparedStatement.buildColumnBuffersFromQueuedRows(secondRows, stmtInfo());
            assertArrayEquals(
                    Stmt2ColumnBindSerializer.serialize(expected),
                    Stmt2ColumnBindSerializer.serialize(second));
        } finally {
            releaseBuffers(expected);
            releaseBuffers(reused);
        }
    }

    private static Map<Integer, Column> row(String tbname, long ts, int value) {
        Map<Integer, Column> row = new HashMap<>();
        row.put(1, new Column(tbname, TSDB_DATA_TYPE_BINARY, 1));
        row.put(2, new Column(ts, TSDB_DATA_TYPE_TIMESTAMP, 2));
        row.put(3, new Column(value, TSDB_DATA_TYPE_INT, 3));
        return row;
    }

    private static StmtInfo stmtInfo() {
        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setInsert(true);
        prepareResp.setStmtId(1L);
        prepareResp.setFields(Arrays.asList(tbnameField(), tsField(), valueField()));
        return new StmtInfo(prepareResp, "INSERT INTO ? VALUES (?, ?)");
    }

    private static Field tbnameField() {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue());
        field.setFieldType((byte) TSDB_DATA_TYPE_BINARY);
        return field;
    }

    private static Field tsField() {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        field.setFieldType((byte) TSDB_DATA_TYPE_TIMESTAMP);
        return field;
    }

    private static Field valueField() {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        field.setFieldType((byte) TSDB_DATA_TYPE_INT);
        return field;
    }

    private static Object getDeclaredField(Object target, String fieldName) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void releaseBuffers(Stmt2ColumnFieldBuffer[] buffers) {
        if (buffers == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer buffer : buffers) {
            if (buffer != null) {
                buffer.release();
            }
        }
    }

    @Test
    public void buildColumnBuffersFromQueuedRows_collectsStatsDuringAppend() throws Exception {
        Method method = WSEWColumnPreparedStatement.class.getDeclaredMethod(
                "buildColumnBuffersFromQueuedRows",
                List.class,
                StmtInfo.class,
                Stmt2ColumnFieldBuffer[].class,
                WSEWChunkSizingUtil.FieldBatchStats[].class);
        method.setAccessible(true);

        List<Map<Integer, Column>> rows = Arrays.asList(
                wideRow("d0", 1000L, "aaaa", "bbbb"),
                wideRow("d1", 2000L, "cccc", "dddd"));
        WSEWChunkSizingUtil.FieldBatchStats[] stats =
                new WSEWChunkSizingUtil.FieldBatchStats[] {null, null, null, null};

        Stmt2ColumnFieldBuffer[] buffers = null;
        try {
            buffers = (Stmt2ColumnFieldBuffer[]) method.invoke(null, rows, wideStmtInfo(), null, stats);

            assertNotNull(buffers);
            assertNotNull("tbname stats must be collected (slot 0)", stats[0]);
            assertEquals(2, stats[0].getRowsWritten());
            assertTrue(stats[0].getObservedValueBytes() >= 2);
            assertNotNull(stats[2]);
            assertEquals(2, stats[2].getRowsWritten());
            assertTrue(stats[2].getObservedValueBytes() >= 8);
        } finally {
            releaseBuffers(buffers);
        }
    }

    @Test
    public void buildColumnBuffersFromQueuedRows_rejectsUndersizedStatsArray() throws Exception {
        Method method = WSEWColumnPreparedStatement.class.getDeclaredMethod(
                "buildColumnBuffersFromQueuedRows",
                List.class,
                StmtInfo.class,
                Stmt2ColumnFieldBuffer[].class,
                WSEWChunkSizingUtil.FieldBatchStats[].class);
        method.setAccessible(true);

        List<Map<Integer, Column>> rows = Collections.singletonList(wideRow("d0", 1000L, "a", "b"));
        // wideStmtInfo has 4 fields; pass only 2 slots — must be rejected
        WSEWChunkSizingUtil.FieldBatchStats[] tooShort =
                new WSEWChunkSizingUtil.FieldBatchStats[2];
        try {
            method.invoke(null, rows, wideStmtInfo(), null, tooShort);
            throw new AssertionError("Expected IllegalArgumentException for undersized stats array");
        } catch (java.lang.reflect.InvocationTargetException e) {
            assertTrue("Expected IllegalArgumentException",
                    e.getCause() instanceof IllegalArgumentException);
        }
    }

    private static Map<Integer, Column> wideRow(String tbname, long ts, String v1, String v2) {
        Map<Integer, Column> row = new HashMap<>();
        row.put(1, new Column(tbname, TSDB_DATA_TYPE_BINARY, 1));
        row.put(2, new Column(ts, TSDB_DATA_TYPE_TIMESTAMP, 2));
        row.put(3, new Column(v1, TSDB_DATA_TYPE_BINARY, 3));
        row.put(4, new Column(v2, TSDB_DATA_TYPE_BINARY, 4));
        return row;
    }

    private static StmtInfo wideStmtInfo() {
        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setInsert(true);
        prepareResp.setStmtId(2L);
        prepareResp.setFields(Arrays.asList(tbnameField(), tsField(), binaryField(), binaryField()));
        return new StmtInfo(prepareResp, "INSERT INTO ? VALUES (?, ?, ?)");
    }

    @Test
    public void serializationTask_collectsStatsDuringRealAppend() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        StmtInfo stmt = wideStmtInfo();
        List<Map<Integer, Column>> rows = Arrays.asList(
                wideRow("d0", 1000L, "hello", "world"),
                wideRow("d1", 2000L, "foo",   "bar"));
        for (Map<Integer, Column> row : rows) {
            info.getWriteQueue().put(row);
        }

        WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask task =
                new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 2, stmt, false);
        task.invoke();

        EWRawBlock block = info.getSerialQueue().poll();
        assertNotNull("task should have produced a serialized block", block);
        assertTrue("no serialization error expected", block.getLastError() == null);

        // Verify the real append path routes through the stats-aware overload
        assertNotNull("tbname stats must be populated by runtime path (slot 0)", task.batchStats[0]);
        assertEquals(2, task.batchStats[0].getRowsWritten());
        assertNotNull("binary col stats must be populated by runtime path (slot 2)", task.batchStats[2]);
        assertEquals(2, task.batchStats[2].getRowsWritten());
        assertNotNull("binary col stats must be populated by runtime path (slot 3)", task.batchStats[3]);
        assertEquals(2, task.batchStats[3].getRowsWritten());

        if (block.getByteBuf() != null) {
            block.getByteBuf().release();
        }
        info.releaseReusableColumnBuffers();
    }

    @Test
    public void nextBatch_growsImmediatelyAfterLargeObservedBatch() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        StmtInfo stmt = wideStmtInfo();
        enqueueRows(info,
                wideRow("d0", 1000L, repeated('a', 20_000), "tiny"),
                wideRow("d1", 2000L, repeated('b', 20_000), "tiny"));

        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 2, stmt, true).invoke();
        releaseRawBlock(info.getSerialQueue().poll());

        WSEWChunkSizingUtil.BufferSpec bootstrap = WSEWChunkSizingUtil.bootstrapSpec();
        WSEWChunkSizingUtil.BufferSpec grown = info.getNextBufferSpecs()[2];
        assertNotNull(grown);
        assertTrue(grown.getChunkBytes() > bootstrap.getChunkBytes());
        assertTrue(grown.getReusableChunkCount() > bootstrap.getReusableChunkCount());
        assertBufferSpecEquals(bootstrap, info.getReusableColumnBuffers()[2].currentReusableSpec());

        enqueueRows(info, wideRow("d2", 3000L, "small", "tiny"), wideRow("d3", 4000L, "tiny", "tiny"));
        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 2, stmt, true).invoke();
        releaseRawBlock(info.getSerialQueue().poll());

        assertBufferSpecEquals(grown, info.getReusableColumnBuffers()[2].currentReusableSpec());
        info.releaseReusableColumnBuffers();
    }

    @Test
    public void nextBatch_reusesWhenCapacityIsEnoughAndChunksAreNotTooFragmented() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        StmtInfo stmt = wideStmtInfo();
        WSEWChunkSizingUtil.BufferSpec largeSpec = new WSEWChunkSizingUtil.BufferSpec(64 * 1024, 4);
        info.setNextBufferSpecs(new WSEWChunkSizingUtil.BufferSpec[] {null, null, largeSpec, largeSpec});
        info.setUnderuseStreaks(new int[stmt.getFields().size()]);

        enqueueRows(info, wideRow("d0", 1000L, "a", "b"));
        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmt, true).invoke();
        releaseRawBlock(info.getSerialQueue().poll());

        assertBufferSpecEquals(largeSpec, info.getReusableColumnBuffers()[2].currentReusableSpec());
        assertBufferSpecEquals(largeSpec, info.getNextBufferSpecs()[2]);
        assertEquals(1, info.getUnderuseStreaks()[2]);
        info.releaseReusableColumnBuffers();
    }

    @Test
    public void nextBatch_shrinksOnlyAfterHundredUnderusedBatches() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        StmtInfo stmt = wideStmtInfo();
        WSEWChunkSizingUtil.BufferSpec largeSpec = new WSEWChunkSizingUtil.BufferSpec(64 * 1024, 4);
        WSEWChunkSizingUtil.BufferSpec smallSpec = WSEWChunkSizingUtil.bootstrapSpec();
        info.setNextBufferSpecs(new WSEWChunkSizingUtil.BufferSpec[] {null, null, largeSpec, largeSpec});
        info.setUnderuseStreaks(new int[stmt.getFields().size()]);

        for (int i = 0; i < 99; i++) {
            enqueueRows(info, wideRow("d" + i, 1000L + i, "a", "b"));
            new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmt, true).invoke();
            releaseRawBlock(info.getSerialQueue().poll());
        }

        assertBufferSpecEquals(largeSpec, info.getNextBufferSpecs()[2]);
        assertEquals(99, info.getUnderuseStreaks()[2]);

        enqueueRows(info, wideRow("d99", 1099L, "a", "b"));
        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmt, true).invoke();
        releaseRawBlock(info.getSerialQueue().poll());

        assertBufferSpecEquals(smallSpec, info.getNextBufferSpecs()[2]);
        assertEquals(0, info.getUnderuseStreaks()[2]);
        assertBufferSpecEquals(largeSpec, info.getReusableColumnBuffers()[2].currentReusableSpec());
        info.releaseReusableColumnBuffers();
    }

    private static Field binaryField() {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        field.setFieldType((byte) TSDB_DATA_TYPE_BINARY);
        return field;
    }

    private static void enqueueRows(EWBackendThreadInfo info, Map<Integer, Column>... rows) throws Exception {
        for (Map<Integer, Column> row : rows) {
            info.getWriteQueue().put(row);
        }
    }

    private static void releaseRawBlock(EWRawBlock block) {
        if (block != null && block.getByteBuf() != null) {
            block.getByteBuf().release();
        }
    }

    private static void assertBufferSpecEquals(WSEWChunkSizingUtil.BufferSpec expected,
                                               WSEWChunkSizingUtil.BufferSpec actual) {
        assertNotNull(actual);
        assertEquals(expected.getChunkBytes(), actual.getChunkBytes());
        assertEquals(expected.getReusableChunkCount(), actual.getReusableChunkCount());
    }

    /**
     * Regression: {@code IllegalArgumentException} thrown inside the inner try-block of
     * {@code ColumnarWSEWSerializationTask.compute()} (e.g. from {@code updateNextBufferSpecs}
     * sizing overflow) must enqueue an error block rather than silently consuming rows and
     * leaving {@code waitWriteCompleted()} hanging.
     *
     * We inject the exception by replacing the task's {@code batchStats} field with an
     * undersized array via reflection, which causes {@code buildColumnBuffersFromQueuedRows}
     * to throw {@code IllegalArgumentException} from its precondition guard — the same
     * unchecked-exception path that a sizing overflow would follow.
     */
    @Test
    public void serializationTask_illegalArgumentExceptionProducesErrorBlockNotHang() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        StmtInfo stmt = wideStmtInfo(); // 4 fields
        enqueueRows(info, wideRow("d0", 1000L, "hello", "world"));

        WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask task =
                new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmt, false);

        // Replace batchStats with a 2-element array (wideStmtInfo has 4 fields).
        // buildColumnBuffersFromQueuedRows rejects it with IllegalArgumentException, which
        // must be caught and routed to the error-block path, not escape the inner try.
        java.lang.reflect.Field batchStatsField =
                WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask.class
                        .getDeclaredField("batchStats");
        batchStatsField.setAccessible(true);
        batchStatsField.set(task, new WSEWChunkSizingUtil.FieldBatchStats[2]);

        task.invoke();

        EWRawBlock block = info.getSerialQueue().poll();
        assertNotNull("task must enqueue a block (not hang) when IllegalArgumentException is thrown", block);
        assertNotNull("error block must carry the exception", block.getLastError());
        assertTrue("error message must reference sizing or IllegalArgumentException",
                block.getLastError().getMessage() != null
                        && block.getLastError().getMessage().length() > 0);
        assertEquals("rows must still be accounted for", 1, block.getRowCount());
    }

    /**
     * Regression: {@code IllegalStateException} thrown inside the inner try-block of
     * {@code ColumnarWSEWSerializationTask.compute()} (e.g. from {@code primeReusableBuffer}
     * or {@code ensureReusableColumnBuffers}) must enqueue an error block rather than
     * escaping unchecked and leaving {@code waitWriteCompleted()} hanging.
     *
     * We inject the exception by overriding {@code releaseReusableColumnBuffers()} in an
     * anonymous {@code EWBackendThreadInfo} subclass.  When {@code reusableColumnBuffers} is
     * null the buffer-spec match check fails, causing {@code ensureReusableColumnBuffers} to
     * call {@code releaseReusableColumnBuffers()} — which is where our injected
     * {@code IllegalStateException} fires.
     *
     * <p>Note: triggering the real {@code primeReusableBuffer} ISE path (which wraps a
     * {@code SQLException} from {@code appendBytes}) requires making a Netty buffer
     * allocation fail mid-write — not practical without mocking.  The anonymous-subclass
     * approach exercises the same catch-block in {@code compute()}, giving equivalent
     * regression coverage.
     */
    @Test
    public void serializationTask_illegalStateExceptionProducesErrorBlockNotHang() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16) {
            @Override
            public void releaseReusableColumnBuffers() {
                throw new IllegalStateException("injected buffer-init failure (simulates primeReusableBuffer ISE)");
            }
        };
        StmtInfo stmt = wideStmtInfo();
        enqueueRows(info, wideRow("d0", 1000L, "hello", "world"));

        WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask task =
                new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmt, false);
        task.invoke();

        EWRawBlock block = info.getSerialQueue().poll();
        assertNotNull("task must enqueue a block (not hang) when ISE escapes buffer init", block);
        assertNotNull("error block must carry the exception", block.getLastError());
        assertTrue("error message must mention buffer initialization",
                block.getLastError().getMessage() != null
                        && block.getLastError().getMessage().length() > 0);
        assertEquals("rows must still be accounted for", 1, block.getRowCount());
    }

    private static String repeated(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }
}
