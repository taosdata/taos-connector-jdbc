package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2BindExecRequestBuilder;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ChunkSizingUtil;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWBackendThreadInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWRawBlock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BLOB;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BOOL;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DOUBLE;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_FLOAT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_SMALLINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TINYINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UBIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARBINARY;
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
    public void serializationTask_preservesQueuedTableOrderInPayloadHeader() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        enqueueRows(info,
                row("d0", 1000L, 11),
                row("d1", 2000L, 12),
                row("d0", 3000L, 13));

        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 3, stmtInfo(), true).invoke();

        EWRawBlock block = info.getSerialQueue().poll();
        assertNotNull("task should have produced a serialized block", block);
        assertTrue("no serialization error expected", block.getLastError() == null);
        ByteBuf rawBlock = block.getByteBuf();
        try {
            int payloadOffset = Stmt2BindExecRequestBuilder.HEADER_SIZE;
            assertEquals(3, rawBlock.getIntLE(payloadOffset
                    + Stmt2ColumnBindSerializer.HEADER_ROW_COUNT_OFFSET));
            assertEquals(3, rawBlock.getIntLE(payloadOffset
                    + Stmt2ColumnBindSerializer.HEADER_TABLE_COUNT_OFFSET));
        } finally {
            releaseRawBlock(block);
            info.releaseReusableColumnBuffers();
        }
    }

    @Test
    public void serializationTask_serializesSupportedColumnValueConversions() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        StmtInfo stmt = conversionStmtInfo();
        enqueueRows(info,
                conversionRow("d0",
                        true,
                        false,
                        true,
                        false,
                        true,
                        new BigInteger("18446744073709551615"),
                        true,
                        false,
                        Instant.ofEpochMilli(1000L),
                        "binary-text",
                        new SerialBlob(new byte[]{1, 2, 3}),
                        new byte[]{4, 5}),
                conversionRow("d1",
                        0,
                        7,
                        8,
                        9,
                        10L,
                        11L,
                        1.5f,
                        2.5d,
                        Timestamp.from(Instant.ofEpochMilli(2000L)),
                        "binary-text-2".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        "blob-text",
                        new byte[]{6, 7}));

        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 2, stmt, true).invoke();

        EWRawBlock block = info.getSerialQueue().poll();
        assertNotNull("task should have produced a serialized block", block);
        assertTrue("no serialization error expected", block.getLastError() == null);
        ByteBuf rawBlock = block.getByteBuf();
        try {
            int payloadOffset = Stmt2BindExecRequestBuilder.HEADER_SIZE;
            assertEquals(2, rawBlock.getIntLE(payloadOffset
                    + Stmt2ColumnBindSerializer.HEADER_ROW_COUNT_OFFSET));
            assertEquals(2, rawBlock.getIntLE(payloadOffset
                    + Stmt2ColumnBindSerializer.HEADER_TABLE_COUNT_OFFSET));
            assertEquals(stmt.getFields().size(), rawBlock.getIntLE(payloadOffset
                    + Stmt2ColumnBindSerializer.HEADER_FIELD_COUNT_OFFSET));
        } finally {
            releaseRawBlock(block);
            info.releaseReusableColumnBuffers();
        }
    }

    @Test
    public void serializationTask_rejectsMissingTbname() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        Map<Integer, Column> row = new HashMap<>();
        row.put(2, new Column(1000L, TSDB_DATA_TYPE_TIMESTAMP, 2));
        row.put(3, new Column(11, TSDB_DATA_TYPE_INT, 3));
        info.getWriteQueue().put(row);

        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmtInfo(), true).invoke();

        EWRawBlock block = info.getSerialQueue().poll();
        assertNotNull("task must enqueue an error block for missing tbname", block);
        assertNotNull("error block must carry the exception", block.getLastError());
        assertTrue(block.getLastError().getMessage().contains("Missing bound column at index 1"));
        assertEquals(1, block.getRowCount());
        info.releaseReusableColumnBuffers();
    }

    @Test
    public void serializationTask_reusesProvidedBuffersAcrossBatches() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        enqueueRows(info,
                row("d0", 1000L, 11),
                row("d1", 2000L, 12));

        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 2, stmtInfo(), true).invoke();
        releaseRawBlock(info.getSerialQueue().poll());

        Stmt2ColumnFieldBuffer[] reused = info.getReusableColumnBuffers();
        Object reusableValueBuffer = getDeclaredField(reused[0], "reusableValueBuffer");
        assertNotNull("tbname buffer should use reusable chunked storage in EW reuse path", reusableValueBuffer);

        enqueueRows(info, row("d2", 3000L, 21));
        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmtInfo(), true).invoke();
        releaseRawBlock(info.getSerialQueue().poll());

        Stmt2ColumnFieldBuffer[] second = info.getReusableColumnBuffers();
        try {
            assertSame(reused, second);
            assertSame(reused[0], second[0]);
            assertSame(reusableValueBuffer, getDeclaredField(second[0], "reusableValueBuffer"));
        } finally {
            info.releaseReusableColumnBuffers();
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

    private static StmtInfo conversionStmtInfo() {
        Stmt2PrepareResp prepareResp = new Stmt2PrepareResp();
        prepareResp.setInsert(true);
        prepareResp.setStmtId(3L);
        prepareResp.setFields(Arrays.asList(
                tbnameField(),
                colField(TSDB_DATA_TYPE_BOOL),
                colField(TSDB_DATA_TYPE_TINYINT),
                colField(TSDB_DATA_TYPE_SMALLINT),
                colField(TSDB_DATA_TYPE_INT),
                colField(TSDB_DATA_TYPE_BIGINT),
                colField(TSDB_DATA_TYPE_UBIGINT),
                colField(TSDB_DATA_TYPE_FLOAT),
                colField(TSDB_DATA_TYPE_DOUBLE),
                colField(TSDB_DATA_TYPE_TIMESTAMP),
                colField(TSDB_DATA_TYPE_BINARY),
                colField(TSDB_DATA_TYPE_BLOB),
                colField(TSDB_DATA_TYPE_VARBINARY)));
        return new StmtInfo(prepareResp, "INSERT INTO ? VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    }

    private static Field colField(int fieldType) {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        field.setFieldType((byte) fieldType);
        return field;
    }

    private static Map<Integer, Column> conversionRow(String tbname, Object boolValue, Object tinyValue,
                                                      Object smallValue, Object intValue, Object bigValue,
                                                      Object uBigValue, Object floatValue, Object doubleValue,
                                                      Object tsValue, Object binaryValue, Object blobValue,
                                                      Object varbinaryValue) {
        Map<Integer, Column> row = new HashMap<>();
        row.put(1, new Column(tbname, TSDB_DATA_TYPE_BINARY, 1));
        row.put(2, new Column(boolValue, TSDB_DATA_TYPE_BOOL, 2));
        row.put(3, new Column(tinyValue, TSDB_DATA_TYPE_TINYINT, 3));
        row.put(4, new Column(smallValue, TSDB_DATA_TYPE_SMALLINT, 4));
        row.put(5, new Column(intValue, TSDB_DATA_TYPE_INT, 5));
        row.put(6, new Column(bigValue, TSDB_DATA_TYPE_BIGINT, 6));
        row.put(7, new Column(uBigValue, TSDB_DATA_TYPE_UBIGINT, 7));
        row.put(8, new Column(floatValue, TSDB_DATA_TYPE_FLOAT, 8));
        row.put(9, new Column(doubleValue, TSDB_DATA_TYPE_DOUBLE, 9));
        row.put(10, new Column(tsValue, TSDB_DATA_TYPE_TIMESTAMP, 10));
        row.put(11, new Column(binaryValue, TSDB_DATA_TYPE_BINARY, 11));
        row.put(12, new Column(blobValue, TSDB_DATA_TYPE_BLOB, 12));
        row.put(13, new Column(varbinaryValue, TSDB_DATA_TYPE_VARBINARY, 13));
        return row;
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
    public void serializationTask_enqueuesDetachedDirectPayload() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        StmtInfo stmt = wideStmtInfo();
        enqueueRows(info,
                wideRow("d0", 1000L, "hello", "world"),
                wideRow("d1", 2000L, "foo", "bar"));

        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 2, stmt, false).invoke();

        EWRawBlock block = info.getSerialQueue().poll();
        assertNotNull("task should have produced a serialized block", block);
        assertTrue("no serialization error expected", block.getLastError() == null);
        ByteBuf rawBlock = block.getByteBuf();
        try {
            assertTrue("bind-exec request should be header + detached payload composite",
                    rawBlock instanceof CompositeByteBuf);
            ByteBuf payloadComponent = ((CompositeByteBuf) rawBlock).component(1);
            assertTrue("WSEW queued payload should not wrap a heap byte[]", payloadComponent.isDirect());
        } finally {
            releaseRawBlock(block);
            info.releaseReusableColumnBuffers();
        }
    }

    @Test
    public void firstBatch_bootstrapsReusableBuffersWithDefaultSpec() throws Exception {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16);
        StmtInfo stmt = wideStmtInfo();
        enqueueRows(info, wideRow("d0", 1000L, "hello", "world"));

        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmt, true).invoke();
        releaseRawBlock(info.getSerialQueue().poll());

        Stmt2ChunkSizingUtil.BufferSpec bootstrap = Stmt2ChunkSizingUtil.bootstrapSpec();
        assertBufferSpecEquals(bootstrap, info.getReusableColumnBuffers()[2].currentReusableSpec());
        assertBufferSpecEquals(bootstrap, info.getReusableColumnBuffers()[3].currentReusableSpec());
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

        Stmt2ChunkSizingUtil.BufferSpec bootstrap = Stmt2ChunkSizingUtil.bootstrapSpec();
        Stmt2ChunkSizingUtil.BufferSpec grown = info.getNextBufferSpecs()[2];
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
        Stmt2ChunkSizingUtil.BufferSpec largeSpec = new Stmt2ChunkSizingUtil.BufferSpec(64 * 1024, 4);
        info.setNextBufferSpecs(new Stmt2ChunkSizingUtil.BufferSpec[] {null, null, largeSpec, largeSpec});
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
        Stmt2ChunkSizingUtil.BufferSpec largeSpec = new Stmt2ChunkSizingUtil.BufferSpec(64 * 1024, 4);
        Stmt2ChunkSizingUtil.BufferSpec smallSpec = Stmt2ChunkSizingUtil.bootstrapSpec();
        info.setNextBufferSpecs(new Stmt2ChunkSizingUtil.BufferSpec[] {null, null, largeSpec, largeSpec});
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

    private static void assertBufferSpecEquals(Stmt2ChunkSizingUtil.BufferSpec expected,
                                               Stmt2ChunkSizingUtil.BufferSpec actual) {
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
        batchStatsField.set(task, new Stmt2ChunkSizingUtil.FieldBatchStats[2]);

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
     * Buffer initialization is eager now: initialization failures should fail task
     * construction instead of being deferred to {@code compute()} as an error block.
     */
    @Test(expected = IllegalStateException.class)
    public void serializationTask_illegalStateExceptionFailsTaskConstruction() {
        EWBackendThreadInfo info = new EWBackendThreadInfo(16, 16) {
            @Override
            public void releaseReusableColumnBuffers() {
                throw new IllegalStateException("injected buffer-init failure (simulates primeReusableBuffer ISE)");
            }
        };
        StmtInfo stmt = wideStmtInfo();

        new WSEWColumnPreparedStatement.ColumnarWSEWSerializationTask(info, 1, stmt, false);
    }

    private static String repeated(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }
}
