package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.WSEWChunkSizingUtil;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
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

    private static Field binaryField() {
        Field field = new Field();
        field.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        field.setFieldType((byte) TSDB_DATA_TYPE_BINARY);
        return field;
    }
}
