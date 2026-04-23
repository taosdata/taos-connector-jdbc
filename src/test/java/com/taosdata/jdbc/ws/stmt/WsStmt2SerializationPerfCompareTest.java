package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.WSRowPreparedStatement;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import io.netty.buffer.CompositeByteBuf;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.HEADER_SIZE;
import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.HEADER_TOTAL_LENGTH_OFFSET;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Serialization comparison harness: columnar (new stmt2 path) vs row-serial
 * ({@link WSRowPreparedStatement} line-mode path).
 *
 * <h3>Purpose</h3>
 * <p>Verifies that {@link Stmt2ColumnBindSerializer} produces a well-formed binary payload
 * and provides a repeatable in-process timing harness that illustrates the relative effort
 * of the two serialization strategies without imposing a hard performance SLA.
 *
 * <h3>Row-serial baseline</h3>
 * <p>The row-serial measurement uses the production class {@link WSRowPreparedStatement}
 * directly: fields are bound via its public setter API, and the internal raw block is
 * extracted via reflection (calling private {@code buffersStopWrite()} then
 * {@code getRawBlock()}) so that no live server is required.  This produces an
 * apples-to-apples comparison because both paths serialise the same logical N-row dataset.
 *
 * <h3>Assertion philosophy</h3>
 * <ul>
 *   <li>Byte counts are deterministic: exact assertions are used.</li>
 *   <li>Timing is reported via {@code System.out} only; no wall-clock SLA is asserted
 *       to prevent flaky failures on slow CI runners.</li>
 *   <li>The columnar path must never produce more bytes per row than a 2-column payload
 *       warrants (HEADER + 2 column blocks), ensuring no runaway growth.</li>
 * </ul>
 */
public class WsStmt2SerializationPerfCompareTest {

    // -----------------------------------------------------------------------
    // Constants used in comparisons
    // -----------------------------------------------------------------------

    /**
     * Fixed-width column block header size:
     *   totalLength(4) + type(4) + num(4) + isNull[N](N) + haveLength(1) + bufferLength(4)
     * For N == ROWS: 17 + ROWS  (null array is row-major byte array)
     */
    private static final int FIXED_BLOCK_OVERHEAD = 17; // not counting null bytes or value bytes

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Stmt2FieldMeta colMeta(int fieldType) {
        return Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) fieldType, (byte) 0);
    }

    private static int readLE32(byte[] buf, int off) {
        return (buf[off] & 0xFF)
                | ((buf[off + 1] & 0xFF) << 8)
                | ((buf[off + 2] & 0xFF) << 16)
                | ((buf[off + 3] & 0xFF) << 24);
    }

    // -----------------------------------------------------------------------
    // Row-serial baseline via real WSRowPreparedStatement
    // -----------------------------------------------------------------------

    /**
     * Builds a {@link WSRowPreparedStatement} for a 2-field (TIMESTAMP, INT) insert,
     * binds {@code rows} rows via the public setter API, and returns the size in bytes
     * of the raw binary block it would send over the wire.
     *
     * <p>The raw block is extracted via reflection to avoid the need for a live server.
     * No transport calls are made; the mock transport is never actually invoked for send.
     *
     * <p>For N rows of (TIMESTAMP, INT) the expected size is:
     * <pre>
     *   header(58) + colLensBuf(N*4) + colsBuf(N*48) = 58 + N*52
     * </pre>
     * where each timestamp serialises to 26 bytes and each int to 22 bytes.
     *
     * @param rows number of rows to bind
     * @return byte count of the produced raw block
     */
    private static int wsRowPayloadBytes(int rows) throws Exception {
        Transport transport = mock(Transport.class);
        ConnectionParam param = mock(ConnectionParam.class);
        when(transport.getReconnectCount()).thenReturn(0);
        when(transport.isConnected()).thenReturn(true);
        when(transport.isClosed()).thenReturn(false);
        when(transport.getConnectionParam()).thenReturn(param);
        when(param.getRequestTimeout()).thenReturn(30_000);
        when(param.getZoneId()).thenReturn(null);
        when(param.getRetryTimes()).thenReturn(1);
        when(param.isEnableAutoConnect()).thenReturn(false);
        when(param.getDatabase()).thenReturn("testdb");

        AbstractConnection conn = mock(AbstractConnection.class);

        Field tsField = new Field();
        tsField.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        tsField.setFieldType((byte) TSDB_DATA_TYPE_TIMESTAMP);
        Field intField = new Field();
        intField.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        intField.setFieldType((byte) TSDB_DATA_TYPE_INT);

        Stmt2PrepareResp prepResp = new Stmt2PrepareResp();
        prepResp.setCode(0);
        prepResp.setInsert(true);
        prepResp.setStmtId(0L); // 0 → close() does not call transport
        prepResp.setFields(Arrays.asList(tsField, intField));

        WSRowPreparedStatement stmt = new WSRowPreparedStatement(
                transport, param, "testdb", conn,
                "INSERT INTO t VALUES(?,?)", 0L, prepResp);

        for (int i = 0; i < rows; i++) {
            stmt.setTimestamp(1, new Timestamp(1700000000000L + (long) i * 1000L));
            stmt.setInt(2, i);
            stmt.addBatch();
        }

        // Extract raw block via reflection (private buffersStopWrite + getRawBlock)
        Method buffersStopWrite =
                WSRowPreparedStatement.class.getDeclaredMethod("buffersStopWrite");
        buffersStopWrite.setAccessible(true);
        buffersStopWrite.invoke(stmt);

        Method getRawBlock =
                WSRowPreparedStatement.class.getDeclaredMethod("getRawBlock");
        getRawBlock.setAccessible(true);
        CompositeByteBuf rawBlock = (CompositeByteBuf) getRawBlock.invoke(stmt);

        int size = rawBlock.readableBytes();
        rawBlock.release();
        return size;
    }

    // -----------------------------------------------------------------------
    // 1. Deterministic byte-count comparisons
    // -----------------------------------------------------------------------

    @Test
    public void testColumnarPayloadSize_singleRow_twoColumns() throws SQLException {
        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));

        tsCol.appendTimestamp(1700000000000L);
        intCol.appendInt(42);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
                new Stmt2ColumnFieldBuffer[]{tsCol, intCol});

        // Expected: HEADER(20) + block_ts(17+1+8=26) + block_int(17+1+4=22) = 68
        int expectedTsBlock = FIXED_BLOCK_OVERHEAD + 1 /* null[1] */ + 8 /* ts bytes */;
        int expectedIntBlock = FIXED_BLOCK_OVERHEAD + 1 /* null[1] */ + 4 /* int bytes */;
        int expectedTotal = HEADER_SIZE + expectedTsBlock + expectedIntBlock;

        assertEquals("single-row 2-column payload size", expectedTotal, payload.length);
        assertEquals("total_length header", expectedTotal, readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
    }

    @Test
    public void testColumnarPayloadSize_scalesLinearlyWithRows() throws SQLException {
        int[] rowCounts = {1, 10, 100};
        for (int rows : rowCounts) {
            Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
            Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));

            for (int i = 0; i < rows; i++) {
                tsCol.appendTimestamp(1700000000000L + i * 1000L);
                intCol.appendInt(i);
            }

            byte[] payload = Stmt2ColumnBindSerializer.serialize(
                    new Stmt2ColumnFieldBuffer[]{tsCol, intCol});

            // block_ts = 17 + rows(null) + 8*rows(values) = 17 + 9*rows
            // block_int = 17 + rows(null) + 4*rows(values) = 17 + 5*rows
            int expectedSize = HEADER_SIZE
                    + (FIXED_BLOCK_OVERHEAD + rows + rows * 8)
                    + (FIXED_BLOCK_OVERHEAD + rows + rows * 4);

            assertEquals("payload size for " + rows + " rows",
                    expectedSize, payload.length);
        }
    }

    @Test
    public void testRowSerialPayloadSize_singleRow() throws Exception {
        int actual = wsRowPayloadBytes(1);
        // header(58) + colLensBuf(1*4) + colsBuf(1*(26+22)) = 58 + 52 = 110
        assertEquals("WSRowPreparedStatement 1-row payload", 58 + 1 * 52, actual);
    }

    @Test
    public void testRowSerialPayloadSize_scalesLinearly() throws Exception {
        // Expected: 58 + N*52 for N rows of (TIMESTAMP, INT)
        for (int rows : new int[]{1, 10, 100}) {
            int actual = wsRowPayloadBytes(rows);
            assertEquals("WSRowPreparedStatement " + rows + "-row payload", 58 + rows * 52, actual);
        }
    }

    // -----------------------------------------------------------------------
    // 2. Bytes-per-row comparison (columnar vs row-serial)
    // -----------------------------------------------------------------------

    @Test
    public void testBytesPerRow_columnarHasHigherFixedOverheadButLessPerRowForLargeN()
            throws Exception {
        // For small N, columnar has higher per-row cost due to header overhead.
        // For large N, per-row overhead approaches (8+1) ts + (4+1) int = 14 bytes/row
        // vs WSRowPreparedStatement ≈ 52 bytes/row (per-column metadata per row).
        // This test validates that both approaches grow linearly with N and that
        // the columnar byte count / row is bounded above by a reasonable constant.

        int rows = 1000;
        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));

        for (int i = 0; i < rows; i++) {
            tsCol.appendTimestamp(1700000000000L + i * 1000L);
            intCol.appendInt(i);
        }

        byte[] columnarPayload = Stmt2ColumnBindSerializer.serialize(
                new Stmt2ColumnFieldBuffer[]{tsCol, intCol});
        int wsRowBytes = wsRowPayloadBytes(rows);

        double columnarBytesPerRow = (double) columnarPayload.length / rows;
        double rowSerialBytesPerRow = (double) wsRowBytes / rows;

        System.out.printf("[PerfCompare] %d rows | columnar: %d bytes (%.1f/row) | " +
                        "row-serial: %d bytes (%.1f/row)%n",
                rows,
                columnarPayload.length, columnarBytesPerRow,
                wsRowBytes, rowSerialBytesPerRow);

        // Columnar payload should be < 25 bytes/row for ts+int (fixed-width only).
        // Actual = (20 + 26 + 22) / 1000 ≈ 14 bytes/row – well within this bound.
        assertTrue("columnar bytes/row must be bounded above 5",
                columnarBytesPerRow > 5);
        assertTrue("columnar bytes/row for ts+int must be < 25",
                columnarBytesPerRow < 25);

        // WSRowPreparedStatement per-row cost is ~52 bytes; columnar is more compact
        assertTrue("columnar must be more compact than WSRowPreparedStatement for 1000 rows",
                columnarPayload.length < wsRowBytes);
    }

    // -----------------------------------------------------------------------
    // 3. In-process timing harness (no SLA assertions)
    // -----------------------------------------------------------------------

    @Test
    public void testTimingHarness_columnarVsRowSerial_noSlaAssertion() throws Exception {
        final int rows = 10_000;
        final int warmupRounds = 3;
        final int measureRounds = 5;

        // Warm-up
        for (int w = 0; w < warmupRounds; w++) {
            buildColumnarPayload(rows);
            wsRowPayloadBytes(rows);
        }

        // Measure columnar
        long columnarNs = 0;
        for (int r = 0; r < measureRounds; r++) {
            long t0 = System.nanoTime();
            byte[] p = buildColumnarPayload(rows);
            columnarNs += System.nanoTime() - t0;
            assertNotNull(p);
        }

        // Measure WSRowPreparedStatement row-serial
        long rowSerialNs = 0;
        for (int r = 0; r < measureRounds; r++) {
            long t0 = System.nanoTime();
            int size = wsRowPayloadBytes(rows);
            rowSerialNs += System.nanoTime() - t0;
            assertTrue("wsRowPayloadBytes must return a positive size", size > 0);
        }

        long avgColumnarUs = columnarNs / measureRounds / 1_000;
        long avgRowSerialUs = rowSerialNs / measureRounds / 1_000;

        System.out.printf("[PerfCompare] %d rows | columnar avg: %d µs | " +
                        "row-serial avg: %d µs%n",
                rows, avgColumnarUs, avgRowSerialUs);

        // Only sanity-check: both paths complete in under 30 seconds (30_000_000 µs)
        // to catch infinite-loop or allocation catastrophe without imposing a tight SLA.
        assertTrue("columnar serialisation must complete in < 30 s",
                avgColumnarUs < 30_000_000L);
        assertTrue("row-serial serialisation must complete in < 30 s",
                avgRowSerialUs < 30_000_000L);
    }

    // -----------------------------------------------------------------------
    // 4. Variable-width column comparison
    // -----------------------------------------------------------------------

    @Test
    public void testVarWidthPayloadSize_exactCalculation() throws SQLException {
        // Row values: "a" (1 byte), "bb" (2 bytes), "ccc" (3 bytes)
        String[] values = {"a", "bb", "ccc"};
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                        (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));
        for (String v : values) {
            buf.appendBytes(v.getBytes(StandardCharsets.UTF_8));
        }

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});

        // block: 4(totalLen) + 4(type) + 4(num) + 3(isNull) + 1(haveLen) + 3*4(lengths) + 4(bufLen) + (1+2+3)
        int expectedBlock = 4 + 4 + 4 + 3 + 1 + (3 * 4) + 4 + 6;
        int expectedTotal = HEADER_SIZE + expectedBlock;
        assertEquals("var-width payload exact size", expectedTotal, payload.length);
    }

    @Test
    public void testVarWidthPayloadSize_rowSerialComparison() throws Exception {
        // Columnar adds a per-row length[] array (4 bytes each) but groups by column.
        // For N=100 rows of 5-byte strings:
        //   columnar block overhead per row: 1(null) + 4(length) = 5 extra bytes / row
        // This test validates that the columnar payload length matches the formula.

        int rows = 100;
        byte[] fixed5 = "hello".getBytes(StandardCharsets.UTF_8);

        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(
                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                        (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));
        for (int i = 0; i < rows; i++) {
            buf.appendBytes(fixed5);
        }

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});

        // Expected block: 4+4+4 + rows(null) + 1 + rows*4(lengths) + 4 + rows*5(values)
        int expectedBlock = 17 + rows + rows * 4 + rows * 5;
        assertEquals("columnar payload with " + rows + " uniform varchar rows",
                HEADER_SIZE + expectedBlock, payload.length);

        System.out.printf("[PerfCompare] %d varchar(5) rows | columnar: %d bytes%n",
                rows, payload.length);
    }

    // -----------------------------------------------------------------------
    // 5. Structural output-shape validation
    // -----------------------------------------------------------------------

    @Test
    public void testOutputShape_twoColumnPayload_fieldCountAndOffset() throws SQLException {
        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        tsCol.appendTimestamp(1700000000000L);
        intCol.appendInt(0);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
                new Stmt2ColumnFieldBuffer[]{tsCol, intCol});

        assertEquals("field_count", 2, readLE32(payload, 12));
        assertEquals("field_offset always HEADER_SIZE",
                HEADER_SIZE, readLE32(payload, 16));
    }

    @Test
    public void testOutputShape_columnarIsColumnMajor_twoColumnsIndependent() throws SQLException {
        // Verify that ts block appears at HEADER_SIZE and int block follows immediately.
        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        tsCol.appendTimestamp(1700000000001L);
        intCol.appendInt(999);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
                new Stmt2ColumnFieldBuffer[]{tsCol, intCol});

        // ts block total_length
        int tsBlockLen = readLE32(payload, HEADER_SIZE);
        // int block starts right after ts block
        int intBlockStart = HEADER_SIZE + tsBlockLen;

        // Sanity: int block type should be TSDB_DATA_TYPE_INT
        assertEquals("int block type", TSDB_DATA_TYPE_INT, readLE32(payload, intBlockStart + 4));

        // Payload length = header + tsBlockLen + intBlockLen
        int intBlockLen = readLE32(payload, intBlockStart);
        assertEquals("total matches sum of blocks",
                HEADER_SIZE + tsBlockLen + intBlockLen, payload.length);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private static byte[] buildColumnarPayload(int rows) throws SQLException {
        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        for (int i = 0; i < rows; i++) {
            tsCol.appendTimestamp(1700000000000L + i * 1000L);
            intCol.appendInt(i);
        }
        return Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{tsCol, intCol});
    }
}
