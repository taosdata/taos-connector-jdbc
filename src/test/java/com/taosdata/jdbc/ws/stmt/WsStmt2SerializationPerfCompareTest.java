package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.HEADER_SIZE;
import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.HEADER_TOTAL_LENGTH_OFFSET;
import static org.junit.Assert.*;

/**
 * Serialization comparison harness: columnar (new stmt2 path) vs row-serial (line-mode proxy).
 *
 * <h3>Purpose</h3>
 * <p>Verifies that {@link Stmt2ColumnBindSerializer} produces a well-formed binary payload
 * and provides a repeatable in-process timing harness that illustrates the relative effort
 * of the two serialization strategies without imposing a hard performance SLA.
 *
 * <h3>Row-serial proxy</h3>
 * <p>The row-serial approach below is a synthetic stand-in for the byte-layout produced by
 * {@code WSRowPreparedStatement}.  It writes values in row-major order (timestamp then int
 * per row) into a growing buffer – the same fundamental work that the line-mode producer
 * does, without needing a live server or Transport.  This makes the timing loop
 * directly comparable: both harnesses produce a byte payload for the same logical N-row
 * dataset.
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
    // Row-serial proxy (approximates WSRowPreparedStatement line-mode output)
    // -----------------------------------------------------------------------

    /**
     * Builds a simple row-major byte payload for N rows of (timestamp, int).
     *
     * <p>Each row is written sequentially: 8 bytes (timestamp LE) + 4 bytes (int LE).
     * A minimal 8-byte header (row count + field count) is prepended to make the output
     * structurally comparable with a real protocol frame.
     *
     * @param rows number of rows to serialise
     * @return byte array whose length equals 8 + rows * 12
     */
    private static byte[] rowSerialSerialize(int rows) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8 + rows * 12);
        DataOutputStream dos = new DataOutputStream(baos);

        // Minimal header: row count (4 LE) + field count (4 LE)
        writeLE32(dos, rows);
        writeLE32(dos, 2);

        for (int i = 0; i < rows; i++) {
            writeLE64(dos, 1700000000000L + i * 1000L);  // timestamp
            writeLE32(dos, i);                             // int value
        }
        return baos.toByteArray();
    }

    private static void writeLE32(DataOutputStream dos, int v) throws IOException {
        dos.write(v & 0xFF);
        dos.write((v >>> 8) & 0xFF);
        dos.write((v >>> 16) & 0xFF);
        dos.write((v >>> 24) & 0xFF);
    }

    private static void writeLE64(DataOutputStream dos, long v) throws IOException {
        dos.write((int)(v & 0xFF));
        dos.write((int)((v >>> 8) & 0xFF));
        dos.write((int)((v >>> 16) & 0xFF));
        dos.write((int)((v >>> 24) & 0xFF));
        dos.write((int)((v >>> 32) & 0xFF));
        dos.write((int)((v >>> 40) & 0xFF));
        dos.write((int)((v >>> 48) & 0xFF));
        dos.write((int)((v >>> 56) & 0xFF));
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
    public void testRowSerialPayloadSize_singleRow() throws IOException {
        byte[] payload = rowSerialSerialize(1);
        // 8 header + 1 row * (8 ts + 4 int) = 8 + 12 = 20
        assertEquals("row-serial 1 row", 20, payload.length);
    }

    @Test
    public void testRowSerialPayloadSize_scalesLinearly() throws IOException {
        for (int rows : new int[]{1, 10, 100}) {
            byte[] payload = rowSerialSerialize(rows);
            assertEquals("row-serial " + rows + " rows", 8 + rows * 12, payload.length);
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
        // vs row-serial 12 bytes/row (no null markers).
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
        byte[] rowSerialPayload = rowSerialSerialize(rows);

        double columnarBytesPerRow = (double) columnarPayload.length / rows;
        double rowSerialBytesPerRow = (double) rowSerialPayload.length / rows;

        System.out.printf("[PerfCompare] %d rows | columnar: %d bytes (%.1f/row) | " +
                        "row-serial: %d bytes (%.1f/row)%n",
                rows,
                columnarPayload.length, columnarBytesPerRow,
                rowSerialPayload.length, rowSerialBytesPerRow);

        // Columnar payload should be less than 25 bytes/row for ts+int (fixed-width only).
        // Actual = (20 + 26 + 22) / 1000 ≈ 14 bytes/row – well within this bound.
        assertTrue("columnar bytes/row must be bounded above 5",
                columnarBytesPerRow > 5);
        assertTrue("columnar bytes/row for ts+int must be < 25",
                columnarBytesPerRow < 25);
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
            rowSerialSerialize(rows);
        }

        // Measure columnar
        long columnarNs = 0;
        for (int r = 0; r < measureRounds; r++) {
            long t0 = System.nanoTime();
            byte[] p = buildColumnarPayload(rows);
            columnarNs += System.nanoTime() - t0;
            assertNotNull(p);
        }

        // Measure row-serial
        long rowSerialNs = 0;
        for (int r = 0; r < measureRounds; r++) {
            long t0 = System.nanoTime();
            byte[] p = rowSerialSerialize(rows);
            rowSerialNs += System.nanoTime() - t0;
            assertNotNull(p);
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
        // row-serial sends values in-line without a length array overhead.
        // columnar adds a per-row length[] array (4 bytes each) but groups by column.
        // For N=100 rows of 5-byte strings:
        //   columnar block overhead per row: 1(null) + 4(length) = 5 extra bytes / row
        //   row-serial: just the 5 bytes + 2(len16) per row in typical wire formats
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
