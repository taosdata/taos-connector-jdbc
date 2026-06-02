//package com.taosdata.jdbc.ws.stmt;
//
//import com.taosdata.jdbc.AbstractConnection;
//import com.taosdata.jdbc.common.ConnectionParam;
//import com.taosdata.jdbc.enums.FieldBindType;
//import com.taosdata.jdbc.ws.Transport;
//import com.taosdata.jdbc.ws.WSColumnFastPreparedStatement;
//import com.taosdata.jdbc.ws.stmt2.Stmt2BindExecRequestBuilder;
//import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
//import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
//import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
//import com.taosdata.jdbc.ws.stmt2.entity.Field;
//import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
//import com.taosdata.jdbc.utils.Utils;
//import io.netty.buffer.ByteBuf;
//import io.netty.buffer.ByteBufUtil;
//import io.netty.buffer.CompositeByteBuf;
//import org.junit.Assume;
//import org.junit.Test;
//
//import java.lang.reflect.Method;
//import java.nio.charset.StandardCharsets;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.sql.Timestamp;
//import java.util.Arrays;
//
//import static com.taosdata.jdbc.TSDBConstants.*;
//import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.HEADER_SIZE;
//import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.HEADER_TOTAL_LENGTH_OFFSET;
//import static org.junit.Assert.*;
//import static org.mockito.Mockito.*;
//
///**
// * Serialization comparison harness: columnar (new stmt2 path) vs row-serial
// * ({@link WSRowPreparedStatement} line-mode path).
// *
// * <h3>Purpose</h3>
// * <p>Verifies that {@link Stmt2ColumnBindSerializer} produces a well-formed binary payload
// * and provides a repeatable in-process timing harness that illustrates the relative effort
// * of the two serialization strategies without imposing a hard performance SLA.
// *
// * <h3>Row-serial baseline</h3>
// * <p>The row-serial measurement uses the production class {@link WSRowPreparedStatement}
// * directly: fields are bound via its public setter API, and the internal raw block is
// * extracted via reflection (calling private {@code buffersStopWrite()} then
// * {@code getRawBlock()}) so that no live server is required.  This produces an
// * apples-to-apples comparison because both paths serialise the same logical N-row dataset.
// *
// * <h3>Assertion philosophy</h3>
// * <ul>
// *   <li>Byte counts are deterministic: exact assertions are used.</li>
// *   <li>Timing is reported via {@code System.out} only; no wall-clock SLA is asserted
// *       to prevent flaky failures on slow CI runners.</li>
// *   <li>The columnar path must never produce more bytes per row than a 2-column payload
// *       warrants (HEADER + 2 column blocks), ensuring no runaway growth.</li>
// * </ul>
// */
//public class WsStmt2SerializationPerfCompareTest {
//
//    // -----------------------------------------------------------------------
//    // Constants used in comparisons
//    // -----------------------------------------------------------------------
//
//    /**
//     * Fixed-width column block header size:
//     *   totalLength(4) + type(4) + num(4) + isNull[N](N) + haveLength(1) + bufferLength(4)
//     * For N == ROWS: 17 + ROWS  (null array is row-major byte array)
//     */
//    private static final int FIXED_BLOCK_OVERHEAD = 17; // not counting null bytes or value bytes
//    private static final long BASE_TS = 1_700_000_000_000L;
//    private static final int DEFAULT_REALISTIC_ROWS = 10_000;
//    private static final int DEFAULT_WARMUP_ROUNDS = 3;
//    private static final int DEFAULT_MEASURE_ROUNDS = 5;
//
//    private enum BenchmarkMode {
//        FAST,
//        LINE
//    }
//
//    private static final class ClientSerializationResult {
//        final int requestBytes;
//
//        private ClientSerializationResult(int requestBytes) {
//            this.requestBytes = requestBytes;
//        }
//    }
//
//    private static final class RealisticWorkload {
//        final String[] tableNames;
//        final Timestamp[] timestamps;
//        final float[] current;
//        final int[] voltage;
//        final float[] phase;
//
//        private RealisticWorkload(String[] tableNames,
//                                  Timestamp[] timestamps,
//                                  float[] current,
//                                  int[] voltage,
//                                  float[] phase) {
//            this.tableNames = tableNames;
//            this.timestamps = timestamps;
//            this.current = current;
//            this.voltage = voltage;
//            this.phase = phase;
//        }
//
//        private int rows() {
//            return tableNames.length;
//        }
//    }
//
//    // -----------------------------------------------------------------------
//    // Helpers
//    // -----------------------------------------------------------------------
//
//    private static Stmt2FieldMeta colMeta(int fieldType) {
//        return Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
//                (byte) fieldType, (byte) 0);
//    }
//
//    private static int readLE32(byte[] buf, int off) {
//        return (buf[off] & 0xFF)
//                | ((buf[off + 1] & 0xFF) << 8)
//                | ((buf[off + 2] & 0xFF) << 16)
//                | ((buf[off + 3] & 0xFF) << 24);
//    }
//
//    private static float currentFor(int rowIndex) {
//        return (float) (rowIndex % 1_000) / 10.0f;
//    }
//
//    private static int voltageFor(int rowIndex) {
//        return 220 + (rowIndex % 50);
//    }
//
//    private static float phaseFor(int rowIndex) {
//        return (float) (rowIndex % 360) / 10.0f;
//    }
//
//    private static String paddedTableName(int rowIndex) {
//        String value = Integer.toString(rowIndex);
//        StringBuilder builder = new StringBuilder(10);
//        builder.append('d');
//        for (int i = value.length(); i < 9; i++) {
//            builder.append('0');
//        }
//        builder.append(value);
//        return builder.toString();
//    }
//
//    private static RealisticWorkload buildRealisticWorkload(int rows) {
//        String[] tableNames = new String[rows];
//        Timestamp[] timestamps = new Timestamp[rows];
//        float[] current = new float[rows];
//        int[] voltage = new int[rows];
//        float[] phase = new float[rows];
//        for (int i = 0; i < rows; i++) {
//            tableNames[i] = paddedTableName(i);
//            timestamps[i] = new Timestamp(BASE_TS + i);
//            current[i] = currentFor(i);
//            voltage[i] = voltageFor(i);
//            phase[i] = phaseFor(i);
//        }
//        return new RealisticWorkload(tableNames, timestamps, current, voltage, phase);
//    }
//
//    private static ConnectionParam mockParam() {
//        ConnectionParam param = mock(ConnectionParam.class);
//        when(param.getRequestTimeout()).thenReturn(30_000);
//        when(param.getZoneId()).thenReturn(null);
//        when(param.getRetryTimes()).thenReturn(1);
//        when(param.isEnableAutoConnect()).thenReturn(false);
//        when(param.getDatabase()).thenReturn("testdb");
//        return param;
//    }
//
//    private static Transport mockTransport(ConnectionParam param) {
//        Transport transport = mock(Transport.class);
//        when(transport.getReconnectCount()).thenReturn(0);
//        when(transport.isConnected()).thenReturn(true);
//        when(transport.isClosed()).thenReturn(false);
//        when(transport.getConnectionParam()).thenReturn(param);
//        return transport;
//    }
//
//    private static BenchmarkMode[] benchmarkOrderForRound(int roundIndex) {
//        BenchmarkMode[] modes = BenchmarkMode.values();
//        BenchmarkMode[] order = new BenchmarkMode[modes.length];
//        int startIndex = roundIndex % modes.length;
//        for (int i = 0; i < modes.length; i++) {
//            order[i] = modes[(startIndex + i) % modes.length];
//        }
//        return order;
//    }
//
//    private static Field stmtField(byte bindType, byte fieldType) {
//        Field field = new Field();
//        field.setBindType(bindType);
//        field.setFieldType(fieldType);
//        field.setPrecision((byte) 0);
//        return field;
//    }
//
//    private static Stmt2PrepareResp realisticInsertPrepareResp() {
//        Stmt2PrepareResp prepResp = new Stmt2PrepareResp();
//        prepResp.setCode(0);
//        prepResp.setInsert(true);
//        prepResp.setStmtId(0L);
//        prepResp.setFields(Arrays.asList(
//                stmtField((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(), (byte) TSDB_DATA_TYPE_VARCHAR),
//                stmtField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_TIMESTAMP),
//                stmtField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_FLOAT),
//                stmtField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_INT),
//                stmtField((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) TSDB_DATA_TYPE_FLOAT)));
//        return prepResp;
//    }
//
//    private static void bindRealisticWorkload(PreparedStatement stmt, RealisticWorkload workload) throws SQLException {
//        for (int i = 0; i < workload.rows(); i++) {
//            stmt.setString(1, workload.tableNames[i]);
//            stmt.setTimestamp(2, workload.timestamps[i]);
//            stmt.setFloat(3, workload.current[i]);
//            stmt.setInt(4, workload.voltage[i]);
//            stmt.setFloat(5, workload.phase[i]);
//            stmt.addBatch();
//        }
//    }
//
//    private static final class Stmt2PayloadAccess {
//        private final Object owner;
//        private final Method buildPayloadBuffer;
//        private final Method reset;
//
//        private Stmt2PayloadAccess(Object owner, Method buildPayloadBuffer, Method reset) {
//            this.owner = owner;
//            this.buildPayloadBuffer = buildPayloadBuffer;
//            this.reset = reset;
//        }
//
//        private ByteBuf buildPayloadBuffer() throws Exception {
//            return (ByteBuf) buildPayloadBuffer.invoke(owner);
//        }
//
//        private void reset() throws Exception {
//            reset.invoke(owner);
//        }
//    }
//
//    private static Stmt2PayloadAccess stmt2PayloadAccess(Object stmt, Class<?> stmtClass) throws Exception {
//        try {
//            Method buildPayloadBuffer = stmtClass.getDeclaredMethod("buildPayloadBuffer");
//            buildPayloadBuffer.setAccessible(true);
//            Method resetFastState = stmtClass.getDeclaredMethod("resetFastState");
//            resetFastState.setAccessible(true);
//            return new Stmt2PayloadAccess(stmt, buildPayloadBuffer, resetFastState);
//        } catch (NoSuchMethodException ignored) {
//            java.lang.reflect.Field batchStateField = stmtClass.getDeclaredField("batchState");
//            batchStateField.setAccessible(true);
//            Object batchState = batchStateField.get(stmt);
//            Method buildPayloadBuffer = batchState.getClass().getDeclaredMethod("buildPayloadBuffer");
//            buildPayloadBuffer.setAccessible(true);
//            Method reset = batchState.getClass().getDeclaredMethod("reset");
//            reset.setAccessible(true);
//            return new Stmt2PayloadAccess(batchState, buildPayloadBuffer, reset);
//        }
//    }
//
//    private static byte[] buildStmt2RequestBytes(Object stmt, Class<?> stmtClass) throws Exception {
//        Stmt2PayloadAccess access = stmt2PayloadAccess(stmt, stmtClass);
//        ByteBuf payload = access.buildPayloadBuffer();
//        ByteBuf request = Stmt2BindExecRequestBuilder.build(payload);
//        try {
//            return ByteBufUtil.getBytes(request);
//        } finally {
//            Utils.releaseByteBuf(request);
//            access.reset();
//        }
//    }
//
//    private static ClientSerializationResult wsFastColumnRealisticSerialization(RealisticWorkload workload) throws Exception {
//        ConnectionParam param = mockParam();
//        Transport transport = mockTransport(param);
//        AbstractConnection conn = mock(AbstractConnection.class);
//        Stmt2PrepareResp prepResp = realisticInsertPrepareResp();
//
//        WSColumnFastPreparedStatement stmt = new WSColumnFastPreparedStatement(
//                transport, param, "testdb", conn,
//                "insert into meters (tbname, ts, current, voltage, phase) values(?,?,?,?,?)",
//                0L, prepResp);
//        Stmt2PayloadAccess access = stmt2PayloadAccess(stmt, WSColumnFastPreparedStatement.class);
//
//        try {
//            for (int i = 0; i < workload.rows(); i++) {
//                stmt.setString(1, workload.tableNames[i]);
//                stmt.setTimestamp(2, workload.timestamps[i]);
//                stmt.setFloat(3, workload.current[i]);
//                stmt.setInt(4, workload.voltage[i]);
//                stmt.setFloat(5, workload.phase[i]);
//                stmt.addBatch();
//            }
//
//            ByteBuf payload = access.buildPayloadBuffer();
//            ByteBuf request = Stmt2BindExecRequestBuilder.build(payload);
//            try {
//                return new ClientSerializationResult(request.readableBytes());
//            } finally {
//                Utils.releaseByteBuf(request);
//                access.reset();
//            }
//        } finally {
//            stmt.close();
//        }
//    }
//
//    private static byte[] wsFastColumnRealisticRequestBytes(RealisticWorkload workload) throws Exception {
//        ConnectionParam param = mockParam();
//        Transport transport = mockTransport(param);
//        AbstractConnection conn = mock(AbstractConnection.class);
//        Stmt2PrepareResp prepResp = realisticInsertPrepareResp();
//
//        WSColumnFastPreparedStatement stmt = new WSColumnFastPreparedStatement(
//                transport, param, "testdb", conn,
//                "insert into meters (tbname, ts, current, voltage, phase) values(?,?,?,?,?)",
//                0L, prepResp);
//
//        try {
//            bindRealisticWorkload(stmt, workload);
//            return buildStmt2RequestBytes(stmt, WSColumnFastPreparedStatement.class);
//        } finally {
//            stmt.close();
//        }
//    }
//
//    // -----------------------------------------------------------------------
//    // 1. Deterministic byte-count comparisons
//    // -----------------------------------------------------------------------
//
//    @Test
//    public void testColumnarPayloadSize_singleRow_twoColumns() throws SQLException {
//        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
//        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
//
//        tsCol.appendTimestamp(1700000000000L);
//        intCol.appendInt(42);
//
//        byte[] payload = serializeAndRelease(tsCol, intCol);
//
//        // Expected: HEADER(20) + block_ts(17+1+8=26) + block_int(17+1+4=22) = 68
//        int expectedTsBlock = FIXED_BLOCK_OVERHEAD + 1 /* null[1] */ + 8 /* ts bytes */;
//        int expectedIntBlock = FIXED_BLOCK_OVERHEAD + 1 /* null[1] */ + 4 /* int bytes */;
//        int expectedTotal = HEADER_SIZE + expectedTsBlock + expectedIntBlock;
//
//        assertEquals("single-row 2-column payload size", expectedTotal, payload.length);
//        assertEquals("total_length header", expectedTotal, readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
//    }
//
//    @Test
//    public void testColumnarPayloadSize_scalesLinearlyWithRows() throws SQLException {
//        int[] rowCounts = {1, 10, 100};
//        for (int rows : rowCounts) {
//            Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
//            Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
//
//            for (int i = 0; i < rows; i++) {
//                tsCol.appendTimestamp(1700000000000L + i * 1000L);
//                intCol.appendInt(i);
//            }
//
//            byte[] payload = serializeAndRelease(tsCol, intCol);
//
//            // block_ts = 17 + rows(null) + 8*rows(values) = 17 + 9*rows
//            // block_int = 17 + rows(null) + 4*rows(values) = 17 + 5*rows
//            int expectedSize = HEADER_SIZE
//                    + (FIXED_BLOCK_OVERHEAD + rows + rows * 8)
//                    + (FIXED_BLOCK_OVERHEAD + rows + rows * 4);
//
//            assertEquals("payload size for " + rows + " rows",
//                    expectedSize, payload.length);
//        }
//    }
//
//    @Test
//    public void testBenchmarkMeasurementOrder_rotatesAcrossRounds() {
//        assertArrayEquals(new BenchmarkMode[]{BenchmarkMode.FAST, BenchmarkMode.LINE},
//                benchmarkOrderForRound(0));
//        assertArrayEquals(new BenchmarkMode[]{BenchmarkMode.LINE, BenchmarkMode.FAST},
//                benchmarkOrderForRound(1));
//        assertArrayEquals(new BenchmarkMode[]{BenchmarkMode.FAST, BenchmarkMode.LINE},
//                benchmarkOrderForRound(2));
//        assertArrayEquals(new BenchmarkMode[]{BenchmarkMode.LINE, BenchmarkMode.FAST},
//                benchmarkOrderForRound(3));
//    }
//
//    // -----------------------------------------------------------------------
//    // 2. Bytes-per-row comparison (columnar vs row-serial)
//    // -----------------------------------------------------------------------
//
//    @Test
//    public void testBytesPerRow_columnarHasHigherFixedOverheadButLessPerRowForLargeN()
//            throws Exception {
//        // For small N, columnar has higher per-row cost due to header overhead.
//        // For large N, per-row overhead approaches (8+1) ts + (4+1) int = 14 bytes/row
//        // vs WSRowPreparedStatement ≈ 52 bytes/row (per-column metadata per row).
//        // This test validates that both approaches grow linearly with N and that
//        // the columnar byte count / row is bounded above by a reasonable constant.
//
//        int rows = 1000;
//        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
//        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
//
//        for (int i = 0; i < rows; i++) {
//            tsCol.appendTimestamp(1700000000000L + i * 1000L);
//            intCol.appendInt(i);
//        }
//
//        byte[] columnarPayload = serializeAndRelease(tsCol, intCol);
//        int wsRowBytes = wsRowPayloadBytes(rows);
//
//        double columnarBytesPerRow = (double) columnarPayload.length / rows;
//        double rowSerialBytesPerRow = (double) wsRowBytes / rows;
//
//        System.out.printf("[PerfCompare] %d rows | columnar: %d bytes (%.1f/row) | " +
//                        "row-serial: %d bytes (%.1f/row)%n",
//                rows,
//                columnarPayload.length, columnarBytesPerRow,
//                wsRowBytes, rowSerialBytesPerRow);
//
//        // Columnar payload should be < 25 bytes/row for ts+int (fixed-width only).
//        // Actual = (20 + 26 + 22) / 1000 ≈ 14 bytes/row – well within this bound.
//        assertTrue("columnar bytes/row must be bounded above 5",
//                columnarBytesPerRow > 5);
//        assertTrue("columnar bytes/row for ts+int must be < 25",
//                columnarBytesPerRow < 25);
//
//        // WSRowPreparedStatement per-row cost is ~52 bytes; columnar is more compact
//        assertTrue("columnar must be more compact than WSRowPreparedStatement for 1000 rows",
//                columnarPayload.length < wsRowBytes);
//    }
//
//    // -----------------------------------------------------------------------
//    // 3. In-process timing harness (no SLA assertions)
//    // -----------------------------------------------------------------------
//
//    @Test
//    public void testTimingHarness_columnarVsRowSerial_noSlaAssertion() throws Exception {
//        final int rows = 10_000;
//        final int warmupRounds = 3;
//        final int measureRounds = 5;
//
//        // Warm-up
//        for (int w = 0; w < warmupRounds; w++) {
//            buildColumnarPayload(rows);
//            wsRowPayloadBytes(rows);
//        }
//
//        // Measure columnar
//        long columnarNs = 0;
//        for (int r = 0; r < measureRounds; r++) {
//            long t0 = System.nanoTime();
//            byte[] p = buildColumnarPayload(rows);
//            columnarNs += System.nanoTime() - t0;
//            assertNotNull(p);
//        }
//
//        // Measure WSRowPreparedStatement row-serial
//        long rowSerialNs = 0;
//        for (int r = 0; r < measureRounds; r++) {
//            long t0 = System.nanoTime();
//            int size = wsRowPayloadBytes(rows);
//            rowSerialNs += System.nanoTime() - t0;
//            assertTrue("wsRowPayloadBytes must return a positive size", size > 0);
//        }
//
//        long avgColumnarUs = columnarNs / measureRounds / 1_000;
//        long avgRowSerialUs = rowSerialNs / measureRounds / 1_000;
//
//        System.out.printf("[PerfCompare] %d rows | columnar avg: %d µs | " +
//                        "row-serial avg: %d µs%n",
//                rows, avgColumnarUs, avgRowSerialUs);
//
//        // Only sanity-check: both paths complete in under 30 seconds (30_000_000 µs)
//        // to catch infinite-loop or allocation catastrophe without imposing a tight SLA.
//        assertTrue("columnar serialisation must complete in < 30 s",
//                avgColumnarUs < 30_000_000L);
//        assertTrue("row-serial serialisation must complete in < 30 s",
//                avgRowSerialUs < 30_000_000L);
//    }
//
//    @Test
//    public void benchmarkClientOnly_realisticWorkload_columnarVsRowMode() throws Exception {
//        Assume.assumeTrue("Set -Dws.perf.client.serialize=true to run client-only benchmark",
//                Boolean.getBoolean("ws.perf.client.serialize"));
//
//        final int rows = Integer.getInteger("ws.perf.client.serialize.rows", DEFAULT_REALISTIC_ROWS);
//        final int warmupRounds = Integer.getInteger("ws.perf.client.serialize.warmup", DEFAULT_WARMUP_ROUNDS);
//        final int measureRounds = Integer.getInteger("ws.perf.client.serialize.rounds", DEFAULT_MEASURE_ROUNDS);
//        final RealisticWorkload workload = buildRealisticWorkload(rows);
//
//        for (int i = 0; i < warmupRounds; i++) {
//            for (BenchmarkMode mode : benchmarkOrderForRound(i)) {
//                switch (mode) {
//                    case FAST:
//                        wsFastColumnRealisticSerialization(workload);
//                        break;
//                    case LINE:
//                        wsRowRealisticSerialization(workload);
//                        break;
//                    default:
//                        throw new AssertionError("unexpected benchmark mode: " + mode);
//                }
//            }
//        }
//
//        long fastNs = 0L;
//        long rowNs = 0L;
//        ClientSerializationResult fastResult = null;
//        ClientSerializationResult rowResult = null;
//        for (int i = 0; i < measureRounds; i++) {
//            for (BenchmarkMode mode : benchmarkOrderForRound(i)) {
//                long t0 = System.nanoTime();
//                ClientSerializationResult result;
//                switch (mode) {
//                    case FAST:
//                        result = wsFastColumnRealisticSerialization(workload);
//                        fastNs += System.nanoTime() - t0;
//                        fastResult = result;
//                        break;
//                    case LINE:
//                        result = wsRowRealisticSerialization(workload);
//                        rowNs += System.nanoTime() - t0;
//                        rowResult = result;
//                        break;
//                    default:
//                        throw new AssertionError("unexpected benchmark mode: " + mode);
//                }
//            }
//        }
//
//        double avgFastMs = fastNs / (double) measureRounds / 1_000_000.0;
//        double avgRowMs = rowNs / (double) measureRounds / 1_000_000.0;
//        double fastSpeedup = avgRowMs / avgFastMs;
//
//        assertNotNull(fastResult);
//        assertNotNull(rowResult);
//        System.out.printf(
//                "[ClientSerializePerfFast] rows=%d fastBytes=%d lineBytes=%d fastAvgMs=%.2f lineAvgMs=%.2f speedup=%.3f%n",
//                rows,
//                fastResult.requestBytes,
//                rowResult.requestBytes,
//                avgFastMs,
//                avgRowMs,
//                fastSpeedup);
//
//        assertTrue("fast-mode request must have bytes", fastResult.requestBytes > 0);
//        assertTrue("line-mode request must have bytes", rowResult.requestBytes > 0);
//        assertTrue("fast benchmark must finish in under 30s", avgFastMs < 30_000.0);
//        assertTrue("line-mode benchmark must finish in under 30s", avgRowMs < 30_000.0);
//    }
//
//    // -----------------------------------------------------------------------
//    // 4. Variable-width column comparison
//    // -----------------------------------------------------------------------
//
//    @Test
//    public void testVarWidthPayloadSize_exactCalculation() throws SQLException {
//        // Row values: "a" (1 byte), "bb" (2 bytes), "ccc" (3 bytes)
//        String[] values = {"a", "bb", "ccc"};
//        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(
//                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
//                        (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));
//        for (String v : values) {
//            buf.appendBytes(v.getBytes(StandardCharsets.UTF_8));
//        }
//
//        byte[] payload = serializeAndRelease(buf);
//
//        // block: 4(totalLen) + 4(type) + 4(num) + 3(isNull) + 1(haveLen) + 3*4(lengths) + 4(bufLen) + (1+2+3)
//        int expectedBlock = 4 + 4 + 4 + 3 + 1 + (3 * 4) + 4 + 6;
//        int expectedTotal = HEADER_SIZE + expectedBlock;
//        assertEquals("var-width payload exact size", expectedTotal, payload.length);
//    }
//
//    @Test
//    public void testVarWidthPayloadSize_rowSerialComparison() throws Exception {
//        // Columnar adds a per-row length[] array (4 bytes each) but groups by column.
//        // For N=100 rows of 5-byte strings:
//        //   columnar block overhead per row: 1(null) + 4(length) = 5 extra bytes / row
//        // This test validates that the columnar payload length matches the formula.
//
//        int rows = 100;
//        byte[] fixed5 = "hello".getBytes(StandardCharsets.UTF_8);
//
//        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(
//                Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
//                        (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0));
//        for (int i = 0; i < rows; i++) {
//            buf.appendBytes(fixed5);
//        }
//
//        byte[] payload = serializeAndRelease(buf);
//
//        // Expected block: 4+4+4 + rows(null) + 1 + rows*4(lengths) + 4 + rows*5(values)
//        int expectedBlock = 17 + rows + rows * 4 + rows * 5;
//        assertEquals("columnar payload with " + rows + " uniform varchar rows",
//                HEADER_SIZE + expectedBlock, payload.length);
//
//        System.out.printf("[PerfCompare] %d varchar(5) rows | columnar: %d bytes%n",
//                rows, payload.length);
//    }
//
//    // -----------------------------------------------------------------------
//    // 5. Structural output-shape validation
//    // -----------------------------------------------------------------------
//
//    @Test
//    public void testOutputShape_twoColumnPayload_fieldCountAndOffset() throws SQLException {
//        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
//        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
//        tsCol.appendTimestamp(1700000000000L);
//        intCol.appendInt(0);
//
//        byte[] payload = serializeAndRelease(tsCol, intCol);
//
//        assertEquals("field_count", 2, readLE32(payload, 12));
//        assertEquals("field_offset always HEADER_SIZE",
//                HEADER_SIZE, readLE32(payload, 16));
//    }
//
//    @Test
//    public void testOutputShape_columnarIsColumnMajor_twoColumnsIndependent() throws SQLException {
//        // Verify that ts block appears at HEADER_SIZE and int block follows immediately.
//        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
//        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
//        tsCol.appendTimestamp(1700000000001L);
//        intCol.appendInt(999);
//
//        byte[] payload = serializeAndRelease(tsCol, intCol);
//
//        // ts block total_length
//        int tsBlockLen = readLE32(payload, HEADER_SIZE);
//        // int block starts right after ts block
//        int intBlockStart = HEADER_SIZE + tsBlockLen;
//
//        // Sanity: int block type should be TSDB_DATA_TYPE_INT
//        assertEquals("int block type", TSDB_DATA_TYPE_INT, readLE32(payload, intBlockStart + 4));
//
//        // Payload length = header + tsBlockLen + intBlockLen
//        int intBlockLen = readLE32(payload, intBlockStart);
//        assertEquals("total matches sum of blocks",
//                HEADER_SIZE + tsBlockLen + intBlockLen, payload.length);
//    }
//
//    // -----------------------------------------------------------------------
//    // Private helpers
//    // -----------------------------------------------------------------------
//
//    private static byte[] buildColumnarPayload(int rows) throws SQLException {
//        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
//        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
//        for (int i = 0; i < rows; i++) {
//            tsCol.appendTimestamp(1700000000000L + i * 1000L);
//            intCol.appendInt(i);
//        }
//        return serializeAndRelease(tsCol, intCol);
//    }
//
//    private static byte[] serializeAndRelease(Stmt2ColumnFieldBuffer... buffers) throws SQLException {
//        try {
//            return Stmt2ColumnBindSerializer.serialize(buffers);
//        } finally {
//            for (Stmt2ColumnFieldBuffer buffer : buffers) {
//                if (buffer != null) {
//                    buffer.release();
//                }
//            }
//        }
//    }
//}
