package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.utils.VersionUtil;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import com.taosdata.jdbc.ws.WSColumnPreparedStatement;
import com.taosdata.jdbc.ws.WSConnection;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2BindExecRequestBuilder;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit-level regression and routing-fallback tests for the stmt2 bind-exec path.
 *
 * <p>Covers:
 * <ol>
 *   <li>Version capability gate: distinguishes legacy (&lt; 3.4.1.13) from
 *       bind-exec (&ge; 3.4.1.13) servers.</li>
 *   <li>Normal-table insert: TAOS_FIELD_COL fields only; table_count == 1.</li>
 *   <li>Supertable insert: TAOS_FIELD_TBNAME + TAOS_FIELD_TAG + TAOS_FIELD_COL;
 *       table_count derived from tbname column.</li>
 *   <li>Multi-table single-row insert: N distinct table names → table_count == N.</li>
 *   <li>All fixed-width types (bool, tinyint, smallint, int, bigint, float, double,
 *       timestamp) and variable-width types (varchar/binary, nchar, varbinary).</li>
 *   <li>Null handling: all-null columns and mixed null/non-null columns.</li>
 * </ol>
 *
 * <p>No external server is required; every assertion operates on in-memory byte arrays.
 */
public class WsStmt2BindExecFallbackTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static int readLE32(byte[] buf, int off) {
        return (buf[off] & 0xFF)
                | ((buf[off + 1] & 0xFF) << 8)
                | ((buf[off + 2] & 0xFF) << 16)
                | ((buf[off + 3] & 0xFF) << 24);
    }

    private static long readLE64(byte[] buf, int off) {
        long lo = readLE32(buf, off) & 0xFFFFFFFFL;
        long hi = readLE32(buf, off + 4) & 0xFFFFFFFFL;
        return lo | (hi << 32);
    }

    /**
     * Returns the offset within a column block where the per-row null markers begin.
     * Layout: totalLen(4) + type(4) + num(4) = 12 bytes before isNull[].
     */
    private static int nullArrayOffset(int blockStart) {
        return blockStart + 12;
    }

    /**
     * Returns the offset of {@code have_length} for a block that has {@code rows} rows.
     * Layout: totalLen(4) + type(4) + num(4) + isNull[rows] = 12 + rows.
     */
    private static int haveLengthOffset(int blockStart, int rows) {
        return blockStart + 12 + rows;
    }

    /**
     * Returns the offset of {@code buffer_length} for a fixed-width block with {@code rows} rows.
     * Layout: ... + haveLength(1) = 12 + rows + 1.
     */
    private static int bufferLengthOffset(int blockStart, int rows) {
        return blockStart + 12 + rows + 1;
    }

    /**
     * Returns the offset where raw values begin for a fixed-width column block.
     * Layout: ... + bufferLength(4) = 12 + rows + 1 + 4 = 17 + rows.
     */
    private static int fixedValuesOffset(int blockStart, int rows) {
        return blockStart + 17 + rows;
    }

    /**
     * Returns the offset where per-row lengths begin for a variable-width column block.
     * Layout: ... + haveLength(1) = 12 + rows + 1 = 13 + rows.
     * Lengths start right after have_length.
     */
    private static int varLengthArrayOffset(int blockStart, int rows) {
        return blockStart + 12 + rows + 1;
    }

    /**
     * Returns the buffer_length offset for a variable-width column block.
     * Layout: lengths(rows*4) after have_length = 12 + rows + 1 + rows*4.
     */
    private static int varBufLengthOffset(int blockStart, int rows) {
        return blockStart + 12 + rows + 1 + rows * 4;
    }

    private static byte[] serializeAndRelease(Stmt2ColumnFieldBuffer... columns) throws SQLException {
        try {
            return Stmt2ColumnBindSerializer.serialize(columns);
        } finally {
            releaseColumns(columns);
        }
    }

    private static void releaseColumns(Stmt2ColumnFieldBuffer... columns) {
        if (columns == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer column : columns) {
            if (column != null) {
                column.release();
            }
        }
    }

    private static Stmt2FieldMeta colMeta(int fieldType) {
        return Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(),
                (byte) fieldType, (byte) 0);
    }

    private static Stmt2FieldMeta tagMeta(int fieldType) {
        return Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_TAG.getValue(),
                (byte) fieldType, (byte) 0);
    }

    private static Stmt2FieldMeta tbnameMeta() {
        return Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(),
                (byte) TSDB_DATA_TYPE_VARCHAR, (byte) 0);
    }

    // -----------------------------------------------------------------------
    // 1. Binary envelope and version capability gate
    // -----------------------------------------------------------------------

    @Test
    public void testBindExecEnvelope_matchesTaosAdapterWireFormat() {
        byte[] payload = new byte[]{0x11, 0x22, 0x33};
        ByteBuf buf = Stmt2BindExecRequestBuilder.build(payload);
        try {
            assertEquals("header + payload length",
                    Stmt2BindExecRequestBuilder.HEADER_SIZE + payload.length, buf.readableBytes());
            assertEquals("reqId placeholder", 0L, buf.getLongLE(0));
            assertEquals("stmtId placeholder", 0L, buf.getLongLE(8));
            assertEquals("bind-exec action id", Stmt2BindExecRequestBuilder.ACTION_ID, buf.getLongLE(16));
            assertEquals("protocol version",
                    Stmt2BindExecRequestBuilder.PROTOCOL_VERSION, buf.getShortLE(24));
            assertEquals(0x11, buf.getUnsignedByte(26));
            assertEquals(0x22, buf.getUnsignedByte(27));
            assertEquals(0x33, buf.getUnsignedByte(28));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testBindExecEnvelope_acceptsByteBufPayloadWithoutExtraCopy() {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{0x11, 0x22, 0x33});
        ByteBuf buf = Stmt2BindExecRequestBuilder.build(payload);
        try {
            assertEquals("header + payload length",
                    Stmt2BindExecRequestBuilder.HEADER_SIZE + payload.readableBytes(), buf.readableBytes());
            assertEquals("reqId placeholder", 0L, buf.getLongLE(0));
            assertEquals("stmtId placeholder", 0L, buf.getLongLE(8));
            assertEquals("bind-exec action id", Stmt2BindExecRequestBuilder.ACTION_ID, buf.getLongLE(16));
            assertEquals("protocol version",
                    Stmt2BindExecRequestBuilder.PROTOCOL_VERSION, buf.getShortLE(24));
            assertEquals(0x11, buf.getUnsignedByte(26));
            assertEquals(0x22, buf.getUnsignedByte(27));
            assertEquals(0x33, buf.getUnsignedByte(28));
            assertEquals("payload ownership stays with request until release", 1, payload.refCnt());
        } finally {
            buf.release();
        }
        assertEquals("releasing request must release transferred payload ownership", 0, payload.refCnt());
    }

    @Test
    public void testVersionGate_minVersionConstant() {
        assertEquals("version constant must equal 3.4.1.13",
                "3.4.1.13", TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);
    }

    @Test
    public void testVersionGate_atThreshold_returnsTrue() {
        assertTrue(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION + " must enable bind-exec",
                VersionUtil.supportStmt2BindExec(TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION));
    }

    @Test
    public void testVersionGate_justBelowThreshold_returnsFalse() {
        assertFalse("3.4.1.12 must stay on legacy path",
                VersionUtil.supportStmt2BindExec("3.4.1.12"));
    }

    @Test
    public void testVersionGate_minorBelowThreshold_returnsFalse() {
        assertFalse("3.4.0.10 must stay on legacy path",
                VersionUtil.supportStmt2BindExec("3.4.0.10"));
    }

    @Test
    public void testVersionGate_majorBelowThreshold_returnsFalse() {
        assertFalse("3.0.0.0 must stay on legacy path",
                VersionUtil.supportStmt2BindExec("3.0.0.0"));
    }

    @Test
    public void testVersionGate_aboveThreshold_patchMinor_returnsTrue() {
        assertTrue("3.4.1.14 must enable bind-exec",
                VersionUtil.supportStmt2BindExec("3.4.1.14"));
    }

    @Test
    public void testVersionGate_aboveThreshold_minor_returnsTrue() {
        assertTrue("3.4.2.0 must enable bind-exec",
                VersionUtil.supportStmt2BindExec("3.4.2.0"));
    }

    @Test
    public void testVersionGate_aboveThreshold_major_returnsTrue() {
        assertTrue("4.0.0.0 must enable bind-exec",
                VersionUtil.supportStmt2BindExec("4.0.0.0"));
    }

    @Test
    public void testVersionGate_nullVersion_returnsFalse() {
        assertFalse("null version must stay on legacy path",
                VersionUtil.supportStmt2BindExec(null));
    }

    // -----------------------------------------------------------------------
    // 2. Normal table insert (TAOS_FIELD_COL only)
    // -----------------------------------------------------------------------

    @Test
    public void testNormalTableInsert_singleRow_headerTableCountIsOne() throws SQLException {
        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));

        tsCol.appendTimestamp(1700000000000L);
        intCol.appendInt(42);

        byte[] payload = serializeAndRelease(
                new Stmt2ColumnFieldBuffer[]{tsCol, intCol});

        assertEquals("row_count", 1, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals("table_count for normal table must be 1",
                1, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
        assertEquals("field_count", 2, readLE32(payload, HEADER_FIELD_COUNT_OFFSET));
        assertEquals("total_length patch", payload.length, readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
    }

    @Test
    public void testNormalTableInsert_multipleRows_tableCountRemainsOne() throws SQLException {
        Stmt2ColumnFieldBuffer tsCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer intCol = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));

        for (int i = 0; i < 5; i++) {
            tsCol.appendTimestamp(1700000000000L + i * 1000L);
            intCol.appendInt(i);
        }

        byte[] payload = serializeAndRelease(
                new Stmt2ColumnFieldBuffer[]{tsCol, intCol});

        assertEquals("row_count", 5, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals("table_count for normal table must always be 1",
                1, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
        assertEquals("total_length", payload.length, readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
    }

    // -----------------------------------------------------------------------
    // 3. Supertable insert: TAOS_FIELD_TBNAME + TAOS_FIELD_TAG + TAOS_FIELD_COL
    // -----------------------------------------------------------------------

    @Test
    public void testSuperTableInsert_singleRow_tableCountIsOne() throws SQLException {
        Stmt2ColumnFieldBuffer tbNameBuf = new Stmt2ColumnFieldBuffer(tbnameMeta());
        Stmt2ColumnFieldBuffer tagBuf = new Stmt2ColumnFieldBuffer(tagMeta(TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer tsBuf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer valBuf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_DOUBLE));

        tbNameBuf.appendTbName("child_t1");
        tagBuf.appendInt(100);
        tsBuf.appendTimestamp(1700000000000L);
        valBuf.appendDouble(3.14);

        byte[] payload = serializeAndRelease(
                new Stmt2ColumnFieldBuffer[]{tbNameBuf, tagBuf, tsBuf, valBuf});

        assertEquals("row_count", 1, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals("table_count for single-table supertable insert",
                1, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
        assertEquals("field_count", 4, readLE32(payload, HEADER_FIELD_COUNT_OFFSET));
    }

    @Test
    public void testSuperTableInsert_twoRowsSameTable_tableCountIsOne() throws SQLException {
        Stmt2ColumnFieldBuffer tbNameBuf = new Stmt2ColumnFieldBuffer(tbnameMeta());
        Stmt2ColumnFieldBuffer tagBuf = new Stmt2ColumnFieldBuffer(tagMeta(TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer tsBuf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));

        // Two rows for the same child table – sequential runs of same name → table_count == 1
        tbNameBuf.appendTbName("child_t1");
        tbNameBuf.appendTbName("child_t1");
        tagBuf.appendInt(100);
        tagBuf.appendInt(100);
        tsBuf.appendTimestamp(1700000000000L);
        tsBuf.appendTimestamp(1700000001000L);

        byte[] payload = serializeAndRelease(
                new Stmt2ColumnFieldBuffer[]{tbNameBuf, tagBuf, tsBuf});

        assertEquals("row_count", 2, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals("same table twice → table_count 1",
                1, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
    }

    @Test
    public void testSuperTableInsert_twoDistinctTables_tableCountIsTwo() throws SQLException {
        Stmt2ColumnFieldBuffer tbNameBuf = new Stmt2ColumnFieldBuffer(tbnameMeta());
        Stmt2ColumnFieldBuffer tagBuf = new Stmt2ColumnFieldBuffer(tagMeta(TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer tsBuf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));

        tbNameBuf.appendTbName("child_t1");
        tbNameBuf.appendTbName("child_t2");
        tagBuf.appendInt(1);
        tagBuf.appendInt(2);
        tsBuf.appendTimestamp(1700000000000L);
        tsBuf.appendTimestamp(1700000001000L);

        byte[] payload = serializeAndRelease(
                new Stmt2ColumnFieldBuffer[]{tbNameBuf, tagBuf, tsBuf});

        assertEquals("row_count", 2, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals("two distinct tables → table_count 2",
                2, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
    }

    // -----------------------------------------------------------------------
    // 4. Multi-table single-row insert
    // -----------------------------------------------------------------------

    @Test
    public void testMultiTableSingleRow_threeDistinctTables() throws SQLException {
        Stmt2ColumnFieldBuffer tbNameBuf = new Stmt2ColumnFieldBuffer(tbnameMeta());
        Stmt2ColumnFieldBuffer tagBuf = new Stmt2ColumnFieldBuffer(tagMeta(TSDB_DATA_TYPE_BIGINT));
        Stmt2ColumnFieldBuffer tsBuf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));

        for (int i = 0; i < 3; i++) {
            tbNameBuf.appendTbName("t" + i);
            tagBuf.appendBigInt(i * 10L);
            tsBuf.appendTimestamp(1700000000000L + i);
        }

        byte[] payload = serializeAndRelease(
                new Stmt2ColumnFieldBuffer[]{tbNameBuf, tagBuf, tsBuf});

        assertEquals("3 distinct tables → table_count 3",
                3, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
        assertEquals("row_count", 3, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
    }

    @Test
    public void testMultiTableSingleRow_tableCountOnlyCountsRunChanges() throws SQLException {
        // [A, A, B, A] → 3 runs (A, B, A) → table_count == 3
        Stmt2ColumnFieldBuffer tbNameBuf = new Stmt2ColumnFieldBuffer(tbnameMeta());
        Stmt2ColumnFieldBuffer tsBuf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));

        String[] names = {"A", "A", "B", "A"};
        for (int i = 0; i < names.length; i++) {
            tbNameBuf.appendTbName(names[i]);
            tsBuf.appendTimestamp(1700000000000L + i);
        }

        byte[] payload = serializeAndRelease(
                new Stmt2ColumnFieldBuffer[]{tbNameBuf, tsBuf});

        assertEquals("run-length table_count", 3, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
    }

    // -----------------------------------------------------------------------
    // 5a. Fixed-width types: bool, tinyint, smallint, int, bigint
    // -----------------------------------------------------------------------

    @Test
    public void testFixedWidth_bool_trueAndFalse() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BOOL));
        buf.appendBool(true);
        buf.appendBool(false);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        assertEquals("row_count", 2, readLE32(payload, HEADER_ROW_COUNT_OFFSET));

        int blockStart = HEADER_SIZE;
        int rows = 2;
        assertEquals("block type BOOL", TSDB_DATA_TYPE_BOOL, readLE32(payload, blockStart + 4));
        int valOff = fixedValuesOffset(blockStart, rows);
        assertEquals("true is 1", 1, payload[valOff] & 0xFF);
        assertEquals("false is 0", 0, payload[valOff + 1] & 0xFF);
    }

    @Test
    public void testFixedWidth_tinyint() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TINYINT));
        buf.appendTinyInt((byte) 127);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("type", TSDB_DATA_TYPE_TINYINT, readLE32(payload, blockStart + 4));
        assertEquals("value", 127, payload[blockStart + 18] & 0xFF);
    }

    @Test
    public void testFixedWidth_uTinyInt() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_UTINYINT));
        buf.appendUTinyInt((byte) 200); // unsigned 200

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("value (unsigned 200)", 200, payload[blockStart + 18] & 0xFF);
    }

    @Test
    public void testFixedWidth_smallint() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_SMALLINT));
        buf.appendSmallInt((short) 0x3210);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("type", TSDB_DATA_TYPE_SMALLINT, readLE32(payload, blockStart + 4));
        // LE: low byte first
        assertEquals("lo byte", 0x10, payload[blockStart + 18] & 0xFF);
        assertEquals("hi byte", 0x32, payload[blockStart + 19] & 0xFF);
    }

    @Test
    public void testFixedWidth_int_multipleRows() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        int[] vals = {0, Integer.MIN_VALUE, Integer.MAX_VALUE, -1};
        for (int v : vals) {
            buf.appendInt(v);
        }

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        assertEquals("row_count", vals.length, readLE32(payload, HEADER_ROW_COUNT_OFFSET));

        int blockStart = HEADER_SIZE;
        int rows = vals.length;
        int valOff = fixedValuesOffset(blockStart, rows);
        for (int i = 0; i < vals.length; i++) {
            assertEquals("int[" + i + "]", vals[i], readLE32(payload, valOff + i * 4));
        }
    }

    @Test
    public void testFixedWidth_bigint() throws SQLException {
        long val = Long.MIN_VALUE;
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BIGINT));
        buf.appendBigInt(val);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("bigint value", val, readLE64(payload, blockStart + 18));
    }

    @Test
    public void testFixedWidth_float() throws SQLException {
        float val = (float) Math.E;
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_FLOAT));
        buf.appendFloat(val);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        int rawBits = readLE32(payload, blockStart + 18);
        assertEquals("float value", val, Float.intBitsToFloat(rawBits), 0.000001f);
    }

    @Test
    public void testFixedWidth_double() throws SQLException {
        double val = Math.PI;
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_DOUBLE));
        buf.appendDouble(val);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        long rawBits = readLE64(payload, blockStart + 18);
        assertEquals("double value", val, Double.longBitsToDouble(rawBits), 0.0);
    }

    @Test
    public void testFixedWidth_timestamp_millisEpoch() throws SQLException {
        long ts = 1700000000123L;
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        buf.appendTimestamp(ts);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("timestamp type", TSDB_DATA_TYPE_TIMESTAMP, readLE32(payload, blockStart + 4));
        assertEquals("timestamp value", ts, readLE64(payload, blockStart + 18));
    }

    // -----------------------------------------------------------------------
    // 5b. Variable-width types: varchar/binary, nchar, varbinary
    // -----------------------------------------------------------------------

    @Test
    public void testVariableWidth_varchar_singleRow() throws SQLException {
        String s = "hello";
        byte[] encoded = s.getBytes(StandardCharsets.UTF_8);

        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_VARCHAR));
        buf.appendBytes(encoded);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});

        int blockStart = HEADER_SIZE;
        assertEquals("type VARCHAR", TSDB_DATA_TYPE_VARCHAR, readLE32(payload, blockStart + 4));
        assertEquals("have_length == 1 for variable-width", 1,
                payload[blockStart + 13] & 0xFF);

        // length[0] is at offset blockStart + 14
        assertEquals("length[0]", encoded.length, readLE32(payload, blockStart + 14));

        // buffer_length at blockStart + 18
        int bufLen = readLE32(payload, blockStart + 18);
        assertEquals("buffer_length", encoded.length, bufLen);

        // raw bytes follow
        for (int i = 0; i < encoded.length; i++) {
            assertEquals("byte[" + i + "]", encoded[i], payload[blockStart + 22 + i]);
        }
    }

    @Test
    public void testVariableWidth_nchar_multipleRows() throws SQLException {
        String[] values = {"alpha", "βeta", "γamma"};
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_NCHAR));
        for (String v : values) {
            buf.appendBytes(v.getBytes(StandardCharsets.UTF_8));
        }

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        assertEquals("row_count", values.length, readLE32(payload, HEADER_ROW_COUNT_OFFSET));

        int blockStart = HEADER_SIZE;
        int rows = values.length;
        assertEquals("type NCHAR", TSDB_DATA_TYPE_NCHAR, readLE32(payload, blockStart + 4));
        assertEquals("have_length == 1", 1,
                payload[haveLengthOffset(blockStart, rows)] & 0xFF);
    }

    @Test
    public void testVariableWidth_varbinary_encodedCorrectly() throws SQLException {
        byte[] data = {0x00, (byte) 0xFF, 0x42, 0x01};
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_VARBINARY));
        buf.appendBytes(data);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("type VARBINARY", TSDB_DATA_TYPE_VARBINARY, readLE32(payload, blockStart + 4));
        assertEquals("length[0]", data.length, readLE32(payload, blockStart + 14));
    }

    // -----------------------------------------------------------------------
    // 6. Null handling
    // -----------------------------------------------------------------------

    @Test
    public void testNullHandling_singleRow_isNullMarkerSet() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        buf.appendNull();

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});

        int blockStart = HEADER_SIZE;
        // is_null[0] is at offset blockStart + 12
        assertEquals("is_null[0] must be 1", 1, payload[blockStart + 12] & 0xFF);
    }

    @Test
    public void testNullHandling_allNullColumn_zeroBufferLength() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BIGINT));
        buf.appendNull();
        buf.appendNull();
        buf.appendNull();

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});

        int blockStart = HEADER_SIZE;
        int rows = 3;
        // have_length is 0 for fixed-width
        assertEquals("have_length 0 for fixed-width", 0,
                payload[haveLengthOffset(blockStart, rows)] & 0xFF);
        // buffer_length 0 because all rows are null
        assertEquals("buffer_length 0 when all null", 0,
                readLE32(payload, bufferLengthOffset(blockStart, rows)));
    }

    @Test
    public void testNullHandling_mixedNullAndNonNull_markersCorrect() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        buf.appendInt(99);
        buf.appendNull();
        buf.appendInt(-7);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});

        int blockStart = HEADER_SIZE;
        int rows = 3;
        int nullOff = nullArrayOffset(blockStart);
        // is_null markers
        assertEquals("row 0 not null", 0, payload[nullOff] & 0xFF);
        assertEquals("row 1 is null", 1, payload[nullOff + 1] & 0xFF);
        assertEquals("row 2 not null", 0, payload[nullOff + 2] & 0xFF);

        // buffer_length = 3 * 4 = 12
        int bufLen = readLE32(payload, bufferLengthOffset(blockStart, rows));
        assertEquals("buffer_length for 3 int rows", 12, bufLen);
    }

    @Test
    public void testNullHandling_variableWidth_nullMarkerNoLengthBytes() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_VARCHAR));
        buf.appendBytes("hello".getBytes(StandardCharsets.UTF_8));
        buf.appendNull();
        buf.appendBytes("world".getBytes(StandardCharsets.UTF_8));

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});

        int blockStart = HEADER_SIZE;
        // is_null[0]=0, is_null[1]=1, is_null[2]=0
        assertEquals("row 0 not null", 0, payload[blockStart + 12] & 0xFF);
        assertEquals("row 1 is null", 1, payload[blockStart + 13] & 0xFF);
        assertEquals("row 2 not null", 0, payload[blockStart + 14] & 0xFF);

        // have_length == 1 (variable width)
        assertEquals("have_length", 1, payload[blockStart + 15] & 0xFF);

        // length[1] should be 0 for null row
        int len1 = readLE32(payload, blockStart + 16 + 4); // length[0] at +16, length[1] at +20
        assertEquals("null row length == 0", 0, len1);
    }

    // -----------------------------------------------------------------------
    // 7. Payload structural invariants
    // -----------------------------------------------------------------------

    @Test
    public void testPayload_totalLengthMatchesArrayLength() throws SQLException {
        Stmt2ColumnFieldBuffer ts = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        Stmt2ColumnFieldBuffer val = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_VARCHAR));
        for (int i = 0; i < 10; i++) {
            ts.appendTimestamp(1700000000000L + i * 1000L);
            val.appendBytes(("row" + i).getBytes(StandardCharsets.UTF_8));
        }

        byte[] payload = serializeAndRelease(
                new Stmt2ColumnFieldBuffer[]{ts, val});

        assertEquals("total_length header must equal payload.length",
                payload.length, readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
    }

    @Test
    public void testPayload_fieldOffsetIsAlwaysHeaderSize() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        buf.appendInt(1);

        byte[] payload = serializeAndRelease(new Stmt2ColumnFieldBuffer[]{buf});

        assertEquals("field_offset always equals HEADER_SIZE",
                HEADER_SIZE, readLE32(payload, HEADER_FIELD_OFFSET_OFFSET));
    }

    @Test
    public void testPayload_rowCountMismatch_throws() throws SQLException {
        Stmt2ColumnFieldBuffer c1 = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer c2 = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BIGINT));
        c1.appendInt(1);
        // c2 has 0 rows → mismatch

        try {
            serializeAndRelease(new Stmt2ColumnFieldBuffer[]{c1, c2});
            fail("Expected SQLException on row-count mismatch");
        } catch (SQLException e) {
            assertTrue("message mentions mismatch",
                    e.getMessage().contains("mismatch") || e.getMessage().contains("row count"));
        }
    }

    // -----------------------------------------------------------------------
    // 8. Runtime routing: WSConnection.prepareStatement class dispatch
    // -----------------------------------------------------------------------

    /** Builds a mocked Transport that satisfies StmtUtils.initStmtWithRetry + WSStatement init. */
    private static Transport buildMockTransport(Stmt2PrepareResp prepResp) {
        ConnectionParam connParam = buildMockConnectionParam();
        Transport transport = mock(Transport.class);
        when(transport.getReconnectCount()).thenReturn(0);
        when(transport.isConnected()).thenReturn(true);
        when(transport.isClosed()).thenReturn(false);
        when(transport.getConnectionParam()).thenReturn(connParam);

        // stmt2_init response (one-arg send for StmtUtils)
        Stmt2Resp initResp = new Stmt2Resp();
        initResp.setCode(Code.SUCCESS.getCode());
        initResp.setStmtId(42L);
        try {
            when(transport.send(any(com.taosdata.jdbc.ws.entity.Request.class))).thenReturn(initResp);
            // stmt2_prepare response (three-arg send)
            when(transport.send(any(com.taosdata.jdbc.ws.entity.Request.class), anyBoolean(), anyLong()))
                    .thenReturn(prepResp);
            // stmt2_close response (two-arg send used by releaseStmt / close)
            Stmt2Resp closeResp = new Stmt2Resp();
            closeResp.setCode(Code.SUCCESS.getCode());
            when(transport.send(any(com.taosdata.jdbc.ws.entity.Request.class), anyLong()))
                    .thenReturn(closeResp);
        } catch (SQLException e) {
            throw new RuntimeException("Unexpected Mockito exception", e);
        }
        return transport;
    }

    private static ConnectionParam buildMockConnectionParam() {
        ConnectionParam param = mock(ConnectionParam.class);
        when(param.getZoneId()).thenReturn(null);
        when(param.getRequestTimeout()).thenReturn(30_000);
        when(param.getRetryTimes()).thenReturn(1);
        when(param.isEnableAutoConnect()).thenReturn(false);
        when(param.getDatabase()).thenReturn("testdb");
        return param;
    }

    /** Builds a two-field (TIMESTAMP, INT) insert prepare response. */
    private static Stmt2PrepareResp buildInsertPrepareResp() {
        Field tsField = new Field();
        tsField.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        tsField.setFieldType((byte) TSDB_DATA_TYPE_TIMESTAMP);
        Field intField = new Field();
        intField.setBindType((byte) FieldBindType.TAOS_FIELD_COL.getValue());
        intField.setFieldType((byte) TSDB_DATA_TYPE_INT);
        Stmt2PrepareResp resp = new Stmt2PrepareResp();
        resp.setCode(Code.SUCCESS.getCode());
        resp.setInsert(true);
        resp.setStmtId(42L);
        resp.setFields(Arrays.asList(tsField, intField));
        return resp;
    }

    @Test
    public void testRouting_capableServer_returnsWSColumnPreparedStatement() throws Exception {
        Stmt2PrepareResp prepResp = buildInsertPrepareResp();
        Transport transport = buildMockTransport(prepResp);
        ConnectionParam param = buildMockConnectionParam();
        WSConnection conn = new WSConnection("jdbc:TAOS-RS://localhost:6041/testdb",
                new Properties(), transport, param, TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES(?,?)");

        assertTrue("capable server (" + TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION
                        + ") must return WSColumnPreparedStatement",
                stmt instanceof WSColumnPreparedStatement);
    }

    @Test
    public void testRouting_legacyServer_returnsTSWSPreparedStatement() throws Exception {
        Stmt2PrepareResp prepResp = buildInsertPrepareResp();
        Transport transport = buildMockTransport(prepResp);
        ConnectionParam param = buildMockConnectionParam();
        WSConnection conn = new WSConnection("jdbc:TAOS-RS://localhost:6041/testdb",
                new Properties(), transport, param, "3.4.1.12");

        PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES(?,?)");

        assertTrue("legacy server (3.4.1.12) must return TSWSPreparedStatement",
                stmt instanceof TSWSPreparedStatement);
    }

    @Test
    public void testRouting_capableVersions_allReturnWSColumnPreparedStatement() throws Exception {
        for (String ver : new String[]{
                TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION, "3.4.1.14", "3.4.2.0", "4.0.0.0"}) {
            Stmt2PrepareResp prepResp = buildInsertPrepareResp();
            Transport transport = buildMockTransport(prepResp);
            ConnectionParam param = buildMockConnectionParam();
            WSConnection conn = new WSConnection("jdbc:TAOS-RS://localhost:6041/testdb",
                    new Properties(), transport, param, ver);

            PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES(?,?)");

            assertTrue("v" + ver + " must return WSColumnPreparedStatement",
                    stmt instanceof WSColumnPreparedStatement);
        }
    }

    @Test
    public void testRouting_legacyVersions_allReturnTSWSPreparedStatement() throws Exception {
        for (String ver : new String[]{"3.4.1.12", "3.4.0.13", "3.0.0.0", "2.6.0.0"}) {
            Stmt2PrepareResp prepResp = buildInsertPrepareResp();
            Transport transport = buildMockTransport(prepResp);
            ConnectionParam param = buildMockConnectionParam();
            WSConnection conn = new WSConnection("jdbc:TAOS-RS://localhost:6041/testdb",
                    new Properties(), transport, param, ver);

            PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES(?,?)");

            assertTrue("v" + ver + " must return TSWSPreparedStatement",
                    stmt instanceof TSWSPreparedStatement);
        }
    }

    // -----------------------------------------------------------------------
    // 9. WSColumnPreparedStatement runtime send-path (STMT2_BIND_EXEC)
    // -----------------------------------------------------------------------

    /** Creates WSColumnPreparedStatement directly with a capable-server WSConnection. */
    private static WSColumnPreparedStatement buildWSColumnPreparedStatement(
            Transport transport, ConnectionParam param, Stmt2PrepareResp prepResp) {
        WSConnection conn = new WSConnection("jdbc:TAOS-RS://localhost:6041/testdb",
                new Properties(), transport, param, TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);
        return new WSColumnPreparedStatement(transport, param, "testdb", conn,
                "INSERT INTO t VALUES(?,?)", 1L, prepResp);
    }

    private static Transport buildExecuteTransport(ConnectionParam param, int affectedRows) {
        Transport transport = mock(Transport.class);
        when(transport.getReconnectCount()).thenReturn(0);
        when(transport.isConnected()).thenReturn(true);
        when(transport.isClosed()).thenReturn(false);
        when(transport.getConnectionParam()).thenReturn(param);
        try {
            Stmt2ExecResp execResp = new Stmt2ExecResp();
            execResp.setCode(Code.SUCCESS.getCode());
            execResp.setAffected(affectedRows);
            execResp.setStmtId(42L);
            when(transport.send(eq("stmt2_bind_exec"), anyLong(), any(ByteBuf.class),
                    eq(false), anyLong())).thenAnswer(invocation -> {
                ReferenceCountUtil.safeRelease(invocation.getArgument(2));
                return execResp;
            });
            Stmt2Resp closeResp = new Stmt2Resp();
            closeResp.setCode(Code.SUCCESS.getCode());
            when(transport.send(any(com.taosdata.jdbc.ws.entity.Request.class), anyLong()))
                    .thenReturn(closeResp);
        } catch (SQLException e) {
            throw new RuntimeException("Unexpected Mockito exception", e);
        }
        return transport;
    }

    @Test
    public void testColumnStmt_executeUpdate_sendsSingleStmt2BindExec() throws Exception {
        ConnectionParam param = buildMockConnectionParam();
        Transport transport = buildExecuteTransport(param, 1);
        WSColumnPreparedStatement stmt =
                buildWSColumnPreparedStatement(transport, param, buildInsertPrepareResp());

        stmt.setTimestamp(1, new Timestamp(1700000000000L));
        stmt.setInt(2, 42);

        int affected = stmt.executeUpdate();

        verify(transport, times(1)).send(eq("stmt2_bind_exec"), anyLong(),
                any(ByteBuf.class), eq(false), anyLong());
        verify(transport, never()).send(eq("stmt2_bind"), anyLong(),
                any(ByteBuf.class), eq(false), anyLong());
        assertEquals("affected rows from server must be propagated", 1, affected);
    }

    @Test
    public void testColumnStmt_executeBatch_sendsSingleStmt2BindExec() throws Exception {
        ConnectionParam param = buildMockConnectionParam();
        Transport transport = buildExecuteTransport(param, 3);
        WSColumnPreparedStatement stmt =
                buildWSColumnPreparedStatement(transport, param, buildInsertPrepareResp());

        for (int i = 0; i < 3; i++) {
            stmt.setTimestamp(1, new Timestamp(1700000000000L + (long) i * 1000L));
            stmt.setInt(2, i);
            stmt.addBatch();
        }

        int[] results = stmt.executeBatch();

        verify(transport, times(1)).send(eq("stmt2_bind_exec"), anyLong(),
                any(ByteBuf.class), eq(false), anyLong());
        assertEquals("result array length equals affected count", 3, results.length);
    }

    @Test
    public void testColumnStmt_executeBatch_resetsBatchStateForReuse() throws Exception {
        ConnectionParam param = buildMockConnectionParam();
        Transport transport = buildExecuteTransport(param, 2);
        WSColumnPreparedStatement stmt =
                buildWSColumnPreparedStatement(transport, param, buildInsertPrepareResp());

        // First batch: 2 rows
        for (int i = 0; i < 2; i++) {
            stmt.setTimestamp(1, new Timestamp(1700000000000L + (long) i * 1000L));
            stmt.setInt(2, i);
            stmt.addBatch();
        }
        stmt.executeBatch();

        // Second single-row execution – must not throw due to stale batch state
        stmt.setTimestamp(1, new Timestamp(1700000002000L));
        stmt.setInt(2, 99);
        int secondResult = stmt.executeUpdate();

        // Two separate STMT2_BIND_EXEC calls, not one combined 3-row call
        verify(transport, times(2)).send(eq("stmt2_bind_exec"), anyLong(),
                any(ByteBuf.class), eq(false), anyLong());
        assertEquals("second executeUpdate returns server affected rows", 2, secondResult);
    }
}
