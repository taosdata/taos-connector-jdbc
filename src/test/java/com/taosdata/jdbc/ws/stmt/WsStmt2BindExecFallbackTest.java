package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.utils.VersionUtil;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.*;
import static org.junit.Assert.*;

/**
 * Unit-level regression and routing-fallback tests for the stmt2 bind-exec path.
 *
 * <p>Covers:
 * <ol>
 *   <li>Version capability gate: distinguishes legacy (&lt; 3.1.4.10) from
 *       bind-exec (&ge; 3.1.4.10) servers.</li>
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
    // 1. Version capability gate
    // -----------------------------------------------------------------------

    @Test
    public void testVersionGate_minVersionConstant() {
        assertEquals("version constant must equal 3.1.4.10",
                "3.1.4.10", TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);
    }

    @Test
    public void testVersionGate_atThreshold_returnsTrue() {
        assertTrue("3.1.4.10 must enable bind-exec",
                VersionUtil.supportStmt2BindExec("3.1.4.10"));
    }

    @Test
    public void testVersionGate_justBelowThreshold_returnsFalse() {
        assertFalse("3.1.4.9 must stay on legacy path",
                VersionUtil.supportStmt2BindExec("3.1.4.9"));
    }

    @Test
    public void testVersionGate_minorBelowThreshold_returnsFalse() {
        assertFalse("3.1.3.0 must stay on legacy path",
                VersionUtil.supportStmt2BindExec("3.1.3.0"));
    }

    @Test
    public void testVersionGate_majorBelowThreshold_returnsFalse() {
        assertFalse("3.0.0.0 must stay on legacy path",
                VersionUtil.supportStmt2BindExec("3.0.0.0"));
    }

    @Test
    public void testVersionGate_aboveThreshold_patchMinor_returnsTrue() {
        assertTrue("3.1.4.11 must enable bind-exec",
                VersionUtil.supportStmt2BindExec("3.1.4.11"));
    }

    @Test
    public void testVersionGate_aboveThreshold_minor_returnsTrue() {
        assertTrue("3.1.5.0 must enable bind-exec",
                VersionUtil.supportStmt2BindExec("3.1.5.0"));
    }

    @Test
    public void testVersionGate_aboveThreshold_major_returnsTrue() {
        assertTrue("3.2.0.0 must enable bind-exec",
                VersionUtil.supportStmt2BindExec("3.2.0.0"));
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("type", TSDB_DATA_TYPE_TINYINT, readLE32(payload, blockStart + 4));
        assertEquals("value", 127, payload[blockStart + 18] & 0xFF);
    }

    @Test
    public void testFixedWidth_uTinyInt() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_UTINYINT));
        buf.appendUTinyInt((byte) 200); // unsigned 200

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("value (unsigned 200)", 200, payload[blockStart + 18] & 0xFF);
    }

    @Test
    public void testFixedWidth_smallint() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_SMALLINT));
        buf.appendSmallInt((short) 0x3210);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        assertEquals("bigint value", val, readLE64(payload, blockStart + 18));
    }

    @Test
    public void testFixedWidth_float() throws SQLException {
        float val = (float) Math.E;
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_FLOAT));
        buf.appendFloat(val);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        int rawBits = readLE32(payload, blockStart + 18);
        assertEquals("float value", val, Float.intBitsToFloat(rawBits), 0.000001f);
    }

    @Test
    public void testFixedWidth_double() throws SQLException {
        double val = Math.PI;
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_DOUBLE));
        buf.appendDouble(val);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
        int blockStart = HEADER_SIZE;
        long rawBits = readLE64(payload, blockStart + 18);
        assertEquals("double value", val, Double.longBitsToDouble(rawBits), 0.0);
    }

    @Test
    public void testFixedWidth_timestamp_millisEpoch() throws SQLException {
        long ts = 1700000000123L;
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TIMESTAMP));
        buf.appendTimestamp(ts);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});

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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});
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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});

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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});

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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});

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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});

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

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
                new Stmt2ColumnFieldBuffer[]{ts, val});

        assertEquals("total_length header must equal payload.length",
                payload.length, readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
    }

    @Test
    public void testPayload_fieldOffsetIsAlwaysHeaderSize() throws SQLException {
        Stmt2ColumnFieldBuffer buf = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        buf.appendInt(1);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{buf});

        assertEquals("field_offset always equals HEADER_SIZE",
                HEADER_SIZE, readLE32(payload, HEADER_FIELD_OFFSET_OFFSET));
    }

    @Test
    public void testPayload_rowCountMismatch_throws() {
        Stmt2ColumnFieldBuffer c1 = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer c2 = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BIGINT));
        c1.appendInt(1);
        // c2 has 0 rows → mismatch

        try {
            Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{c1, c2});
            fail("Expected SQLException on row-count mismatch");
        } catch (SQLException e) {
            assertTrue("message mentions mismatch",
                    e.getMessage().contains("mismatch") || e.getMessage().contains("row count"));
        }
    }
}
