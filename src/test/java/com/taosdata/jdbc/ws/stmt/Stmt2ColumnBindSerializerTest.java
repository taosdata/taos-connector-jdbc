package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer.*;
import static org.junit.Assert.*;

/**
 * Unit tests for the columnar stmt2 binary payload builder.
 *
 * Each test validates byte-level layout against the adapter's
 * MarshalStmt2ColumnBinary / go_stmt2_bind_column_binary spec.
 */
public class Stmt2ColumnBindSerializerTest {

    // ------------------------------------------------------------------
    // Helper utilities
    // ------------------------------------------------------------------

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

    private static Stmt2FieldMeta meta(int bindType, int fieldType) {
        return Stmt2FieldMeta.of((byte) bindType, (byte) fieldType, (byte) 0);
    }

    private static Stmt2FieldMeta metaWithPrecision(int bindType, int fieldType, int precision) {
        return Stmt2FieldMeta.of((byte) bindType, (byte) fieldType, (byte) precision);
    }

    // ------------------------------------------------------------------
    // Payload header tests
    // ------------------------------------------------------------------

    @Test
    public void testHeaderLayout_singleIntColumn() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        col.appendInt(42);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});

        assertEquals("total_length matches payload length",
                payload.length, readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
        assertEquals("row_count", 1, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals("table_count", 1, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
        assertEquals("field_count", 1, readLE32(payload, HEADER_FIELD_COUNT_OFFSET));
        assertEquals("field_offset", HEADER_SIZE, readLE32(payload, HEADER_FIELD_OFFSET_OFFSET));
    }

    @Test
    public void testHeaderLayout_multipleRowsAndColumns() throws SQLException {
        Stmt2ColumnFieldBuffer c1 = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer c2 = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_BIGINT));
        for (int i = 0; i < 5; i++) {
            c1.appendInt(i);
            c2.appendBigInt(i);
        }

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
                new Stmt2ColumnFieldBuffer[]{c1, c2});

        assertEquals(payload.length, readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
        assertEquals(5, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals(1, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
        assertEquals(2, readLE32(payload, HEADER_FIELD_COUNT_OFFSET));
        assertEquals(HEADER_SIZE, readLE32(payload, HEADER_FIELD_OFFSET_OFFSET));
    }

    // ------------------------------------------------------------------
    // Fixed-width column block tests
    // ------------------------------------------------------------------

    @Test
    public void testFixedWidthBlock_int_singleRow() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        col.appendInt(0x01020304);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;

        // Column block layout: totalLen(4) type(4) num(4) isNull(1) haveLen(1) bufLen(4) value(4)
        // = 17 + 1 (null) + 4 (value) = 22
        int expectedBlockLen = 17 + 1 + 4;
        assertEquals("block total_length", expectedBlockLen, readLE32(payload, blockStart));
        assertEquals("block type", TSDB_DATA_TYPE_INT, readLE32(payload, blockStart + 4));
        assertEquals("block num", 1, readLE32(payload, blockStart + 8));
        assertEquals("is_null[0]", 0, payload[blockStart + 12] & 0xFF);
        assertEquals("have_length", 0, payload[blockStart + 13] & 0xFF);
        assertEquals("buffer_length", 4, readLE32(payload, blockStart + 14));
        assertEquals("value LE", 0x01020304, readLE32(payload, blockStart + 18));
    }

    @Test
    public void testFixedWidthBlock_bigint_singleRow() throws SQLException {
        long val = 0x0102030405060708L;
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_BIGINT));
        col.appendBigInt(val);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        // 17 + 1 (null) + 8 (value) = 26
        int expectedBlockLen = 17 + 1 + 8;
        assertEquals(expectedBlockLen, readLE32(payload, blockStart));
        assertEquals("buffer_length", 8, readLE32(payload, blockStart + 14));
        assertEquals("value LE", val, readLE64(payload, blockStart + 18));
    }

    @Test
    public void testFixedWidthBlock_bool() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_BOOL));
        col.appendBool(true);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        // 17 + 1 + 1 = 19
        assertEquals(19, readLE32(payload, blockStart));
        assertEquals("value", 1, payload[blockStart + 18] & 0xFF);
    }

    @Test
    public void testFixedWidthBlock_float() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_FLOAT));
        col.appendFloat(3.14f);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        // 17 + 1 + 4 = 22
        assertEquals(22, readLE32(payload, blockStart));
        int rawBits = readLE32(payload, blockStart + 18);
        assertEquals(3.14f, Float.intBitsToFloat(rawBits), 0.0001f);
    }

    @Test
    public void testFixedWidthBlock_floatRawBits() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_FLOAT));
        int rawBits = Float.floatToRawIntBits(3.14f);
        col.appendFixed4Raw(rawBits);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        assertEquals(rawBits, readLE32(payload, blockStart + 18));
    }

    @Test
    public void testFixedWidthBlock_double() throws SQLException {
        double val = Math.PI;
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_DOUBLE));
        col.appendDouble(val);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        // 17 + 1 + 8 = 26
        assertEquals(26, readLE32(payload, blockStart));
        long rawBits = readLE64(payload, blockStart + 18);
        assertEquals(val, Double.longBitsToDouble(rawBits), 0.0);
    }

    @Test
    public void testFixedWidthBlock_doubleRawBits() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_DOUBLE));
        long rawBits = Double.doubleToRawLongBits(Math.PI);
        col.appendFixed8Raw(rawBits);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        assertEquals(rawBits, readLE64(payload, blockStart + 18));
    }

    @Test
    public void testFixedWidthBlock_timestamp() throws SQLException {
        long ts = 1700000000000L;
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_TIMESTAMP));
        col.appendTimestamp(ts);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        assertEquals(TSDB_DATA_TYPE_TIMESTAMP, readLE32(payload, blockStart + 4));
        assertEquals("buffer_length", 8, readLE32(payload, blockStart + 14));
        assertEquals("timestamp", ts, readLE64(payload, blockStart + 18));
    }

    @Test
    public void testFixedWidthBlock_smallInt() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_SMALLINT));
        col.appendSmallInt((short) 0x1234);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        // 17 + 1 + 2 = 20
        assertEquals(20, readLE32(payload, blockStart));
        int lo = payload[blockStart + 18] & 0xFF;
        int hi = payload[blockStart + 19] & 0xFF;
        assertEquals(0x34, lo);
        assertEquals(0x12, hi);
    }

    @Test
    public void testFixedWidthBlock_tinyInt() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_TINYINT));
        col.appendTinyInt((byte) 99);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;
        // 17 + 1 + 1 = 19
        assertEquals(19, readLE32(payload, blockStart));
        assertEquals(99, payload[blockStart + 18] & 0xFF);
    }

    @Test
    public void testFixedWidthBlock_usesHeaderAndValueComponents() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        col.appendInt(42);

        ByteBuf block = col.buildColumnBlockBuffer();
        try {
            assertTrue(block instanceof CompositeByteBuf);
            assertEquals(2, ((CompositeByteBuf) block).numComponents());
        } finally {
            block.release();
            col.release();
        }
    }

    @Test
    public void testVarWidthBlock_usesHeaderAndValueComponents() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR));
        col.appendBytes("ab".getBytes(StandardCharsets.UTF_8));

        ByteBuf block = col.buildColumnBlockBuffer();
        try {
            assertTrue(block instanceof CompositeByteBuf);
            assertEquals(2, ((CompositeByteBuf) block).numComponents());
        } finally {
            block.release();
            col.release();
        }
    }

    @Test
    public void testSerializeBuffer_flattensColumnBlockComponentsIntoPayload() throws SQLException {
        Stmt2ColumnFieldBuffer fixed = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer var = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR));
        fixed.appendInt(42);
        var.appendBytes("ab".getBytes(StandardCharsets.UTF_8));

        ByteBuf payload = Stmt2ColumnBindSerializer.serializeBuffer(new Stmt2ColumnFieldBuffer[]{fixed, var});
        try {
            assertTrue(payload instanceof CompositeByteBuf);
            CompositeByteBuf composite = (CompositeByteBuf) payload;
            assertEquals(5, composite.numComponents());
            for (int i = 0; i < composite.numComponents(); i++) {
                assertFalse("payload component " + i + " should be flattened",
                        composite.component(i) instanceof CompositeByteBuf);
            }
        } finally {
            payload.release();
            fixed.release();
            var.release();
        }
    }

    // ------------------------------------------------------------------
    // Null handling tests
    // ------------------------------------------------------------------

    @Test
    public void testNullRow_fixedWidth_singleNull() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        col.appendNull();

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;

        assertEquals("is_null[0] must be 1", 1, payload[blockStart + 12] & 0xFF);
        // All-null: buffer_length must be 0
        assertEquals("buffer_length", 0, readLE32(payload, blockStart + 14));
    }

    @Test
    public void testNullRow_fixedWidth_mixedNullAndValue() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        col.appendInt(1);
        col.appendNull();
        col.appendInt(3);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;

        assertEquals("num", 3, readLE32(payload, blockStart + 8));
        assertEquals("is_null[0]", 0, payload[blockStart + 12] & 0xFF);
        assertEquals("is_null[1]", 1, payload[blockStart + 13] & 0xFF);
        assertEquals("is_null[2]", 0, payload[blockStart + 14] & 0xFF);
        assertEquals("have_length", 0, payload[blockStart + 15] & 0xFF);
        assertEquals("buffer_length", 12, readLE32(payload, blockStart + 16));
        assertEquals("value[0]", 1, readLE32(payload, blockStart + 20));
        assertEquals("value[1] (null placeholder)", 0, readLE32(payload, blockStart + 24));
        assertEquals("value[2]", 3, readLE32(payload, blockStart + 28));
    }

    @Test
    public void testNullRow_variableWidth_allNull() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR));
        col.appendNull();
        col.appendNull();

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;

        // is_null at blockStart+12, haveLength at blockStart+14 (after 2 null bytes)
        assertEquals("have_length", 1, payload[blockStart + 14] & 0xFF);
        // length[0] at 15, length[1] at 19; buffer_length at blockStart+23
        // offset = 12 (isNull start) + 2 (isNull[2]) + 1 (haveLength) + 2*4 (lengths) = 23
        assertEquals("buffer_length", 0, readLE32(payload, blockStart + 23));
    }

    // ------------------------------------------------------------------
    // Variable-width column block tests
    // ------------------------------------------------------------------

    @Test
    public void testVariableWidthBlock_binary_singleRow() throws SQLException {
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR));
        col.appendBytes(data);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;

        assertEquals("type", TSDB_DATA_TYPE_VARCHAR, readLE32(payload, blockStart + 4));
        assertEquals("num", 1, readLE32(payload, blockStart + 8));
        assertEquals("is_null[0]", 0, payload[blockStart + 12] & 0xFF);
        assertEquals("have_length", 1, payload[blockStart + 13] & 0xFF);
        assertEquals("length[0]", 5, readLE32(payload, blockStart + 14));
        assertEquals("buffer_length", 5, readLE32(payload, blockStart + 18));
        // raw bytes follow
        assertArrayEquals("raw value", data, java.util.Arrays.copyOfRange(payload, blockStart + 22, blockStart + 27));
    }

    @Test
    public void testVariableWidthBlock_binary_multiRow_withNull() throws SQLException {
        byte[] hello = "hi".getBytes(StandardCharsets.UTF_8);
        byte[] world = "world".getBytes(StandardCharsets.UTF_8);
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR));
        col.appendBytes(hello);
        col.appendNull();
        col.appendBytes(world);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        int blockStart = HEADER_SIZE;

        assertEquals("is_null[0]", 0, payload[blockStart + 12] & 0xFF);
        assertEquals("is_null[1]", 1, payload[blockStart + 13] & 0xFF);
        assertEquals("is_null[2]", 0, payload[blockStart + 14] & 0xFF);
        assertEquals("have_length", 1, payload[blockStart + 15] & 0xFF);
        // lengths: [2, 0, 5]
        assertEquals("length[0]", 2, readLE32(payload, blockStart + 16));
        assertEquals("length[1]", 0, readLE32(payload, blockStart + 20));
        assertEquals("length[2]", 5, readLE32(payload, blockStart + 24));
        // buffer_length = 2 + 5 = 7 (null row contributes 0 bytes)
        assertEquals("buffer_length", 7, readLE32(payload, blockStart + 28));
    }

    // ------------------------------------------------------------------
    // Table count derivation tests
    // ------------------------------------------------------------------

    @Test
    public void testTableCount_singleTable() throws SQLException {
        Stmt2ColumnFieldBuffer tbName = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_TBNAME.getValue(), TSDB_DATA_TYPE_VARCHAR));
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        tbName.appendTbName("table1");
        tbName.appendTbName("table1");
        col.appendInt(1);
        col.appendInt(2);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
                new Stmt2ColumnFieldBuffer[]{tbName, col});
        assertEquals("table_count", 1, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
    }

    @Test
    public void testTableCount_multipleConsecutiveTables() throws SQLException {
        Stmt2ColumnFieldBuffer tbName = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_TBNAME.getValue(), TSDB_DATA_TYPE_VARCHAR));
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        // 3 rows for table1, then 2 rows for table2
        for (int i = 0; i < 3; i++) { tbName.appendTbName("t1"); col.appendInt(i); }
        for (int i = 0; i < 2; i++) { tbName.appendTbName("t2"); col.appendInt(i); }

        byte[] payload = Stmt2ColumnBindSerializer.serialize(
                new Stmt2ColumnFieldBuffer[]{tbName, col});
        assertEquals("table_count", 2, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
    }

    // ------------------------------------------------------------------
    // Query bind special case
    // ------------------------------------------------------------------

    @Test
    public void testQueryBind_rowCountOne() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_QUERY.getValue(), TSDB_DATA_TYPE_INT));
        col.appendInt(7);

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
        assertEquals("row_count", 1, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals("total_length matches", payload.length,
                readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET));
    }

    @Test(expected = SQLException.class)
    public void testQueryBind_moreThanOneRow_throws() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_QUERY.getValue(), TSDB_DATA_TYPE_INT));
        col.appendInt(1);
        col.appendInt(2);
        Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{col});
    }

    @Test
    public void testSerializeQuery_rowCountOne() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_BIGINT));
        col.appendBigInt(42L);

        byte[] payload = Stmt2ColumnBindSerializer.serializeQuery(new Stmt2ColumnFieldBuffer[]{col});
        assertEquals(1, readLE32(payload, HEADER_ROW_COUNT_OFFSET));
        assertEquals(1, readLE32(payload, HEADER_TABLE_COUNT_OFFSET));
    }

    @Test(expected = SQLException.class)
    public void testSerializeQuery_moreThanOneRow_throws() throws SQLException {
        Stmt2ColumnFieldBuffer col = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        col.appendInt(1);
        col.appendInt(2);
        Stmt2ColumnBindSerializer.serializeQuery(new Stmt2ColumnFieldBuffer[]{col});
    }

    // ------------------------------------------------------------------
    // Row-count mismatch
    // ------------------------------------------------------------------

    @Test(expected = SQLException.class)
    public void testRowCountMismatch_throws() throws SQLException {
        Stmt2ColumnFieldBuffer c1 = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer c2 = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_INT));
        c1.appendInt(1);
        c2.appendInt(1);
        c2.appendInt(2);
        Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{c1, c2});
    }

    // ------------------------------------------------------------------
    // Payload integrity: total_length covers all column blocks
    // ------------------------------------------------------------------

    @Test
    public void testPayloadIntegrity_totalLengthCoversAllBlocks() throws SQLException {
        Stmt2ColumnFieldBuffer c1 = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_BIGINT));
        Stmt2ColumnFieldBuffer c2 = new Stmt2ColumnFieldBuffer(
                meta(FieldBindType.TAOS_FIELD_COL.getValue(), TSDB_DATA_TYPE_VARCHAR));
        for (int i = 0; i < 4; i++) {
            c1.appendBigInt(i * 1000L);
            c2.appendBytes(("row" + i).getBytes(StandardCharsets.UTF_8));
        }

        byte[] payload = Stmt2ColumnBindSerializer.serialize(new Stmt2ColumnFieldBuffer[]{c1, c2});
        int declared = readLE32(payload, HEADER_TOTAL_LENGTH_OFFSET);
        assertEquals("declared total_length == actual length", payload.length, declared);

        // Walk column blocks: sum of block total_lengths + header must equal payload length
        int offset = HEADER_SIZE;
        int blockSum = 0;
        for (int i = 0; i < 2; i++) {
            int blockLen = readLE32(payload, offset);
            blockSum += blockLen;
            offset += blockLen;
        }
        assertEquals("block bytes + header == payload length", HEADER_SIZE + blockSum, payload.length);
    }

    // ------------------------------------------------------------------
    // Stmt2FieldMeta helpers
    // ------------------------------------------------------------------

    @Test
    public void testFieldMeta_isVariableWidth() {
        assertTrue(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_VARCHAR));
        assertTrue(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_NCHAR));
        assertTrue(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_JSON));
        assertTrue(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_VARBINARY));
        assertTrue(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_BLOB));
        assertTrue(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_GEOMETRY));
        assertTrue(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_DECIMAL128));
        assertTrue(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_DECIMAL64));

        assertFalse(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_INT));
        assertFalse(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_BIGINT));
        assertFalse(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_BOOL));
        assertFalse(Stmt2FieldMeta.isVariableWidth(TSDB_DATA_TYPE_TIMESTAMP));
    }

    @Test
    public void testFieldMeta_fixedWidth() {
        assertEquals(1, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_BOOL,      (byte)0).fixedWidth());
        assertEquals(1, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_TINYINT,   (byte)0).fixedWidth());
        assertEquals(1, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_UTINYINT,  (byte)0).fixedWidth());
        assertEquals(2, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_SMALLINT,  (byte)0).fixedWidth());
        assertEquals(2, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_USMALLINT, (byte)0).fixedWidth());
        assertEquals(4, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_INT,       (byte)0).fixedWidth());
        assertEquals(4, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_UINT,      (byte)0).fixedWidth());
        assertEquals(4, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_FLOAT,     (byte)0).fixedWidth());
        assertEquals(8, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_BIGINT,    (byte)0).fixedWidth());
        assertEquals(8, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_UBIGINT,   (byte)0).fixedWidth());
        assertEquals(8, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_DOUBLE,    (byte)0).fixedWidth());
        assertEquals(8, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_TIMESTAMP, (byte)0).fixedWidth());
        assertEquals(0, Stmt2FieldMeta.of((byte)1, (byte) TSDB_DATA_TYPE_VARCHAR,   (byte)0).fixedWidth());
    }
}
