package com.taosdata.jdbc.ws.stmt2;

import com.taosdata.jdbc.enums.FieldBindType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BOOL;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DOUBLE;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_FLOAT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_SMALLINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TINYINT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class Stmt2ColumnFieldBufferTest {

    @Test
    public void fixedWidthBuffer_serializesAllNullRowsWithoutValuePayloadAndResets() throws Exception {
        Stmt2ColumnFieldBuffer buffer = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT),
                new Stmt2ColumnFieldBuffer.BufferSizeHints(1, 1, 1));
        try {
            buffer.appendNull();
            buffer.appendNull();

            ByteBuffer block = littleEndian(buffer.buildColumnBlock());
            assertEquals(19, block.getInt(0));
            assertEquals(TSDB_DATA_TYPE_INT, block.getInt(4));
            assertEquals(2, block.getInt(8));
            assertEquals(1, block.get(12));
            assertEquals(1, block.get(13));
            assertEquals(0, block.get(14));
            assertEquals(0, block.getInt(15));
            assertEquals(19, buffer.getSerializedSize());
            assertTrue(buffer.snapshotUsage().getClass().getName().contains("BufferSizeHints"));

            buffer.reset();

            assertEquals(0, buffer.getRowCount());
            ByteBuffer empty = littleEndian(buffer.buildColumnBlock());
            assertEquals(17, empty.getInt(0));
            assertEquals(0, empty.getInt(8));
            assertEquals(0, empty.getInt(13));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void fixedWidthAppendAliases_writeRowsForEachPrimitiveWidth() throws Exception {
        Stmt2ColumnFieldBuffer oneByte = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_TINYINT));
        Stmt2ColumnFieldBuffer twoByte = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_SMALLINT));
        Stmt2ColumnFieldBuffer fourByte = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_INT));
        Stmt2ColumnFieldBuffer eightByte = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BIGINT));
        try {
            oneByte.appendBool(true);
            oneByte.appendTinyInt((byte) 2);
            oneByte.appendUTinyInt((byte) 3);
            assertFixedBlock(oneByte, TSDB_DATA_TYPE_TINYINT, 3, new byte[]{1, 2, 3});

            twoByte.appendSmallInt((short) 0x0102);
            twoByte.appendUSmallInt((short) 0x0304);
            assertEquals(2, twoByte.getRowCount());
            assertEquals(4, valueLength(twoByte.buildColumnBlock(), 2));

            fourByte.appendInt(0x01020304);
            fourByte.appendUInt(0x05060708);
            fourByte.appendFloat(1.25f);
            assertEquals(3, fourByte.getRowCount());
            assertEquals(12, valueLength(fourByte.buildColumnBlock(), 3));

            eightByte.appendBigInt(1L);
            eightByte.appendUBigInt(2L);
            eightByte.appendDouble(3.5d);
            eightByte.appendTimestamp(4L);
            assertEquals(4, eightByte.getRowCount());
            assertEquals(32, valueLength(eightByte.buildColumnBlock(), 4));
        } finally {
            oneByte.release();
            twoByte.release();
            fourByte.release();
            eightByte.release();
        }
    }

    @Test
    public void variableWidthBuffer_tracksNullsLengthsAndSingleHeaderComponentWhenAllNull() throws Exception {
        Stmt2ColumnFieldBuffer buffer = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BINARY));
        try {
            buffer.appendEncodedVar(null);
            buffer.appendString(null);

            assertEquals(1, buffer.blockComponentCount());
            ByteBuf block = buffer.buildColumnBlockBuffer();
            try {
                assertEquals(1, block.nioBufferCount());
                ByteBuffer bytes = littleEndian(ByteBufUtil.getBytes(block));
                assertEquals(27, bytes.getInt(0));
                assertEquals(2, bytes.getInt(8));
                assertEquals(1, bytes.get(12));
                assertEquals(1, bytes.get(13));
                assertEquals(1, bytes.get(14));
                assertEquals(0, bytes.getInt(15));
                assertEquals(0, bytes.getInt(19));
                assertEquals(0, bytes.getInt(23));
            } finally {
                block.release();
            }
        } finally {
            buffer.release();
        }
    }

    @Test
    public void tableNameBytesValidateUtf8AndCoalesceSameTableAcrossStringAndBytes() throws Exception {
        Stmt2ColumnFieldBuffer buffer = new Stmt2ColumnFieldBuffer(tbNameMeta());
        byte[] nameBytes = "é".getBytes(StandardCharsets.UTF_8);
        try {
            buffer.appendString("é", ByteBufUtil.utf8Bytes("é"));
            buffer.appendTbName("é", ByteBufUtil.utf8Bytes("é"));
            buffer.appendTbNameBytes(nameBytes, nameBytes.length);
            buffer.appendTbName("é", ByteBufUtil.utf8Bytes("é"));
            buffer.appendEncodedVar(nameBytes, nameBytes.length);

            assertEquals(5, buffer.getRowCount());
            assertEquals(1, buffer.computeTableCount());
            assertEquals(10, valueLength(buffer.buildColumnBlock(), 5));

            IllegalArgumentException empty = assertIllegalArgument(() -> buffer.appendTbNameBytes(nameBytes, 0));
            assertTrue(empty.getMessage().contains("Table name must not be null or empty"));

            IllegalArgumentException tooLong = assertIllegalArgument(() -> buffer.appendTbNameBytes(nameBytes, 3));
            assertTrue(tooLong.getMessage().contains("length exceeds"));

            SQLException invalidUtf8 = assertSqlException(() -> buffer.appendTbNameBytes(new byte[]{(byte) 0xC0}, 1));
            assertTrue(invalidUtf8.getMessage().contains("valid UTF-8"));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void tableNameBytesAcceptValidUtf8BoundariesAndRejectTruncatedSequences() throws Exception {
        Stmt2ColumnFieldBuffer buffer = new Stmt2ColumnFieldBuffer(tbNameMeta());
        try {
            String[] validNames = {"¢", "ࠀ", "€", "\uE000", "\uD800\uDF48", "😀", "\uDBFF\uDFFF"};
            for (String name : validNames) {
                byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
                buffer.appendTbNameBytes(bytes, bytes.length);
            }

            assertEquals(validNames.length, buffer.getRowCount());
            assertEquals(validNames.length, buffer.computeTableCount());

            assertSqlException(() -> buffer.appendTbNameBytes(new byte[]{(byte) 0xE0, (byte) 0xA0}, 2));
            assertSqlException(() -> buffer.appendTbNameBytes(new byte[]{(byte) 0xF0, (byte) 0x90, (byte) 0x80}, 3));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void explicitLengthNullEncodedVarAppendsNullRow() throws Exception {
        Stmt2ColumnFieldBuffer buffer = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BINARY));
        try {
            buffer.appendEncodedVar(null, 0);

            assertEquals(1, buffer.getRowCount());
            ByteBuffer block = littleEndian(buffer.buildColumnBlock());
            assertEquals(1, block.getInt(8));
            assertEquals(1, block.get(12));
            assertEquals(0, block.getInt(18));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void tableNameSpecificMethodsRejectNonTableNameColumns() throws Exception {
        Stmt2ColumnFieldBuffer buffer = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BINARY));
        try {
            assertIllegalState(() -> buffer.appendTbName("t0"));
            assertIllegalState(() -> buffer.appendTbName("t0", 2));
            assertIllegalState(() -> buffer.appendTbNameBytes("t0".getBytes(StandardCharsets.UTF_8), 2));
            assertIllegalState(buffer::computeTableCount);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void reusableAccessorsExposeDisabledEmptyAndPrimedStates() throws Exception {
        Stmt2ColumnFieldBuffer normal = new Stmt2ColumnFieldBuffer(colMeta(TSDB_DATA_TYPE_BINARY));
        Stmt2ColumnFieldBuffer reusable = Stmt2ColumnFieldBuffer.forReusableValueBuffer(
                colMeta(TSDB_DATA_TYPE_BINARY), null, 1024, 512, 2);
        try {
            assertNull(normal.currentReusableSpec());
            assertEquals(0, normal.activeReusableChunkCount());
            assertEquals(0, normal.reusableOverflowCount());
            assertIllegalState(() -> normal.primeReusableValueChunks(1));

            assertNull(reusable.currentReusableSpec());
            reusable.primeReusableValueChunks(2);
            assertNotNull(reusable.currentReusableSpec());
            assertEquals(0, reusable.activeReusableChunkCount());

            reusable.appendEncodedVar("abc".getBytes(StandardCharsets.UTF_8), 3);
            assertEquals(1, reusable.activeReusableChunkCount());
            assertFalse(reusable.currentReusableSpec().getChunkBytes() < 1024);
        } finally {
            normal.release();
            reusable.release();
        }
    }

    private static Stmt2FieldMeta colMeta(int type) {
        return Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_COL.getValue(), (byte) type, (byte) 0);
    }

    private static Stmt2FieldMeta tbNameMeta() {
        return Stmt2FieldMeta.of((byte) FieldBindType.TAOS_FIELD_TBNAME.getValue(),
                (byte) TSDB_DATA_TYPE_BINARY, (byte) 0);
    }

    private static ByteBuffer littleEndian(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static int valueLength(byte[] block, int rowCount) {
        int offset = 12 + rowCount;
        boolean haveLength = block[offset] != 0;
        offset += 1;
        if (haveLength) {
            offset += rowCount * Integer.BYTES;
        }
        return littleEndian(block).getInt(offset);
    }

    private static void assertFixedBlock(Stmt2ColumnFieldBuffer buffer, int type, int rows, byte[] expectedValues)
            throws Exception {
        byte[] block = buffer.buildColumnBlock();
        ByteBuffer bytes = littleEndian(block);
        assertEquals(type, bytes.getInt(4));
        assertEquals(rows, bytes.getInt(8));
        assertEquals(expectedValues.length, valueLength(block, rows));
        assertArrayEquals(expectedValues, java.util.Arrays.copyOfRange(
                block, block.length - expectedValues.length, block.length));
    }

    private static SQLException assertSqlException(ThrowingRunnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected SQLException");
        } catch (SQLException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError("Expected SQLException but got " + e.getClass().getName(), e);
        }
    }

    private static IllegalArgumentException assertIllegalArgument(ThrowingRunnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError("Expected IllegalArgumentException but got " + e.getClass().getName(), e);
        }
    }

    private static void assertIllegalState(ThrowingRunnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // expected
        } catch (Exception e) {
            throw new AssertionError("Expected IllegalStateException but got " + e.getClass().getName(), e);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
