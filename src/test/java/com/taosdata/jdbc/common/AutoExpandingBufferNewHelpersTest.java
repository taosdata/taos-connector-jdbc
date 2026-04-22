package com.taosdata.jdbc.common;

import io.netty.buffer.CompositeByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * Coverage for the direct-write helpers added to AutoExpandingBuffer in Task 2.
 * These helpers avoid Unpooled.buffer(...) on the slow path.
 *
 * Pre-existing method tests live in AutoExpandingBufferTest; this file only
 * covers the new methods.
 */
public class AutoExpandingBufferNewHelpersTest {

    private static final int SMALL_BUF = 4;   // forces slow path quickly
    private static final int MAX_COMP  = 64;

    private AutoExpandingBuffer buf;

    @Before
    public void setUp() {
        buf = new AutoExpandingBuffer(SMALL_BUF, MAX_COMP);
    }

    @After
    public void tearDown() {
        if (buf != null) {
            buf.release();
        }
    }

    // -----------------------------------------------------------------------
    // writeBytes(byte[], off, len)
    // -----------------------------------------------------------------------

    @Test
    public void writeBytesRange_subrange() throws SQLException {
        byte[] src = {0x01, 0x02, 0x03, 0x04, 0x05};
        buf.writeBytes(src, 1, 3);  // should write 0x02 0x03 0x04
        buf.stopWrite();
        byte[] out = extract(buf);
        assertEquals(3, out.length);
        assertEquals(0x02, out[0] & 0xFF);
        assertEquals(0x03, out[1] & 0xFF);
        assertEquals(0x04, out[2] & 0xFF);
    }

    @Test
    public void writeBytesRange_spansBufBoundary() throws SQLException {
        // SMALL_BUF=4: first 4 bytes fill current buffer, next 3 spill to new one
        byte[] src = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
        buf.writeBytes(src, 0, 7);
        buf.stopWrite();
        byte[] out = extract(buf);
        assertEquals(7, out.length);
        for (int i = 0; i < 7; i++) {
            assertEquals(i + 1, out[i] & 0xFF);
        }
    }

    // -----------------------------------------------------------------------
    // writeByte
    // -----------------------------------------------------------------------

    @Test
    public void writeByte_fitsInCurrentBuffer() throws SQLException {
        buf.writeByte((byte) 0xAB);
        buf.stopWrite();
        byte[] out = extract(buf);
        assertEquals(1, out.length);
        assertEquals(0xAB, out[0] & 0xFF);
    }

    @Test
    public void writeByte_triggersBufRollover() throws SQLException {
        // Fill the 4-byte buffer then add one more byte
        buf.writeBytes(new byte[]{1, 2, 3, 4});
        buf.writeByte((byte) 5);
        buf.stopWrite();
        byte[] out = extract(buf);
        assertEquals(5, out.length);
        assertEquals(5, out[4] & 0xFF);
    }

    // -----------------------------------------------------------------------
    // writeIntLE
    // -----------------------------------------------------------------------

    @Test
    public void writeIntLE_fitsInBuffer() throws SQLException {
        buf.writeIntLE(0x12345678);
        buf.stopWrite();
        byte[] out = extract(buf);
        assertEquals(4, out.length);
        assertEquals(0x78, out[0] & 0xFF);
        assertEquals(0x56, out[1] & 0xFF);
        assertEquals(0x34, out[2] & 0xFF);
        assertEquals(0x12, out[3] & 0xFF);
    }

    @Test
    public void writeIntLE_spansBufBoundary() throws SQLException {
        // Leave only 1 byte free in the 4-byte buffer
        buf.writeBytes(new byte[]{0});
        buf.writeIntLE(0xDEADBEEF);
        buf.stopWrite();
        byte[] out = extract(buf);
        assertEquals(5, out.length);
        assertEquals((byte) 0xEF, out[1]);
        assertEquals((byte) 0xBE, out[2]);
        assertEquals((byte) 0xAD, out[3]);
        assertEquals((byte) 0xDE, out[4]);
    }

    // -----------------------------------------------------------------------
    // writeLongLE
    // -----------------------------------------------------------------------

    @Test
    public void writeLongLE_fitsInBuffer() throws SQLException {
        AutoExpandingBuffer big = new AutoExpandingBuffer(16, MAX_COMP);
        long val = 0x0102030405060708L;
        big.writeLongLE(val);
        big.stopWrite();
        byte[] out = extract(big);
        big.release();

        assertEquals(8, out.length);
        assertEquals(0x08, out[0] & 0xFF);
        assertEquals(0x07, out[1] & 0xFF);
        assertEquals(0x01, out[7] & 0xFF);
    }

    @Test
    public void writeLongLE_spansBufBoundary() throws SQLException {
        // Fill 3 bytes then write a long (crosses boundary at byte 4)
        buf.writeBytes(new byte[]{0, 0, 0});
        buf.writeLongLE(0x0807060504030201L);
        buf.stopWrite();
        byte[] out = extract(buf);
        assertEquals(11, out.length);
        assertEquals(0x01, out[3] & 0xFF);
        assertEquals(0x02, out[4] & 0xFF);
        assertEquals(0x08, out[10] & 0xFF);
    }

    // -----------------------------------------------------------------------
    // writeFloatLE
    // -----------------------------------------------------------------------

    @Test
    public void writeFloatLE_roundtrip() throws SQLException {
        float val = 3.14159f;
        AutoExpandingBuffer big = new AutoExpandingBuffer(16, MAX_COMP);
        big.writeFloatLE(val);
        big.stopWrite();
        byte[] out = extract(big);
        big.release();

        assertEquals(4, out.length);
        int bits = (out[0] & 0xFF)
                | ((out[1] & 0xFF) << 8)
                | ((out[2] & 0xFF) << 16)
                | ((out[3] & 0xFF) << 24);
        assertEquals(val, Float.intBitsToFloat(bits), 0.0001f);
    }

    // -----------------------------------------------------------------------
    // writeDoubleLE
    // -----------------------------------------------------------------------

    @Test
    public void writeDoubleLE_roundtrip() throws SQLException {
        double val = Math.E;
        AutoExpandingBuffer big = new AutoExpandingBuffer(16, MAX_COMP);
        big.writeDoubleLE(val);
        big.stopWrite();
        byte[] out = extract(big);
        big.release();

        assertEquals(8, out.length);
        long bits = (out[0] & 0xFFL)
                | ((out[1] & 0xFFL) << 8)
                | ((out[2] & 0xFFL) << 16)
                | ((out[3] & 0xFFL) << 24)
                | ((out[4] & 0xFFL) << 32)
                | ((out[5] & 0xFFL) << 40)
                | ((out[6] & 0xFFL) << 48)
                | ((out[7] & 0xFFL) << 56);
        assertEquals(val, Double.longBitsToDouble(bits), 0.0);
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private static byte[] extract(AutoExpandingBuffer b) {
        CompositeByteBuf comp = b.getBuffer();
        byte[] out = new byte[comp.readableBytes()];
        comp.getBytes(0, out);
        return out;
    }
}
