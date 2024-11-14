// src/test/java/com/taosdata/jdbc/utils/DataTypeConverUtilTest.java
package com.taosdata.jdbc.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import org.junit.Test;

import com.taosdata.jdbc.enums.TimestampPrecision;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;

import static com.taosdata.jdbc.TSDBConstants.*;
import static org.junit.Assert.*;

public class DataTypeConverUtilTest {

    @Test
    public void testGetBoolean() throws SQLDataException {
        // Test TINYINT
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_TINYINT, (byte) 1));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_TINYINT, (byte) 0));

        // Test UTINYINT and SMALLINT
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_UTINYINT, (short) 1));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_UTINYINT, (short) 0));
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_SMALLINT, (short) 1));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_SMALLINT, (short) 0));

        // Test USMALLINT and INT
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_USMALLINT, 1));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_USMALLINT, 0));
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_INT, 1));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_INT, 0));

        // Test UINT and BIGINT
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_UINT, 1L));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_UINT, 0L));
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_BIGINT, 1L));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_BIGINT, 0L));

        // Test TIMESTAMP
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_TIMESTAMP, new Timestamp(1L)));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_TIMESTAMP, new Timestamp(0L)));

        // Test UBIGINT
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(1)));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(0)));

        // Test FLOAT
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_FLOAT, 1.0f));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_FLOAT, 0.0f));

        // Test DOUBLE
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_DOUBLE, 1.0));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_DOUBLE, 0.0));

        // Test NCHAR
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_NCHAR, "TRUE"));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_NCHAR, "FALSE"));

        // Test invalid NCHAR
        try {
            DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_NCHAR, "INVALID");
            fail("Expected SQLDataException");
        } catch (SQLDataException e) {
            // Expected exception
        }

        // Test BINARY
        assertTrue(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_BINARY, "TRUE".getBytes()));
        assertFalse(DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_BINARY, "FALSE".getBytes()));

        // Test invalid BINARY
        try {
            DataTypeConverUtil.getBoolean(TSDB_DATA_TYPE_BINARY, "INVALID".getBytes());
            fail("Expected SQLDataException");
        } catch (SQLDataException e) {
            // Expected exception
        }
    }

    @Test
    public void testGetByte() throws SQLException {
       // BOOL
        assertEquals(1, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_BOOL, true, 1));
        assertEquals(0, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_BOOL, false, 1));

        // UTINYINT and SMALLINT
        assertEquals((byte) 127, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_UTINYINT, (short) 127, 1));
        assertEquals((byte) -128, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_SMALLINT, (short) -128, 1));
        try {
            DataTypeConverUtil.getByte(TSDB_DATA_TYPE_UTINYINT, (short) 128, 1);
            fail("Expected SQLException for UTINYINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // USMALLINT and INT
        assertEquals((byte) 127, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_USMALLINT, 127, 1));
        assertEquals((byte) -128, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_INT, -128, 1));
        try {
            DataTypeConverUtil.getByte(TSDB_DATA_TYPE_USMALLINT, 128, 1);
            fail("Expected SQLException for USMALLINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // BIGINT and UINT
        assertEquals((byte) 127, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_BIGINT, 127L, 1));
        assertEquals((byte) -128, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_UINT, -128L, 1));
        try {
            DataTypeConverUtil.getByte(TSDB_DATA_TYPE_BIGINT, 128L, 1);
            fail("Expected SQLException for BIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // UBIGINT
        assertEquals((byte) 127, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(127), 1));
        assertEquals((byte) -128, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(-128), 1));
        try {
            DataTypeConverUtil.getByte(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(128), 1);
            fail("Expected SQLException for UBIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // FLOAT
        assertEquals((byte) 127, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_FLOAT, 127.0f, 1));
        assertEquals((byte) -128, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_FLOAT, -128.0f, 1));
        try {
            DataTypeConverUtil.getByte(TSDB_DATA_TYPE_FLOAT, 128.0f, 1);
            fail("Expected SQLException for FLOAT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // DOUBLE
        assertEquals((byte) 127, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_DOUBLE, 127.0, 1));
        assertEquals((byte) -128, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_DOUBLE, -128.0, 1));
        try {
            DataTypeConverUtil.getByte(TSDB_DATA_TYPE_DOUBLE, 128.0, 1);
            fail("Expected SQLException for DOUBLE out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // NCHAR
        assertEquals((byte) 127, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_NCHAR, "127", 1));
        assertEquals((byte) -128, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_NCHAR, "-128", 1));

        // BINARY
        assertEquals((byte) 127, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_BINARY, "127".getBytes(), 1));
        assertEquals((byte) -128, DataTypeConverUtil.getByte(TSDB_DATA_TYPE_BINARY, "-128".getBytes(), 1));

    }

    @Test
    public void testGetShort() throws SQLException {
        // BOOL
        assertEquals((short) 1, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_BOOL, true, 1));
        assertEquals((short) 0, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_BOOL, false, 1));

        // TINYINT
        assertEquals((short) 127, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1));
        assertEquals((short) -128, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1));

        // UTINYINT
        assertEquals((short) 255, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_UTINYINT, (short) 255, 1));

        // USMALLINT and INT
        assertEquals((short) 32767, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_USMALLINT, 32767, 1));
        assertEquals((short) -32768, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_INT, -32768, 1));
        try {
            DataTypeConverUtil.getShort(TSDB_DATA_TYPE_USMALLINT, 32768, 1);
            fail("Expected SQLException for USMALLINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // BIGINT and UINT
        assertEquals((short) 32767, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_BIGINT, 32767L, 1));
        assertEquals((short) -32768, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_UINT, -32768L, 1));
        try {
            DataTypeConverUtil.getShort(TSDB_DATA_TYPE_BIGINT, 32768L, 1);
            fail("Expected SQLException for BIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // UBIGINT
        assertEquals((short) 32767, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(32767), 1));
        assertEquals((short) -32768, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(-32768), 1));
        try {
            DataTypeConverUtil.getShort(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(32768), 1);
            fail("Expected SQLException for UBIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // FLOAT
        assertEquals((short) 32767, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_FLOAT, 32767.0f, 1));
        assertEquals((short) -32768, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_FLOAT, -32768.0f, 1));
        try {
            DataTypeConverUtil.getShort(TSDB_DATA_TYPE_FLOAT, 32768.0f, 1);
            fail("Expected SQLException for FLOAT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // DOUBLE
        assertEquals((short) 32767, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_DOUBLE, 32767.0, 1));
        assertEquals((short) -32768, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_DOUBLE, -32768.0, 1));
        try {
            DataTypeConverUtil.getShort(TSDB_DATA_TYPE_DOUBLE, 32768.0, 1);
            fail("Expected SQLException for DOUBLE out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // NCHAR
        assertEquals((short) 32767, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_NCHAR, "32767", 1));
        assertEquals((short) -32768, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_NCHAR, "-32768", 1));

        // BINARY
        assertEquals((short) 32767, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_BINARY, "32767".getBytes(), 1));
        assertEquals((short) -32768, DataTypeConverUtil.getShort(TSDB_DATA_TYPE_BINARY, "-32768".getBytes(), 1));
    }

    @Test
    public void testGetInt() throws SQLException {
        // BOOL
        assertEquals(1, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_BOOL, true, 1));
        assertEquals(0, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_BOOL, false, 1));

        // TINYINT
        assertEquals(127, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1));
        assertEquals(-128, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1));

        // UTINYINT and SMALLINT
        assertEquals(32767, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_UTINYINT, (short) 32767, 1));
        assertEquals(-32768, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_SMALLINT, (short) -32768, 1));

        // USMALLINT and INT
        assertEquals(2147483647, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_USMALLINT, 2147483647, 1));
        assertEquals(-2147483648, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_INT, -2147483648, 1));

        // BIGINT and UINT
        assertEquals(2147483647, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_BIGINT, 2147483647L, 1));
        assertEquals(-2147483648, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_UINT, -2147483648L, 1));
        try {
            DataTypeConverUtil.getInt(TSDB_DATA_TYPE_BIGINT, 2147483648L, 1);
            fail("Expected SQLException for BIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // UBIGINT
        assertEquals(2147483647, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(2147483647), 1));
        assertEquals(-2147483648, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(-2147483648), 1));
        try {
            DataTypeConverUtil.getInt(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(2147483648L), 1);
            fail("Expected SQLException for UBIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // FLOAT
        assertEquals(2147483647, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_FLOAT, 2147483647.0f, 1));
        assertEquals(-2147483648, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_FLOAT, -2147483648.0f, 1));
//        try {
//            DataTypeConverUtil.getInt(TSDB_DATA_TYPE_FLOAT, 2147483648.0f, 1);
//            fail("Expected SQLException for FLOAT out of range");
//        } catch (SQLException e) {
//            // Expected exception
//        }

        // DOUBLE
        assertEquals(2147483647, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_DOUBLE, 2147483647.0, 1));
        assertEquals(-2147483648, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_DOUBLE, -2147483648.0, 1));
        try {
            DataTypeConverUtil.getInt(TSDB_DATA_TYPE_DOUBLE, 2147483648.0, 1);
            fail("Expected SQLException for DOUBLE out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // NCHAR
        assertEquals(2147483647, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_NCHAR, "2147483647", 1));
        assertEquals(-2147483648, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_NCHAR, "-2147483648", 1));

        // BINARY
        assertEquals(2147483647, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_BINARY, "2147483647".getBytes(), 1));
        assertEquals(-2147483648, DataTypeConverUtil.getInt(TSDB_DATA_TYPE_BINARY, "-2147483648".getBytes(), 1));
  }

    @Test
    public void testGetLong() throws SQLException {
        // BOOL
        assertEquals(1L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_BOOL, true, 1, TimestampPrecision.MS));
        assertEquals(0L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_BOOL, false, 1, TimestampPrecision.MS));

        // TINYINT
        assertEquals(127L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1, TimestampPrecision.MS));
        assertEquals(-128L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1, TimestampPrecision.MS));

        // UTINYINT and SMALLINT
        assertEquals(32767L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_SMALLINT, (short) 32767, 1, TimestampPrecision.MS));
        assertEquals(-32768L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_SMALLINT, (short) -32768, 1, TimestampPrecision.MS));

        // USMALLINT and INT
        assertEquals(2147483647L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_INT, 2147483647, 1, TimestampPrecision.MS));
        assertEquals(-2147483648L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_INT, -2147483648, 1, TimestampPrecision.MS));

        // UINT and BIGINT
        assertEquals(9223372036854775807L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_BIGINT, 9223372036854775807L, 1, TimestampPrecision.MS));
        assertEquals(-9223372036854775808L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_BIGINT, -9223372036854775808L, 1, TimestampPrecision.MS));

        // UBIGINT
        assertEquals(9223372036854775807L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_UBIGINT, new BigDecimal(9223372036854775807L), 1, TimestampPrecision.MS));
        try {
            DataTypeConverUtil.getLong(TSDB_DATA_TYPE_UBIGINT, new BigDecimal("9223372036854775808"), 1, TimestampPrecision.MS);
            fail("Expected SQLException for UBIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // TIMESTAMP
        Timestamp ts = new Timestamp(1000L);
        assertEquals(1000L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, ts, 1, TimestampPrecision.MS));
        assertEquals(1000000L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, ts, 1, TimestampPrecision.US));
        assertEquals(1000000000L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, ts, 1, TimestampPrecision.NS));

        // FLOAT
        assertEquals(127L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_FLOAT, 127.0f, 1, TimestampPrecision.MS));
        try {
            DataTypeConverUtil.getLong(TSDB_DATA_TYPE_FLOAT, Float.MAX_VALUE, 1, TimestampPrecision.MS);
            fail("Expected SQLException for FLOAT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // DOUBLE
        assertEquals(127L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_DOUBLE, 127.0, 1, TimestampPrecision.MS));
        try {
            DataTypeConverUtil.getLong(TSDB_DATA_TYPE_DOUBLE, Double.MAX_VALUE, 1, TimestampPrecision.MS);
            fail("Expected SQLException for DOUBLE out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // NCHAR
        assertEquals(127L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_NCHAR, "127", 1, TimestampPrecision.MS));
        assertEquals(-128L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_NCHAR, "-128", 1, TimestampPrecision.MS));

        // BINARY
        assertEquals(127L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_BINARY, "127".getBytes(), 1, TimestampPrecision.MS));
        assertEquals(-128L, DataTypeConverUtil.getLong(TSDB_DATA_TYPE_BINARY, "-128".getBytes(), 1, TimestampPrecision.MS));

    }

    @Test
    public void testGetFloat() throws SQLException {
        // BOOL
        assertEquals(1.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_BOOL, true, 1), 0.0f);
        assertEquals(0.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_BOOL, false, 1), 0.0f);

        // TINYINT
        assertEquals(127.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1), 0.0f);
        assertEquals(-128.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1), 0.0f);

        // UTINYINT and SMALLINT
        assertEquals(32767.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_SMALLINT, (short) 32767, 1), 0.0f);
        assertEquals(-32768.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_SMALLINT, (short) -32768, 1), 0.0f);

        // USMALLINT and INT
        assertEquals(2147483647.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_INT, 2147483647, 1), 0.0f);
        assertEquals(-2147483648.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_INT, -2147483648, 1), 0.0f);

        // UINT and BIGINT
        assertEquals(9223372036854775807.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_BIGINT, 9223372036854775807L, 1), 0.0f);
        assertEquals(-9223372036854775808.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_BIGINT, -9223372036854775808L, 1), 0.0f);

        // UBIGINT
        assertEquals(1234567890123456789.0f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_UBIGINT, new BigDecimal("1234567890123456789"), 1), 0.0f);

        // DOUBLE
        assertEquals(3.141592653589793, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_DOUBLE, 3.141592653589793, 1), 0.0001f);

        // NCHAR
        assertEquals(123.45f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_NCHAR, "123.45", 1), 0.0f);

        // BINARY
        assertEquals(123.45f, DataTypeConverUtil.getFloat(TSDB_DATA_TYPE_BINARY, "123.45".getBytes(), 1), 0.0f);
    }

    @Test
    public void testGetDouble() throws SQLException {
        // BOOL
        assertEquals(1.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_BOOL, true, 1), 0.0);
        assertEquals(0.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_BOOL, false, 1), 0.0);

        // TINYINT
        assertEquals(127.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1), 0.0);
        assertEquals(-128.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1), 0.0);

        // UTINYINT and SMALLINT
        assertEquals(255.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_UTINYINT, (short) 255, 1), 0.0);
        assertEquals(32767.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_SMALLINT, (short) 32767, 1), 0.0);

        // USMALLINT and INT
        assertEquals(65535.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_USMALLINT, 65535, 1), 0.0);
        assertEquals(2147483647.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_INT, 2147483647, 1), 0.0);

        // UINT and BIGINT
        assertEquals(4294967295.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_UINT, 4294967295L, 1), 0.0);
        assertEquals(9223372036854775807.0, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_BIGINT, 9223372036854775807L, 1), 0.0);

        // UBIGINT
        assertEquals(1.0E19, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_UBIGINT, new BigDecimal("10000000000000000000"), 1), 0);

        // FLOAT
        assertEquals(3.14f, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_FLOAT, 3.14f, 1), 0.0);

        // DOUBLE
        assertEquals(3.141592653589793, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_DOUBLE, 3.141592653589793, 1), 0.0);

        // NCHAR
        assertEquals(123.456, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_NCHAR, "123.456", 1), 0.0);

        // BINARY
        assertEquals(789.012, DataTypeConverUtil.getDouble(TSDB_DATA_TYPE_BINARY, "789.012".getBytes(), 1), 0.0);
    }

    @Test
    public void testGetBytes() throws SQLException {
        // Test with byte array
        byte[] byteArray = {1, 2, 3};
        assertArrayEquals(byteArray, DataTypeConverUtil.getBytes(byteArray));

        // Test with String
        String str = "test";
        assertArrayEquals(str.getBytes(), DataTypeConverUtil.getBytes(str));

        // Test with Long
        long longValue = 123456789L;
        assertArrayEquals(Longs.toByteArray(longValue), DataTypeConverUtil.getBytes(longValue));

        // Test with Integer
        int intValue = 123456;
        assertArrayEquals(Ints.toByteArray(intValue), DataTypeConverUtil.getBytes(intValue));

        // Test with Short
        short shortValue = 12345;
        assertArrayEquals(Shorts.toByteArray(shortValue), DataTypeConverUtil.getBytes(shortValue));

        // Test with Byte
        byte byteValue = 123;
        assertArrayEquals(new byte[]{byteValue}, DataTypeConverUtil.getBytes(byteValue));

        // Test with Object (String representation)
        Object obj = new Object() {
            @Override
            public String toString() {
                return "object";
            }
        };
        assertArrayEquals("object".getBytes(), DataTypeConverUtil.getBytes(obj));

    }

    @Test
    public void testGetDate() {
        // Test with Timestamp
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Date expectedDate = new Date(timestamp.getTime());
        assertEquals(expectedDate, DataTypeConverUtil.getDate(timestamp));

        // Test with byte array representing a date string
        String dateString = "2023-10-10 10:10:10.123";
        try {
            byte[] dateBytes = dateString.getBytes("UTF-8");
            Date parsedDate = DataTypeConverUtil.getDate(dateBytes);
            assertEquals(Utils.parseDate(dateString), parsedDate);
        } catch (UnsupportedEncodingException e) {
            fail("Unexpected UnsupportedEncodingException: " + e.getMessage());
        }

        // Test with String
        Date parsedDateFromString = DataTypeConverUtil.getDate(dateString);
        assertEquals(Utils.parseDate(dateString), parsedDateFromString);

    }

    @Test
    public void testGetTime() {
        // Test with Timestamp
        Timestamp timestamp = new Timestamp(Instant.now().toEpochMilli());
        Time expectedTime = new Time(timestamp.getTime());
        assertEquals(expectedTime, DataTypeConverUtil.getTime(timestamp));

        // Test with byte array representing a valid time string
        String timeString = "12:34:56";
        byte[] timeBytes = timeString.getBytes();
        Time parsedTime = Time.valueOf(timeString);
        assertEquals(parsedTime, DataTypeConverUtil.getTime(timeBytes));

        // Test with invalid byte array (should throw RuntimeException)
        byte[] invalidBytes = "invalid".getBytes();
        try {
            DataTypeConverUtil.getTime(invalidBytes);
            fail("Expected RuntimeException for invalid byte array");
        } catch (RuntimeException e) {
            // Expected exception
        }

        // Test with String
        assertEquals(parsedTime, DataTypeConverUtil.getTime(timeString));

        // Test with invalid String (should throw RuntimeException)
        try {
            DataTypeConverUtil.getTime("invalid");
            fail("Expected RuntimeException for invalid string");
        } catch (RuntimeException e) {
            // Expected exception
        }
    }

    @Test
    public void testGetBigDecimal() {
        assertEquals(BigDecimal.ONE, DataTypeConverUtil.getBigDecimal(TSDB_DATA_TYPE_BOOL, true));
        assertEquals(BigDecimal.ZERO, DataTypeConverUtil.getBigDecimal(TSDB_DATA_TYPE_BOOL, false));
        assertEquals(new BigDecimal("123.456"), DataTypeConverUtil.getBigDecimal(TSDB_DATA_TYPE_DOUBLE, 123.456));
    }

    @Test
    public void testParseValue() {
        assertEquals(Boolean.TRUE, DataTypeConverUtil.parseValue(TSDB_DATA_TYPE_BOOL, (byte) 1, TimestampPrecision.MS));
        assertEquals(123, DataTypeConverUtil.parseValue(TSDB_DATA_TYPE_INT, 123, TimestampPrecision.MS));
    }

    @Test
    public void testParseTimestampColumnData() {
        long currentTimeMillis = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(currentTimeMillis);
        assertEquals(timestamp, DataTypeConverUtil.parseTimestampColumnData(currentTimeMillis, TimestampPrecision.MS));
    }

    // Add similar tests for getByte, getShort, getInt, getLong, getFloat,
    // getDouble, getBytes, getDate, getTime, getBigDecimal, parseValue, and
    // parseTimestampColumnData
}
