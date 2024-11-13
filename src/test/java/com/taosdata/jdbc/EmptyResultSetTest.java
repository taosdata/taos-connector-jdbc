package com.taosdata.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class EmptyResultSetTest {

    private EmptyResultSet emptyResultSet;

    @Before
    public void setUp() {
        emptyResultSet = new EmptyResultSet();
    }

    @Test
    public void testNext() throws SQLException {
        assertFalse(emptyResultSet.next());
    }

    @Test
    public void testWasNull() throws SQLException {
        assertFalse(emptyResultSet.wasNull());
    }

    @Test
    public void testGetString() throws SQLException {
        assertNull(emptyResultSet.getString(1));
        assertNull(emptyResultSet.getString("column"));
    }

    @Test
    public void testGetBoolean() throws SQLException {
        assertFalse(emptyResultSet.getBoolean(1));
        assertFalse(emptyResultSet.getBoolean("column"));
    }

    @Test
    public void testGetByte() throws SQLException {
        assertEquals(0, emptyResultSet.getByte(1));
        assertEquals(0, emptyResultSet.getByte("column"));
    }

    @Test
    public void testGetShort() throws SQLException {
        assertEquals(0, emptyResultSet.getShort(1));
        assertEquals(0, emptyResultSet.getShort("column"));
    }

    @Test
    public void testGetInt() throws SQLException {
        assertEquals(0, emptyResultSet.getInt(1));
        assertEquals(0, emptyResultSet.getInt("column"));
    }

    @Test
    public void testGetLong() throws SQLException {
        assertEquals(0L, emptyResultSet.getLong(1));
        assertEquals(0L, emptyResultSet.getLong("column"));
    }

    @Test
    public void testGetFloat() throws SQLException {
        assertEquals(0.0f, emptyResultSet.getFloat(1), 0.0);
        assertEquals(0.0f, emptyResultSet.getFloat("column"), 0.0);
    }

    @Test
    public void testGetDouble() throws SQLException {
        assertEquals(0.0, emptyResultSet.getDouble(1), 0.0);
        assertEquals(0.0, emptyResultSet.getDouble("column"), 0.0);
    }

    @Test
    public void testGetBigDecimal() throws SQLException {
        assertNull(emptyResultSet.getBigDecimal(1));
        assertNull(emptyResultSet.getBigDecimal("column"));
    }

    @Test
    public void testGetBytes() throws SQLException {
        assertArrayEquals(new byte[0], emptyResultSet.getBytes(1));
        assertArrayEquals(new byte[0], emptyResultSet.getBytes("column"));
    }

    @Test
    public void testGetDate() throws SQLException {
        assertNull(emptyResultSet.getDate(1));
        assertNull(emptyResultSet.getDate("column"));
    }

    @Test
    public void testGetTime() throws SQLException {
        assertNull(emptyResultSet.getTime(1));
        assertNull(emptyResultSet.getTime("column"));
    }

    @Test
    public void testGetTimestamp() throws SQLException {
        assertNull(emptyResultSet.getTimestamp(1));
        assertNull(emptyResultSet.getTimestamp("column"));
    }

    @Test
    public void testGetMetaData() throws SQLException {
        assertNull(emptyResultSet.getMetaData());
    }

    @Test
    public void testGetWarnings() throws SQLException {
        assertNull(emptyResultSet.getWarnings());
    }

    @Test
    public void testGetCursorName() throws SQLException {
        assertNull(emptyResultSet.getCursorName());
    }

    @Test
    public void testGetStatement() throws SQLException {
        assertNull(emptyResultSet.getStatement());
    }

    @Test
    public void testIsClosed() throws SQLException {
        assertFalse(emptyResultSet.isClosed());
    }

    @Test
    public void testGetHoldability() throws SQLException {
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, emptyResultSet.getHoldability());
    }

    // Add more tests for other methods as needed
}
