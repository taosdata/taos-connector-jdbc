package com.taosdata.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DatabaseMetaDataResultSetTest {

    private DatabaseMetaDataResultSet resultSet;
    private List<ColumnMetaData> columnMetaDataList;
    private List<TSDBResultSetRowData> rowDataList;

    @Before
    public void setUp() {
        resultSet = new DatabaseMetaDataResultSet();
        columnMetaDataList = new ArrayList<>();
        rowDataList = new ArrayList<>();

        // Initialize column metadata
        columnMetaDataList.add(new ColumnMetaData());
        columnMetaDataList.add(new ColumnMetaData());

        // Initialize row data
        TSDBResultSetRowData rowData = new TSDBResultSetRowData(2);
        rowData.setString(1, "test");
        rowData.setInt(2, 123);
        rowDataList.add(rowData);

        resultSet.setColumnMetaDataList(columnMetaDataList);
        resultSet.setRowDataList(rowDataList);
    }

    @Test
    public void testNext() throws SQLException {
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
    }

    @Test
    public void testGetString() throws SQLException {
        resultSet.next();
        assertEquals("test", resultSet.getString(1));
    }

    @Test
    public void testGetInt() throws SQLException {
        resultSet.next();
        assertEquals(123, resultSet.getInt(2));
    }

    @Test
    public void testGetBoolean() throws SQLException {
        resultSet.next();
        assertFalse(resultSet.getBoolean(2)); // Assuming 123 is interpreted as false
    }

    @Test
    public void testGetBigDecimal() throws SQLException {
        resultSet.next();
        assertEquals(BigDecimal.valueOf(123), resultSet.getBigDecimal(2));
    }

    @Test
    public void testGetTimestamp() throws SQLException {
        resultSet.next();
        assertNull(resultSet.getTimestamp(1)); // Assuming no timestamp data
    }

    @Test
    public void testFindColumn() throws SQLException {
        assertEquals(1, resultSet.findColumn("column1"));
        assertEquals(2, resultSet.findColumn("column2"));
    }

    @Test(expected = SQLException.class)
    public void testFindColumnInvalid() throws SQLException {
        resultSet.findColumn("invalidColumn");
    }

    @Test
    public void testIsBeforeFirst() throws SQLException {
        assertTrue(resultSet.isBeforeFirst());
        resultSet.next();
        assertFalse(resultSet.isBeforeFirst());
    }

    @Test
    public void testIsAfterLast() throws SQLException {
        assertFalse(resultSet.isAfterLast());
        resultSet.next();
        resultSet.next();
        assertTrue(resultSet.isAfterLast());
    }

    @Test
    public void testIsFirst() throws SQLException {
        assertFalse(resultSet.isFirst());
        resultSet.next();
        assertTrue(resultSet.isFirst());
    }

    @Test
    public void testIsLast() throws SQLException {
        resultSet.next();
        assertTrue(resultSet.isLast());
    }

    @Test
    public void testGetRow() throws SQLException {
        assertEquals(0, resultSet.getRow());
        resultSet.next();
        assertEquals(1, resultSet.getRow());
    }

    @Test
    public void testIsClosed() throws SQLException {
        assertFalse(resultSet.isClosed());
    }

    // Add more tests for other methods as needed
}