//package com.taosdata.jdbc;
//
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.taosdata.jdbc.utils.SpecifyAddress;
//
//import java.math.BigDecimal;
//import java.sql.Connection;
//import java.sql.DatabaseMetaData;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.*;
//
//public class DatabaseMetaDataResultSetTest {
//
//    static Connection connection;
//    static String host = "127.0.0.1";
//    static DatabaseMetaDataResultSet resultSet;
//
//
//
//    @Before
//    public void setUp() {
//        DatabaseMetaDataResultSet resultSet;
//        List<TSDBResultSetRowData> dataList = new ArrayList<>();
//        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
//
//        dataList.add(new TSDBResultSetRowData(2));
//        dataList.add(new TSDBResultSetRowData(2));
//
//        ColumnMetaData columnMetaData = new ColumnMetaData();
//        columnMetaDataList.add(columnMetaData);
//
//        resultSet = new DatabaseMetaDataResultSet();
//
//        resultSet.setRowDataList(dataList);
//        resultSet.setColumnMetaDataList(columnMetaDataList);
//    }
//
//    @Test
//    public void testNext() throws SQLException {
//        assertTrue(resultSet.next());
//        assertTrue(resultSet.next());
//        assertFalse(resultSet.next());
//    }
//
//    @Test
//    public void testGetString() throws SQLException {
//        resultSet.next();
//        assertEquals("col1", resultSet.getString("COLUMN_NAME"));
//        assertEquals("text", resultSet.getString("TYPE_NAME"));
//    }
//
//    @Test
//    public void testGetInt() throws SQLException {
//        resultSet.next();
//        assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
//    }
//
//    @Test
//    public void testGetBigDecimal() throws SQLException {
//        resultSet.next();
//        assertEquals(new BigDecimal("1"), resultSet.getBigDecimal("ORDINAL_POSITION"));
//    }
//
//    @Test
//    public void testIsBeforeFirst() throws SQLException {
//        assertTrue(resultSet.isBeforeFirst());
//        resultSet.next();
//        assertFalse(resultSet.isBeforeFirst());
//    }
//
//    @Test
//    public void testIsAfterLast() throws SQLException {
//        assertFalse(resultSet.isAfterLast());
//        resultSet.next();
//        resultSet.next();
//        resultSet.next();
//        assertTrue(resultSet.isAfterLast());
//    }
//
//    @Test(expected = SQLException.class)
//    public void testGetStringInvalidColumn() throws SQLException {
//        resultSet.next();
//        resultSet.getString("INVALID_COLUMN");
//    }
//
//    @BeforeClass
//    public static void before() throws SQLException {
//        String url = SpecifyAddress.getInstance().getJniUrl();
//        if (url == null) {
//            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
//        }
//        connection = DriverManager.getConnection(url);
//    }
//
//    @AfterClass
//    public static void after() throws SQLException {
//        if (connection != null) {
//            connection.close();
//        }
//    }
//    // Add more tests for other methods as needed
//}
