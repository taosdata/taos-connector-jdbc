package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class AbstractDatabaseMetaDataColumnTest {
    static Connection connection;
    static String host = "127.0.0.1";
    static DatabaseMetaData metaData;

    @Test
    public void getColumnsAllNull() throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, null, null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()){
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
    }

    @Test
    public void getColumnsAll() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, "ins_tables", "stable_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()){
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    @Ignore
    public void getColumnsCatalog() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, null, null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()){
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsTable() throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, "ins_tables", null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()){
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsColumn() throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, null, "stable_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()){
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsCatalogTable() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, "ins_tables", null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()){
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    @Ignore
    public void getColumnsCatalogColumn() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, null, "stable_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()){
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsTableColumn() throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, "ins_tables", "stable_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()){
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
//                                                System.out.println(columns.getString("TABLE_CAT"));
//            System.out.println(columns.getString("TABLE_NAME"));
//            System.out.println(columns.getString("COLUMN_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        connection = DriverManager.getConnection(url);
        metaData = connection.getMetaData();
    }

    @AfterClass
    public static void after() throws SQLException {
        if (connection != null){
            connection.close();
        }
    }
}