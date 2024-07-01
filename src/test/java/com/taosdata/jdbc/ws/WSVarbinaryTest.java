package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class WSVarbinaryTest {
    static String host = "127.0.0.1";
    static String dbName = "varbinary_test";
    static String tableNative = "varbinary_noraml";
    static String tableStmt = "varbinary_stmt";
    static Connection connection;
    static Statement statement;

    static String testStr = "20160601";
    static byte[] expectedArray = StringUtils.hexToBytes(testStr);

    @Test
    public void testInsert() throws Exception {
        statement.executeUpdate("insert into " + dbName + "." + tableNative + " values(now, \"\\x" + testStr + "\")");
        ResultSet resultSet = statement.executeQuery("select c1 from " + dbName + "." + tableNative);

        resultSet.next();
        byte[] result1 = resultSet.getBytes(1);
        Assert.assertArrayEquals(expectedArray, result1);
    }


    @Test
    public void testPrepare() throws SQLException {
        TSWSPreparedStatement preparedStatement = (TSWSPreparedStatement) connection.prepareStatement("insert into " + dbName + "." + tableStmt + " values (?, ?)");
        preparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));

        preparedStatement.setVarbinary(2, expectedArray);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        ResultSet resultSet = statement.executeQuery("select c1 from " + dbName + "." + tableStmt);
        while (resultSet.next()) {
            Assert.assertArrayEquals(expectedArray, resultSet.getBytes(1));
        }
    }

    @Test
    public void testPrepareOld() throws SQLException {
        TSWSPreparedStatement preparedStatement = (TSWSPreparedStatement) connection.prepareStatement("insert into " + dbName + "." + tableStmt + " values (?, ?)");

        long current = System.currentTimeMillis();
        ArrayList<Long> tsList = new ArrayList<>();
        tsList.add(current);
        preparedStatement.setTimestamp(0, tsList);

        ArrayList<byte[]> list1 = new ArrayList<>();
        list1.add(expectedArray);
        preparedStatement.setVarbinary(2, list1, 20);

        preparedStatement.columnDataAddBatch();
        preparedStatement.columnDataExecuteBatch();
        ResultSet resultSet = statement.executeQuery("select c1 from " + dbName + "." + tableStmt);
        while (resultSet.next()) {
            Assert.assertArrayEquals(expectedArray, resultSet.getBytes(1));
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&batchfetch=true";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + dbName);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("use " + dbName);
        statement.executeUpdate("create table " + tableNative + " (ts timestamp, c1 varbinary(20))");
        statement.executeUpdate("create table " + tableStmt + " (ts timestamp, c1 varbinary(20))");
    }

    @AfterClass
    public static void after() {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.executeUpdate("drop database if exists " + dbName);
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }
}
