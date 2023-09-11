package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class VarbinaryTest {
    static String host = "127.0.0.1";
    static String dbName = "varbinary_test";
    static String tableNative = "varbinary_noraml";
    static String tableStmt = "varbinary_stmt";
    static Connection connection;
    static Statement statement;

    @Test
    public void testInsert() throws Exception {
        statement.executeUpdate("insert into " + dbName + "." + tableNative + " values(now, \"\\x8f4e3e\", \"\\x8f4e3e\")");
        statement.executeUpdate("insert into " + dbName + "." + tableNative + " values(now, \"8f4e3e\", \"8f4e3e\")");
        ResultSet resultSet = statement.executeQuery("select c1, c2 from " + dbName + "." + tableNative);
        resultSet.next();
        Assert.assertEquals("\\x8f4e3e", resultSet.getString(1));
        StringBuilder tmp = new StringBuilder();
        byte[] bytes = resultSet.getBytes(2);
        for (byte aByte : bytes) {
            tmp.append(Integer.toHexString(aByte & 0xFF));
        }
        Assert.assertEquals("8f4e3e", tmp.toString());
        resultSet.next();
        Assert.assertEquals("8f4e3e", resultSet.getString(1));
        Assert.assertEquals("8f4e3e", resultSet.getString(2));
    }

    @Test
    public void testPrepare() throws SQLException {
        TSDBPreparedStatement preparedStatement = (TSDBPreparedStatement) connection.prepareStatement("insert into " + dbName + "." + tableStmt + " values (?, ?, ?)");

        long current = System.currentTimeMillis();
        ArrayList<Long> tsList = new ArrayList<>();
        tsList.add(current);
        preparedStatement.setTimestamp(0, tsList);

        ArrayList<String> list = new ArrayList<>();
        list.add("\\x8f4e3e");
        preparedStatement.setString(1, list, 20);

        ArrayList<String> list1 = new ArrayList<>();
        list1.add("\\x8f4e3e");
        preparedStatement.setVarbinary(2, list1, 20);

        preparedStatement.columnDataAddBatch();
        preparedStatement.columnDataExecuteBatch();
        ResultSet resultSet = statement.executeQuery("select c1, c2 from " + dbName + "." + tableStmt);
        while (resultSet.next()) {
            Assert.assertEquals("\\x8f4e3e", resultSet.getString(1));
            Assert.assertEquals("\\x8f4e3e", resultSet.getString(2));
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + dbName);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("use " + dbName);
        statement.executeUpdate("create table " + tableNative + " (ts timestamp, c1 varchar(20), c2 varbinary(20))");
        statement.executeUpdate("create table " + tableStmt + " (ts timestamp, c1 varchar(20), c2 varbinary(20))");
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
