package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class BlobTest {
    static String host = "127.0.0.1";
    static String dbName = TestUtils.camelToSnake(BlobTest.class);;
    static String tableNative = "blob_noraml";
    static Connection connection;
    static Statement statement;

    static String testStr = "20160601";
    static byte[] expectedArray = StringUtils.hexToBytes(testStr);


    @Test
    public void testInsert() throws Exception {
        statement.executeUpdate("insert into subt_a using " + dbName + "." + tableNative + "  tags( \"123456abcdef\")  values(now, \"\\x" + testStr + "\")");
        ResultSet resultSet = statement.executeQuery("select c1, t1 from " + dbName + "." + tableNative);
        resultSet.next();
        Assert.assertArrayEquals(expectedArray, resultSet.getBytes(1));
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
        statement.executeUpdate("create table " + tableNative + " (ts timestamp, c1 blob)  tags(t1 varchar(20))");
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
