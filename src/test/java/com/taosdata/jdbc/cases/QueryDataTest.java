package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class QueryDataTest {

    static Connection connection;
    static Statement statement;
    static String dbName = TestUtils.camelToSnake(QueryDataTest.class);
    static String stbName = "meters";
    static String host = "127.0.0.1";

    @Before
    public void createDatabase() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":0/";
        }
        connection = DriverManager.getConnection(url, properties);

        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + dbName);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("use " + dbName);

        String createTableSql = "create table " + stbName + "(ts timestamp, name binary(64))";
        statement.executeUpdate(createTableSql);
    }

    @Test
    public void testQueryBinaryData() throws SQLException {
        String insertSql = "insert into " + stbName + " values(now, 'taosdata')";
        statement.executeUpdate(insertSql);

        String querySql = "select * from " + stbName;
        ResultSet rs = statement.executeQuery(querySql);

        while (rs.next()) {
            String name = rs.getString(2);
            assertEquals("taosdata", name);
        }
        rs.close();
    }

    @After
    public void close() throws SQLException {
        if (statement != null) {
            statement.execute("drop database if exists " + dbName);
            statement.close();
        }
        if (connection != null)
            connection.close();
    }

}