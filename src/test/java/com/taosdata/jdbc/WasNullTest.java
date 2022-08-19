package com.taosdata.jdbc;

import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@TestTarget(author = "huolibo", version = "3.0.1")
public class WasNullTest {
    static String host = "127.0.0.1";
    static String dbName = "was_null_test";
    static String tableName = "null_t";
    static Connection connection;
    static Statement statement;

    @Description("when get a null value, the wasNull will retrun true. and then is next is not null, wasNull is not null. Int")
    @Test
    public void nextWasNullInt() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        resultSet.getInt("c1");
        Assert.assertFalse(resultSet.wasNull());
        resultSet.next();
        resultSet.getInt("c1");
        Assert.assertTrue(resultSet.wasNull());
        resultSet.next();
        resultSet.getInt("c1");
        Assert.assertFalse(resultSet.wasNull());
    }

    @Description("when get a null value, the wasNull retrun true. and then is next is not null, wasNull is not null. String")
    @Test
    public void nextWasNullString() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from " + tableName);
        resultSet.next();
        resultSet.getInt("c2");
        Assert.assertFalse(resultSet.wasNull());
        resultSet.next();
        resultSet.getInt("c2");
        Assert.assertTrue(resultSet.wasNull());
        resultSet.next();
        resultSet.getInt("c2");
        Assert.assertFalse(resultSet.wasNull());
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
        statement.executeUpdate("create table " + tableName + " (ts timestamp, c1 int, c2 nchar(10))");
        statement.executeUpdate("insert into " + tableName + " values(now, 1, 'peking')");
        statement.executeUpdate("insert into " + tableName + " (ts) values(now+1s)");
        statement.executeUpdate("insert into " + tableName + " (ts, c1, c2) values(now+2s, 2, 'chicago')");
    }

    @AfterClass
    public static void after() {
        try {
            if (statement != null) {
                statement.executeUpdate("drop database if exists " + dbName);
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
