package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class WSIpv6Test {
    private static final String host = "[::1]";
    private static final int port = 6041;
    private static Connection connection;
    private static final String databaseName = "driver";

    private static void testInsert() throws SQLException {
        Statement statement = connection.createStatement();
        long cur = System.currentTimeMillis();
        List<String> timeList = new ArrayList<>();
        for (long i = 0L; i < 30; i++) {
            long t = cur + i;
            timeList.add("insert into " + databaseName + ".alltype_query values(" + t + ",1,1,1)");
        }
        for (int i = 0; i < 30; i++) {
            statement.execute(timeList.get(i));
        }
        statement.close();
    }

    @Test
    public void testWSSelect() throws SQLException {
        Statement statement = connection.createStatement();
        int count = 0;
        long start = System.nanoTime();
        for (int i = 0; i < 1; i++) {
            ResultSet resultSet = statement.executeQuery("select ts,c1,c2,c3 from " + databaseName + ".alltype_query limit 3000");
            while (resultSet.next()) {
                count++;
                resultSet.getTimestamp(1);
                assertTrue(resultSet.getBoolean(2));
                assertEquals(1, resultSet.getInt(3));
                assertEquals(1, resultSet.getInt(4));
            }
        }
        long d = System.nanoTime() - start;
//        System.out.println(d / 1000);
//        System.out.println(count);
        statement.close();
    }


    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "10000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + databaseName);
        statement.execute("create database " + databaseName);
        statement.execute("create table " + databaseName + ".alltype_query(ts timestamp, c1 bool,c2 tinyint, c3 smallint)");
        statement.close();
        testInsert();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + databaseName);
        }
        connection.close();
    }
}
