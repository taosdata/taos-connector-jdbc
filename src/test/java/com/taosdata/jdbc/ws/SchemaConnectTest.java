package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SchemaConnectTest {
    private static final String host = "127.0.0.1";

    @Test
    public void testUrl() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/sml_test?user=root&password=taosdata";
        }
        SchemalessWriter writer = new SchemalessWriter(url, null, null, null);
    }

    @Test
    public void testAllParam() throws SQLException {
        SchemalessWriter writer = new SchemalessWriter("127.0.0.1", "6041", "root", "taosdata", "sml_test", "ws");
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        }

        try (Connection conn = DriverManager.getConnection(url);
             Statement statement = conn.createStatement()) {
            statement.executeUpdate("drop database if exists sml_test");
            statement.executeUpdate("create database sml_test");
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        }

        try (Connection conn = DriverManager.getConnection(url);
             Statement statement = conn.createStatement()) {
            statement.executeUpdate("drop database if exists sml_test");
        }
    }
}
