package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class PrepareStatementUseDBTest {
    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static final String sql_insert = "insert into t1 values (?, ?)";
    private static final String dbname = "stmt_ws_use_db1";
    private static final String use_db = "stmt_ws_use_db2";

    @Test
    public void testUseDB() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/" + dbname + "?user=root&password=taosdata&batchfetch=true";
        }
        conn = DriverManager.getConnection(url);
        try (PreparedStatement stmt = conn.prepareStatement(sql_insert)) {
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            stmt.setInt(2, 1);
            stmt.addBatch();
            stmt.executeBatch();
            stmt.execute("use " + use_db);
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            stmt.setInt(2, 2);
            stmt.addBatch();
            stmt.executeBatch();
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&batchfetch=true";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        conn = DriverManager.getConnection(url, properties);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table t1 (ts timestamp, c1 int)");

            stmt.execute("drop database if exists " + use_db);
            stmt.execute("create database if not exists " + use_db);
            stmt.execute("use " + use_db);
            stmt.execute("create table t1 (ts timestamp, c1 int)");
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("drop database if exists " + use_db);
        }
        conn.close();
    }
}
