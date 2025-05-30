package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UseNowInsertTimestampTest {
    private static String url ;
    private static String dbName = TestUtils.camelToSnake(UseNowInsertTimestampTest.class);

    @Test
    public void millisec() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName + " precision 'ms'");
            stmt.execute("use " + dbName);
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            Timestamp ts = rs.getTimestamp("ts");
            assertEquals(13, Long.toString(ts.getTime()).length());

            int nanos = ts.getNanos();
            assertEquals(0, nanos % 1000_000);

            stmt.execute("drop database if exists test");
        }
    }

    @Test
    public void microsec() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName + " precision 'us'");
            stmt.execute("use " + dbName);
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            Timestamp ts = rs.getTimestamp("ts");
            int nanos = ts.getNanos();

            assertEquals(0, nanos % 1000);

            stmt.execute("drop database if exists test");
        }
    }

    @Test
    public void nanosec() throws SQLException {
        long now_time = System.currentTimeMillis() * 1000_000L + 123456;
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName + " precision 'ns'");
            stmt.execute("use " + dbName);
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(" + now_time + ", 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();

            Timestamp ts = rs.getTimestamp("ts");

            int nanos = ts.getNanos();
            assertTrue(nanos % 1000 != 0);

            stmt.execute("drop database if exists test");
        }
    }

    @BeforeClass
    public static void beforeClass(){
        url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";
        }
    }

    @AfterClass
    public static void afterClass() {
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
