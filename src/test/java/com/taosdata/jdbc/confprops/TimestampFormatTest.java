package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;

import java.sql.*;
import java.time.Instant;
import java.util.Properties;

@Ignore
public class TimestampFormatTest {
    private static final String host = "127.0.0.1";
    private long ts = Instant.now().toEpochMilli();
    private Connection conn;

    @Test
    public void utcInUrl() throws SQLException {
        // given
        String timestampFormat = "UTC";
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&timestampFormat=" + timestampFormat;
        } else {
            url = url + "&timestampFormat=" + timestampFormat;
        }
        // when & then
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from test.weather");
            while (rs.next()) {
                Object value = rs.getObject("ts");
                Assert.assertTrue(value instanceof Timestamp);
                String expect = new Timestamp(ts).toString();
                Assert.assertEquals(expect, value.toString());
            }
        }
    }

    @Test
    public void utcInProperties() throws SQLException {
        // given
        String timestampFormat = "UTC";
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        }
        // when
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {

            // then
            ResultSet rs = stmt.executeQuery("select * from test.weather");
            while (rs.next()) {
                Object value = rs.getObject("ts");
                Assert.assertTrue(value instanceof Timestamp);
                String expect = new Timestamp(ts).toString();
                Assert.assertEquals(expect, value.toString());
            }
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        }
        conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists test");
        stmt.execute("create database if not exists test");
        stmt.execute("use test");
        stmt.execute("create table weather(ts timestamp, temperature nchar(10))");
        stmt.execute("insert into weather values(" + ts + ", '北京')");
        stmt.close();
    }

    @After
    public void after() {
        try {
            if (null != conn) {
                Statement stmt = conn.createStatement();
                stmt.execute("drop database if exists test");
                stmt.close();
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
