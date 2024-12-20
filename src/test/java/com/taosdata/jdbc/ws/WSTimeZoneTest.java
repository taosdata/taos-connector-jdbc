package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket query test", author = "huolibo", version = "2.0.38")
@FixMethodOrder
public class WSTimeZoneTest {
    private static final String host = "127.0.0.1";
    private static final int port = 6041;
    private static final String db_name = "ws_query";
    private static final String tableName = "wq";
    private Connection connection;

    @Test
    public void TimeZoneTest() throws SQLException, InterruptedException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from "  + db_name + "." + tableName + " limit 1")) {
            while (resultSet.next()) {

                Timestamp ts = resultSet.getTimestamp("ts");

                System.out.println("ts: " + ts);
                System.out.println("ts: " + ts.getTime());
                Assert.assertEquals("2024-01-01 00:00:00.0", ts.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void TimeZoneTest2() throws SQLException, InterruptedException {
        Instant instant = Instant.ofEpochMilli(1704038400000L);
        ZonedDateTime zonedDateTime1 = instant.atZone(ZoneId.of("Asia/Tokyo"));
        System.out.println("zonedDateTime1: " + zonedDateTime1);

        Instant instant2 = Instant.ofEpochMilli(1704038400000L);
        ZonedDateTime zonedDateTime2 = instant2.atZone(ZoneId.of("Asia/Shanghai"));
        System.out.println("zonedDateTime2: " + zonedDateTime2);

    }

    @Test
    public void TimeZoneTest3() throws SQLException, InterruptedException {
        long value = 1704038400000L; // 示例时间戳
        ZoneId zoneId = ZoneId.of("Asia/Tokyo");

        // 获取当前 UTC 时间
        Instant instant1 = Instant.ofEpochMilli(value);
        System.out.println("Original Instant (UTC): " + instant1);

        // 将 Instant 转换为特定时区的 ZonedDateTime
        ZonedDateTime zonedDateTime = instant1.atZone(zoneId);
        System.out.println("ZonedDateTime in Asia/Shanghai: " + zonedDateTime);

        // 将 ZonedDateTime 转换回 Timestamp
        Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
        System.out.println("Timestamp in Asia/Shanghai: " + timestamp);
    }

    @Test(expected = Exception.class)
    public void InvalidTimeZoneTest() throws SQLException, InterruptedException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "invalid/Tokyo");
        DriverManager.getConnection(url, properties);
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "Asia/Tokyo");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName + "(ts timestamp, f int)");

        // Asia/Shanghai +08:00, 2024-01-01 00:00:00
        statement.execute("insert into " + db_name + "." + tableName + " values (\"2024-01-01T00:00:00.000+08:00\", 1)");

        statement.close();
    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop database if exists " + db_name);
            } catch (SQLException e) {
                // do nothing
            }
            connection.close();
        }
    }
}
