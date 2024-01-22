package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket query test", author = "sheyj", version = "2.3.7")
@FixMethodOrder
public class WSQueryBIMode {
    private static final String host = "127.0.0.1";
    private static final int port = 6041;
    private static final String db_name = "ws_query";
    private static final String tableName = "wq";
    private Connection connection;

    @Description("query")
    @Test
    @Ignore
    public void queryTbname() throws InterruptedException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM ws_bi_mode.meters LIMIT 1");
            resultSet.next();
           Assert.assertEquals(7, resultSet.getMetaData().getColumnCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Before
    public void before() throws SQLException {
        String url = "jdbc:TAOS-RS://192.168.1.98:6041/?user=root&password=taosdata&batchfetch=true&conmode=1";
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();

        statement.executeQuery("drop database if exists ws_bi_mode");
        statement.executeQuery("create database ws_bi_mode keep 36500");
        statement.executeQuery("use ws_bi_mode");

        statement.executeQuery("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)");
        statement.executeQuery("CREATE TABLE d1001 USING meters TAGS ('California.SanFrancisco', 2)");
        statement.executeQuery("INSERT INTO d1001 USING meters TAGS ('California.SanFrancisco', 2) VALUES (NOW, 10.2, 219, 0.32)");

        statement.close();

    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            connection.close();
        }
    }
}
