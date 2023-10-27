package com.taosdata.jdbc.ws;

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

    @Ignore
    @Description("query")
    @Test
    public void queryBlock() throws InterruptedException {
        int num = 10;
        CountDownLatch latch = new CountDownLatch(num);
        IntStream.range(0, num).parallel().forEach(x -> {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT ts, `current`, voltage, phase, location\n" +
                        "\t, groupid\n" +
                        "FROM (SELECT phase, `current`, groupid, location, ts, voltage FROM power.bmeters\n" +
                        ") SUB_QRY\n" +
                        "LIMIT 200");
                resultSet.next();
                Assert.assertEquals(219, resultSet.getInt(3));
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        });
        latch.await();
    }

    @Before
    public void before() throws SQLException {
        String url = "jdbc:TAOS-RS://192.168.1.98:7541/?user=root&password=taosdata&batchfetch=true&conmode=1";
//        String url = "jdbc:TAOS-RS://192.168.1.98:7541/?user=root&password=taosdata";


        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            connection.close();
        }
    }
}
