package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket query test", author = "huolibo", version = "2.0.38")
@FixMethodOrder
public class WSQueryTest {
    private static final String host = "127.0.0.1";
    private static final int port = 6041;
    private final String db_name = TestUtils.camelToSnake(WSQueryTest.class);
    private static final String tableName = "wq";
    private Connection connection;

    @Description("query")
    @Test
    public void queryBlock() throws InterruptedException {
        int num = 10;
        CountDownLatch latch = new CountDownLatch(num);
        IntStream.range(0, num).parallel().forEach(x -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("insert into " + db_name + "." + tableName + " values(now+100s, 100)");
                try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName)) {
                    resultSet.next();
                    Assert.assertEquals(100, resultSet.getInt(2));
                }
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
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "80");
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=root&password=taosdata&batchfetch=true";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName + "(ts timestamp, f int)");
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

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}
