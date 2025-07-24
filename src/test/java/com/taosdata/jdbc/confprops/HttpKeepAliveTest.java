package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.common.BaseTest;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Ignore
public class HttpKeepAliveTest extends BaseTest {

    private static final String host = "127.0.0.1";
    private static final String db_name = TestUtils.camelToSnake(HttpKeepAliveTest.class);
    private static Connection connection;

    @Test
    public void test() throws SQLException {
        // given
        int multi = 4000;
        AtomicInteger exceptionCount = new AtomicInteger();

        // when
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("create database if not exists " + db_name);
        }
        List<Thread> threads = IntStream.range(0, multi).mapToObj(i -> new Thread(
                () -> {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute("insert into " + db_name + ".tb_not_exists values(now, 1)");
                        stmt.execute("select last(*) from " + db_name + ".dn");
                    } catch (SQLException throwables) {
                        exceptionCount.getAndIncrement();
                    }
                })).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // then
        Assert.assertEquals(multi, exceptionCount.get());
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties props = new Properties();
        props.setProperty("httpKeepAlive", "false");
        props.setProperty("httpPoolSize", "20");

        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        }
        connection = DriverManager.getConnection(url, props);
    }

    @AfterClass
    public static void afterClass() {
        try (Statement statement = connection.createStatement()){
            statement.executeUpdate("drop database if exists " + db_name);
        } catch (SQLException e) {
            // do nothing
        }
    }
}
