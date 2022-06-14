package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class TSDBConsumerTest {
    private static final String host = "127.0.0.1";
    private static final String dbName = "tmq_test";
    private static final String superTable = "st";
    private static Connection connection;
    private static Statement statement;

    @Test
    public void JNITest() throws Exception {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                statement.executeUpdate(
                        "insert into ct1 values(now, 1, 0.2, 'a')(now+1s,3,0.4,'b')(now+2s,3,0.4,'b')");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        String topic = "topic_ctb_column";
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1 from ct1");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "tg1");

        try (TAOSConsumer consumer = TAOSConsumer.getInstance(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                try (ResultSet resultSet = consumer.poll(Duration.ofMillis(100))) {
                    int count = 0;
                    while (resultSet.next()) {
                        count++;
                        System.out.println(resultSet.getString(1));
                    }
                    Assert.assertEquals(3, count);
                }
                TimeUnit.MILLISECONDS.sleep(10);
            }
            Set<String> subscription = consumer.subscription();
            Assert.assertEquals(1,subscription.size());
            consumer.unsubscribe();
        } finally {
            scheduledExecutorService.shutdown();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        // properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database if not exists " + dbName);
        statement.execute("use " + dbName);
        statement.execute("create stable if not exists " + superTable
                + " (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + superTable + " tags(1000)");
        statement.execute("create table if not exists ct1 using " + superTable + " tags(2000)");
        statement.execute("create table if not exists ct2 using " + superTable + " tags(3000)");
    }

    @AfterClass
    public static void after() {
        try {
            if (connection != null) {
                if (statement != null) {
                    // statement.executeUpdate("drop database if exists " + dbName);
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}