package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;

import org.junit.*;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@FixMethodOrder
public class TaosConsumerTest {
    private static final String host = "127.0.0.1";
    private static final String dbName = "tmq_test";
    private static final String superTable = "st";
    private static Connection connection;
    private static Statement statement;

    @Test
    public void JNI_01_Test() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                statement.executeUpdate(
                        "insert into ct0 values(now, " + a.getAndIncrement() + ", 0.2, 'a','一')" +
                                "(now+1s," + a.getAndIncrement() + ",0.4,'b','二')" +
                                "(now+2s," + a.getAndIncrement() + ",0.6,'c','三')");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        String topic = "topic_ctb_column";
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, t1 from ct0");
//        statement.executeUpdate("create topic if not exists " + topic + " as database " + dbName);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "tg1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            Set<String> subscription = consumer.subscription();
            Assert.assertTrue(subscription.contains(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
                int count = 0;
                for (Map<String, Object> map : consumerRecords) {
                    count++;
                    Assert.assertEquals(6, map.size());
                }
                Assert.assertEquals(3, count);
            }
            TimeUnit.MILLISECONDS.sleep(10);
            consumer.unsubscribe();
        }
        scheduledExecutorService.shutdown();
    }

    @Test
    public void JNI_01_TestWithBean() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        List<String> strings = Arrays.asList("a", "b", "c");
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                statement.executeUpdate(
                        "insert into ct1 values(now, " + a.getAndIncrement() + ", 0.2, 'a','一')" +
                                "(now+1s," + a.getAndIncrement() + ",0.4,'b','二')" +
                                "(now+2s," + a.getAndIncrement() + ",0.6,'c','三')");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        TimeUnit.MILLISECONDS.sleep(11);
        String topic = "topic_ctb_column_with_bean";
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, t1 from ct1");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "withBean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<ResultBean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                int count = 0;
                for (ResultBean bean : consumerRecords) {
                    count++;
                    Assert.assertTrue(strings.contains(bean.getC3()));
                }
                Assert.assertEquals(3, count);
            }
            TimeUnit.MILLISECONDS.sleep(10);
            consumer.unsubscribe();
        }
        scheduledExecutorService.shutdown();
    }

    @Test
    @Ignore
    public void JNI_03_SyncCommitTest() throws Exception {

        String topic = "topic_sync";
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, t1 from ct0");

        Properties properties = new Properties();
//        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.GROUP_ID, "tg3");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                for (ResultBean bean : consumer.poll(Duration.ofMillis(100))) {
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                consumer.commitSync();
            }
            consumer.unsubscribe();
        }
    }

    @Test
    public void JNI_04_ASyncManualCommitTest() throws Exception {

        String topic = "topic_async";
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, t1 from ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "tg4");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                for (ResultBean bean : consumer.poll(Duration.ofMillis(100))) {
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                consumer.commitAsync((result, exception) -> {
                    // do nothing
                });
            }
            consumer.unsubscribe();
        }
    }

    @Test
    @Ignore
    public void JNI_04_ASyncAutoCommitTest() throws Exception {

        String topic = "topic_async_auto";
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, t1 from ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "tg5");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                for (ResultBean bean : consumer.poll(Duration.ofMillis(100))) {
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                consumer.commitAsync();
            }
            consumer.unsubscribe();
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
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10)) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + superTable + " tags(1000)");
        statement.execute("create table if not exists ct1 using " + superTable + " tags(2000)");
    }

    @AfterClass
    public static void after() {
        try {
            if (connection != null) {
                if (statement != null) {
                    statement.executeUpdate("drop database if exists " + dbName);
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}