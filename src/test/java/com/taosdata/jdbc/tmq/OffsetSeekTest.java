package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OffsetSeekTest {
    private static ScheduledExecutorService scheduledExecutorService = null;
    private static Connection connection = null;
    private static Statement statement = null;
    private static final String host = "127.0.0.1";
    private static final String dbName = TestUtils.camelToSnake(OffsetSeekTest.class);
    private static final String superTable = "st";
    private static final String topic = "offset_seek_test";

    @Test
    public void testGetOffset() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, host + ":6030");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "jni");
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "gId");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");

        Map<TopicPartition, Long> offset = null;
        ResultBean tmp = null;

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                if (i == 0) {
                    offset = consumer.position(topic);
                }
                if (i == 5) {
                    if (offset != null) {
                        for (Map.Entry<TopicPartition, Long> entry : offset.entrySet()) {
                            consumer.seek(entry.getKey(), entry.getValue());
                        }
                        TimeUnit.SECONDS.sleep(1);
                    }
                }
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(500));
                // log
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean value = record.value();
                }
                if (i == 0) {
                    for (ConsumerRecord<ResultBean> record : records) {
                        tmp = record.value();
                        break;
                    }
                }
                if (i == 5) {
                    for (ConsumerRecord<ResultBean> record : records) {
                        ResultBean value = record.value();
                        Assert.assertEquals(tmp.getTs(), value.getTs());
                        break;
                    }
                }
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
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database if not exists " + dbName + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + dbName);
        statement.execute("create stable if not exists " + superTable
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + superTable + " tags(1000)");

        AtomicInteger a = new AtomicInteger(1);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                statement.executeUpdate(
                        "insert into ct0 values(now, " + a.getAndIncrement() + ", 0.2, 'a', '一', true)");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, c5, t1 from ct0");
    }

    @AfterClass
    public static void after() throws SQLException {
        scheduledExecutorService.shutdown();
        statement.execute("drop topic if exists " + topic);
        statement.execute("drop database if exists " + dbName);
        statement.close();
        connection.close();
    }

}
