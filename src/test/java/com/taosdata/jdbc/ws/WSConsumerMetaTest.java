package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.ws.tmq.meta.Meta;
import com.taosdata.jdbc.ws.tmq.meta.MetaCreateChildTable;
import com.taosdata.jdbc.ws.tmq.meta.MetaType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WSConsumerMetaTest {
    private static final String host = "127.0.0.1";
    private static final String dbName = "tmq_ws_test";
    private static final String superTable = "st";
    private static Connection connection;
    private static Statement statement;
    private static String[] topics = {"topic_ws_map", "topic_ws_bean"};

    private static volatile int count = 0;

    @Test
    public void testCreateChildTable() throws Exception {
        AtomicInteger a = new AtomicInteger(1);

        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + superTable);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Meta> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = a.getAndIncrement();
                String sql = String.format("create table if not exists %s using %s.%s tags(%s, '%s')", "ct" + idx, dbName, superTable, idx, "t" + idx);
                statement.execute(sql);
            }

            while (true) {
                ConsumerRecords<Meta> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Meta> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.value();
                    Assert.assertEquals(MetaType.CREATE, meta.getType());
                    Assert.assertEquals("ct1", meta.getTableName());
                }
                if (!consumerRecords.isEmpty()){
                    break;
                }
            }
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":6041/?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        // properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        for (String topic : topics) {
            statement.executeUpdate("drop topic if exists " + topic);
        }
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database if not exists " + dbName + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + dbName);
        statement.execute("create stable if not exists " + superTable
                + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10))");
    }

    @AfterClass
    public static void after() throws InterruptedException {
        try {
            if (connection != null) {
                if (statement != null) {
                    for (String topic : topics) {
                        TimeUnit.SECONDS.sleep(3);
                        statement.executeUpdate("drop topic if exists " + topic);
                    }
                    statement.executeUpdate("drop database if exists " + dbName);
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }
}
