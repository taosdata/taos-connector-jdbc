package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WSConsumerSubscribeDBTest {
    private static final String host = "127.0.0.1";
    private static final String dbName = "tmq_ws_enh_test";
    private static final String superTable1 = "st1";
    private static final String superTable2 = "st2";
    private static Connection connection;
    private static Statement statement;
    private static String[] topics = {"topic_ws_map"};

    @Test
    public void testWSEhnMapDB() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as database " + dbName);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapEnhanceDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        int subTable1Count = 0;
        int subTable2Count = 0;
        try (TaosConsumer<TMQEnhMap> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<TMQEnhMap> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (i == 0){
                    statement.executeUpdate(
                            "insert into subtb1 using "+ superTable1 + " tags(1) values(now, " + a.getAndIncrement() + ", 0.2, 'a','一', true)" +
                                    "(now+1s," + a.getAndIncrement() + ",0.4,'b','二', false)" +
                                    "(now+2s," + a.getAndIncrement() + ",0.6,'c','三', false)");

                    statement.executeUpdate(
                            "insert into subtb2 using "+ superTable2 + " tags(1, 2) values(now, " + a.getAndIncrement() + ")" +
                                    "(now+1s," + a.getAndIncrement() + ")");
                }
                if (consumerRecords.isEmpty()){
                    continue;
                }
                for (ConsumerRecord<TMQEnhMap> r : consumerRecords) {
                    if (r.value().getTableName().equalsIgnoreCase("subtb1")){
                        Assert.assertEquals(6, r.value().getMap().size());
                        subTable1Count++;
                    }
                    if (r.value().getTableName().equalsIgnoreCase("subtb2")){
                        Assert.assertEquals(2, r.value().getMap().size());
                        subTable2Count++;
                    }
                }
            }

            Assert.assertEquals(3, subTable1Count);
            Assert.assertEquals(2, subTable2Count);
            consumer.unsubscribe();
        }
    }

    @Test
    public void testWSEhnMapStable() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as STABLE " + superTable1);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapEnhanceDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        int beforeCol = 0;
        int afterCol = 0;
        try (TaosConsumer<TMQEnhMap> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<TMQEnhMap> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (i == 0){
                    statement.executeUpdate(
                            "insert into subtb1 using "+ superTable1 + " tags(1) values(now, " + a.getAndIncrement() + ", 0.2, 'a','一', true)" +
                                    "(now+1s," + a.getAndIncrement() + ",0.4,'b','二', false)" +
                                    "(now+2s," + a.getAndIncrement() + ",0.6,'c','三', false)");


                    statement.executeUpdate(
                            "ALTER STABLE " + superTable1 + " ADD COLUMN c6 int");

                    statement.executeUpdate(
                            "insert into subtb1 using "+ superTable1 + " tags(1) values(now, " + a.getAndIncrement() + ", 0.2, 'a','一', true, 6)" +
                                    "(now+1s," + a.getAndIncrement() + ",0.4,'b','二', false, 7)");
                }
                if (consumerRecords.isEmpty()){
                    continue;
                }
                for (ConsumerRecord<TMQEnhMap> r : consumerRecords) {
                    if (r.value().getTableName().equalsIgnoreCase("subtb1") && r.value().getMap().size() == 6){
                        beforeCol++;
                    }
                    if (r.value().getTableName().equalsIgnoreCase("subtb1") && r.value().getMap().size() == 7){
                        afterCol++;
                    }
                }
            }
            Assert.assertEquals(3, beforeCol);
            Assert.assertEquals(2, afterCol);
            consumer.unsubscribe();
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
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
        statement.execute("create stable if not exists " + superTable1
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");

        statement.execute("create stable if not exists " + superTable2
                + " (ts timestamp, cc1 int) tags(t1 int, t2 int)");
    }

    @After
    public void after() throws InterruptedException {
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
