package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

@Ignore
public class JNIConsumerTest {
    private static final String host = "127.0.0.1";
    private static Connection connection;
    private static String dbName = "consumer_test";
    private static String superTable = "super_table";
    private static String topic = "consumer_topic";

    @Test
    public void createConsumerWithPropTest() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty(TMQConstants.GROUP_ID, "A");
        TAOSConsumer consumer = TAOSConsumer.getInstance( properties);
        consumer.close();
    }

    @Test
    // don't end
    public void subscribeTest() throws SQLException {
        Properties properties = connection.getClientInfo();
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty(TMQConstants.GROUP_ID, "b");
        TAOSConsumer consumer = TAOSConsumer.getInstance(properties);
        consumer.subscribe(Collections.emptyList());
        consumer.close();
    }

    @Test
    public void subscribeWithTopicTest() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty(TMQConstants.GROUP_ID, "c");
        TAOSConsumer consumer = TAOSConsumer.getInstance( properties);
        consumer.subscribe(Collections.singletonList(topic));
        consumer.close();
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
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database if not exists " + dbName);
        statement.execute("use " + dbName);
        statement.execute("create stable if not exists " + superTable
                + " (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)");
        statement.execute("create table if not exists ct1 using " + superTable + " tags(2000)");
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1 from ct1");
    }
}