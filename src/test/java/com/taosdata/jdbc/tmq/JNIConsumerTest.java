package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

public class JNIConsumerTest {
    private static final String host = "127.0.0.1";
    private static Connection connection;
    private String topic = "consumer_topic";

    @Test
    public void createConsumerTest() throws SQLException {
        TAOSConsumer consumer = TAOSConsumer.getInstance();
        consumer.close();
    }

    @Test
    public void createConsumerWithPropTest() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("enable.auto.commit", "true");
        TAOSConsumer consumer = TAOSConsumer.getInstance( properties);
        consumer.close();
    }

    @Test
    public void subscribeTest() throws SQLException {
        Properties properties = connection.getClientInfo();
        properties.setProperty("enable.auto.commit", "true");
        TAOSConsumer consumer = TAOSConsumer.getInstance(properties);
        consumer.subscribe(Collections.emptyList());
        consumer.close();
    }

    @Test
    public void subscribeWithTopicTest() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("enable.auto.commit", "true");
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
    }
}