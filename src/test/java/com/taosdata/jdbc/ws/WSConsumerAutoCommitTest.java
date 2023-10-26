package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WSConsumerAutoCommitTest {
    private static final String host = "127.0.0.1";
    private static final String dbName = "ws_tmq_auto_test";
    private static final String superTable = "st";
    private static String topic = "ws_topic_with_bean";

    private static Connection connection;

    @Test
    public void TestWithBean() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, host + ":6041");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.GROUP_ID, "withBean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.ws.WSConsumerAutoCommitTest$BeanDeserializer");

        Timestamp ts = null;
        try (TaosConsumer<Bean> consumer = new TaosConsumer<>(properties);) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 2; i++) {
                ConsumerRecords<Bean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Bean> r : consumerRecords) {
                    Bean bean = r.value();
                    if (ts == null) {
                        ts = bean.getTs();
                    } else {
                        assert ts.equals(bean.getTs());
                    }
                }
            }
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        try (Statement statement = connection.createStatement()) {

            statement.executeUpdate("drop topic if exists " + topic);
            statement.executeUpdate("drop database if exists " + dbName);
            statement.executeUpdate("create database if not exists " + dbName + " WAL_RETENTION_PERIOD 3650");
            statement.executeUpdate("use " + dbName);
            statement.executeUpdate("create stable if not exists " + superTable
                    + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");
            statement.executeUpdate("create table if not exists ct0 using " + superTable + " tags(1000)");
            statement.executeUpdate("insert into " + dbName + ".ct0 (ts) values (now)");
            statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, c5, t1 from ct0");
        }
    }

    @AfterClass
    public static void after() throws InterruptedException, SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("drop database if exists " + dbName);
        TimeUnit.SECONDS.sleep(3);
        connection.close();
       }

    static class BeanDeserializer extends ReferenceDeserializer<Bean> {
    }

    static class Bean {
        private Timestamp ts;
        private Integer c1;
        private Float c2;
        private String c3;
        private byte[] c4;
        private Integer t1;
        private Boolean c5;

        public Timestamp getTs() {
            return ts;
        }

        public void setTs(Timestamp ts) {
            this.ts = ts;
        }

        public Integer getC1() {
            return c1;
        }

        public void setC1(Integer c1) {
            this.c1 = c1;
        }

        public Float getC2() {
            return c2;
        }

        public void setC2(Float c2) {
            this.c2 = c2;
        }

        public String getC3() {
            return c3;
        }

        public void setC3(String c3) {
            this.c3 = c3;
        }

        public byte[] getC4() {
            return c4;
        }

        public void setC4(byte[] c4) {
            this.c4 = c4;
        }

        public Integer getT1() {
            return t1;
        }

        public void setT1(Integer t1) {
            this.t1 = t1;
        }

        public Boolean getC5() {
            return c5;
        }

        public void setC5(Boolean c5) {
            this.c5 = c5;
        }
    }
}
