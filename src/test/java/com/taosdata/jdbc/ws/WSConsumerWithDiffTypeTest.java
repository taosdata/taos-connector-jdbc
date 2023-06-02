package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class WSConsumerWithDiffTypeTest {
    private static final String host = "127.0.0.1";
    private static final String dbName = "tmq_ws_test_diff_type";
    private static final String superTable = "tmq_type";
    private static Connection connection;
    private static Statement statement;
    private static String[] topics = {"topic_ws_bean_diff_type"};

    @Test
    public void testWSBeanObject() throws Exception {
        try {
            statement.executeUpdate("insert into " + dbName + ".ct0 values(now, 1, 2.2, '1', '一', true)");
        } catch (SQLException e) {
            // ignore
        }
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic +
                " as select ts, c1, c2, c3, c4, c5, t1 from " + dbName + ".ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.ws.ResultDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");

        try (TaosConsumer<Bean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Bean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Bean> r : consumerRecords) {
                    Bean bean = r.value();
                    Assert.assertEquals(1.0, bean.getC1(), 0.000001);
                    Assert.assertEquals(2.2, bean.getC2(), 0.000001);
                    Assert.assertEquals(1, bean.getC3());
                    Assert.assertEquals("一", bean.getC4());
                    Assert.assertEquals(1, bean.getC5());
                    Assert.assertEquals(1000.0, bean.getT1(), 0.000001);
                }
            }
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        // properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database if not exists " + dbName + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + dbName);
        statement.execute("create stable if not exists " + superTable
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + superTable + " tags(1000)");
    }

    @AfterClass
    public static void after() {
        try {
            if (connection != null) {
                if (statement != null) {
                    for (String topic : topics) {
                        statement.executeUpdate("drop topic " + topic);
                        statement.executeUpdate("drop database if exists " + dbName);
                    }
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            //
        }
    }
}

class ResultDeserializer extends ReferenceDeserializer<Bean> {
}

class Bean {
    private long ts;
    private float c1;
    private double c2;
    private int c3;
    private String c4;
    private byte c5;
    private double t1;

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public float getC1() {
        return c1;
    }

    public void setC1(float c1) {
        this.c1 = c1;
    }

    public double getC2() {
        return c2;
    }

    public void setC2(double c2) {
        this.c2 = c2;
    }

    public int getC3() {
        return c3;
    }

    public void setC3(int c3) {
        this.c3 = c3;
    }

    public String getC4() {
        return c4;
    }

    public void setC4(String c4) {
        this.c4 = c4;
    }

    public double getT1() {
        return t1;
    }

    public void setT1(double t1) {
        this.t1 = t1;
    }

    public byte getC5() {
        return c5;
    }

    public void setC5(byte c5) {
        this.c5 = c5;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Bean{");
        sb.append("ts=").append(ts);
        sb.append(", c1=").append(c1);
        sb.append(", c2=").append(c2);
        sb.append(", c3=").append(c3);
        sb.append(", c4='").append(c4).append('\'');
        sb.append(", c5=").append(c5);
        sb.append(", t1=").append(t1);
        sb.append('}');
        return sb.toString();
    }
}