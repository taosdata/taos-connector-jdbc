package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
            statement.executeUpdate("insert into " + dbName + ".ct0 values(now, 1, 100, 2.2, 2.3, '1', 12, 2, true, '一', 'POINT(1 1)', '\\x0101', 1.2234)");
        } catch (SQLException e) {
            // ignore
        }
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic +
                " as select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, t1 from " + dbName + ".ct0");

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
                    Assert.assertEquals(100L, bean.getC2());
                    Assert.assertEquals(2.2, bean.getC3(), 0.000001);
                    Assert.assertEquals(2.3, bean.getC4(), 0.000001);
                    Assert.assertEquals("1", bean.getC5());
                    Assert.assertEquals(12, bean.getC6());
                    Assert.assertEquals(2, bean.getC7());
                    Assert.assertTrue(bean.isC8());
                    Assert.assertEquals("一", bean.getC9());
                    Assert.assertEquals(1.2234, bean.getC12().doubleValue(), 0.000001);

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
                + " (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(10), c6 SMALLINT, c7 TINYINT, c8 BOOL, c9 nchar(100), c10 GEOMETRY(100), c11 VARBINARY(100), c12 double) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + superTable + " tags(1000)");
    }

    @AfterClass
    public static void after() throws InterruptedException {
        try {
            if (connection != null) {
                if (statement != null) {
                    for (String topic : topics) {
                        TimeUnit.SECONDS.sleep(3);
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
    private int t1;
    private long ts;
    private int c1;
    private long c2;


    private float c3;
    private double c4;
    private byte[] c5;
    private short c6;

    private byte c7;
    private boolean c8;
    private String c9;
    private byte[] c10;
    private byte[] c11;



    private BigDecimal c12;

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Bean{");
        sb.append("ts=").append(ts);
        sb.append(", t1=").append(t1);
        sb.append(", c1=").append(c1);
        sb.append(", c2=").append(c2);
        sb.append(", c3=").append(c3);
        sb.append(", c4=").append(c4);
        sb.append(", c5=").append(java.util.Arrays.toString(c5));
        sb.append(", c6=").append(c6);
        sb.append(", c7=").append(c7);
        sb.append(", c8=").append(c8);
        sb.append(", c9='").append(c9).append('\'');
        sb.append(", c10=").append(java.util.Arrays.toString(c10));
        sb.append(", c11=").append(java.util.Arrays.toString(c11));
        sb.append(", c12=").append(c12);
        sb.append('}');
        return sb.toString();
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public int getC1() {
        return c1;
    }

    public void setC1(int c1) {
        this.c1 = c1;
    }

    public long getC2() {
        return c2;
    }

    public void setC2(long c2) {
        this.c2 = c2;
    }

    public float getC3() {
        return c3;
    }

    public void setC3(float c3) {
        this.c3 = c3;
    }

    public double getC4() {
        return c4;
    }

    public void setC4(double c4) {
        this.c4 = c4;
    }

    public byte[] getC5() {
        return c5;
    }

    public void setC5(byte[] c5) {
        this.c5 = c5;
    }

    public short getC6() {
        return c6;
    }

    public void setC6(short c6) {
        this.c6 = c6;
    }

    public byte getC7() {
        return c7;
    }

    public void setC7(byte c7) {
        this.c7 = c7;
    }

    public boolean isC8() {
        return c8;
    }

    public void setC8(boolean c8) {
        this.c8 = c8;
    }

    public String getC9() {
        return c9;
    }

    public void setC9(String c9) {
        this.c9 = c9;
    }

    public byte[] getC10() {
        return c10;
    }

    public void setC10(byte[] c10) {
        this.c10 = c10;
    }

    public byte[] getC11() {
        return c11;
    }

    public void setC11(byte[] c11) {
        this.c11 = c11;
    }
    public BigDecimal getC12() {
        return c12;
    }

    public void setC12(BigDecimal c12) {
        this.c12 = c12;
    }
    public int getT1() {
        return t1;
    }

    public void setT1(int t1) {
        this.t1 = t1;
    }

}