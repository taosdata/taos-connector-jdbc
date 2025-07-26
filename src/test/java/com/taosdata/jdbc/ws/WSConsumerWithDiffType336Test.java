package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.Bean336;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WSConsumerWithDiffType336Test {
    private static final String host = "127.0.0.1";
    private static final String dbName = TestUtils.camelToSnake(WSConsumerWithDiffType336Test.class);
    private static final String superTable = "tmq_type";
    private static Connection connection;
    private static Statement statement;
    private static String[] topics = {"topic_ws_bean_diff_type"};
    private static byte[] expectedBinary = new byte[]{0x39, 0x38, 0x66, 0x34, 0x36, 0x33};
    private void insertOneRow() throws SQLException {
        statement.executeUpdate("insert into " + dbName + ".ct0 values(now, 1, 100, 2.2, 2.3, '1', 12, 2, true, '一', 'POINT(1 1)', '\\x0101', 1.2234, now, 255, 65535, 4294967295, 18446744073709551615, -12345678901234567890123.4567890000, 12345678.901234)");
    }

    @Test
    public void testWSBeanObject() throws Exception {
        insertOneRow();

        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic +
                " as select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, t1 from " + dbName + ".ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.common.Result336Deserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");

        try (TaosConsumer<Bean336> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Bean336> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Bean336> r : consumerRecords) {
                    Bean336 bean = r.value();
                    Assert.assertEquals(1.0, bean.getC1(), 0.000001);
                    Assert.assertEquals(100L, bean.getC2());
                    Assert.assertEquals(2.2, bean.getC3(), 0.000001);
                    Assert.assertEquals(2.3, bean.getC4(), 0.000001);
                    Assert.assertArrayEquals("1".getBytes(), bean.getC5());
                    Assert.assertEquals(12, bean.getC6());
                    Assert.assertEquals(2, bean.getC7());
                    Assert.assertTrue(bean.isC8());
                    Assert.assertEquals("一", bean.getC9());
                    Assert.assertEquals(1.2234, bean.getC12().doubleValue(), 0.000001);

                    Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, bean.getC14());
                    Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, bean.getC15());
                    Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, bean.getC16());
                    Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), bean.getC17());

                    Assert.assertEquals(new BigDecimal("-12345678901234567890123.4567890000"), bean.getC18());
                    Assert.assertEquals(new BigDecimal("12345678.901234"), bean.getC19());

                    Assert.assertEquals(1000.0, bean.getT1(), 0.000001);
                }
            }
            consumer.unsubscribe();
        }
    }

    @Test
    public void testWSBeanMap() throws Exception {
        //statement.executeUpdate("insert into " + dbName + ".ct0 values(now, 1, 100, 2.2, 2.3, '1', 12, 2, true, '一', 'POINT(1 1)', '\\x0101', 1.2234, now, 255, 65535, 4294967295, 18446744073709551615)");
        insertOneRow();

        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic +
                " as select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, t1 from " + dbName + ".ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    Map<String, Object> map = r.value();
                    Assert.assertEquals(21 , map.size());
                    Assert.assertTrue(map.get("ts") instanceof Timestamp);

                    Assert.assertEquals(1, (int) map.get("c1"));
                    Assert.assertEquals(100L, map.get("c2"));
                    Assert.assertEquals(2.2, (float)map.get("c3"), 0.000001);
                    Assert.assertEquals(2.3, (double)map.get("c4"), 0.000001);
                    Assert.assertArrayEquals("1".getBytes(), (byte[]) map.get("c5"));
                    Assert.assertEquals(12, (short)map.get("c6"));
                    Assert.assertEquals(2, (byte)map.get("c7"));
                    Assert.assertTrue((boolean) map.get("c8"));
                    Assert.assertEquals("一", map.get("c9"));
                    Assert.assertEquals(1.2234, (double)map.get("c12"), 0.000001);

                    Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, (short)map.get("c14"));
                    Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, (int)map.get("c15"));
                    Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, (long)map.get("c16"));
                    Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), map.get("c17"));

                    Assert.assertEquals(new BigDecimal("-12345678901234567890123.4567890000"), map.get("c18"));
                    Assert.assertEquals(new BigDecimal("12345678.901234"), map.get("c19"));

                    Assert.assertEquals(1000.0, (int)map.get("t1"), 0.000001);

                }
            }
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        TestUtils.runIn336();

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

        statement.executeUpdate("drop topic if exists " + topics[0]);
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database if not exists " + dbName + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + dbName);
        statement.execute("create stable if not exists " + superTable
                + " (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(10), c6 SMALLINT, c7 TINYINT, " +
                "c8 BOOL, c9 nchar(100), c10 GEOMETRY(100), c11 VARBINARY(100), c12 double, c13 timestamp, " +
                "c14 tinyint unsigned, c15 smallint unsigned, c16 int unsigned, c17 bigint unsigned, c18 decimal(38, 10), c19 decimal(18, 6)) tags(t1 int)");
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