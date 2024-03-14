package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@FixMethodOrder
public class WSCompressTest {
    private static final String host = "127.0.0.1";
    private static final int port = 6041;
    private static final String db_name = "ws_query";
    private static final String tableName = "compress";

    private static final String topicName = "compress_topic";
    private static Connection connection;
    @Description("inertRows")
    @Test
    public void inertRows() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            // 必须超过1024个字符才会触发压缩
            statement.execute("INSERT INTO " + db_name + "." + tableName + " (tbname, location, groupId, ts, current, voltage, phase) \n" +
                    "                values('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:34.630', 10.2, 219, 0.32) \n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:35.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:36.780', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:37.781', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:38.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:39.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:40.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:41.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:42.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:43.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:44.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:45.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:46.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:47.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:48.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:49.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:50.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:51.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:52.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:53.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:54.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:55.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:56.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:57.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:58.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:59.779', 10.16, 217, 0.33)");

            ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName + " order by ts desc limit 1");
            resultSet.next();
            Assert.assertTrue(resultSet.getFloat(2) > 10.15);
        }
    }

    @Description("test schemaless insert use compress")
    @Test
    public void schemaless() throws SQLException {
        String lineDemo = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243097\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243100\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243101\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243102\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243103\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243104\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243105\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243106\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243107\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243108\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243109\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243110\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243111\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243112\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243113\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243114\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243115\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243116\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243117\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243118\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243119\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243120\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243121\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243122\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243123\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243124\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243125\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243126\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243127\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.4000002f64,voltage=219i32,phase=0.31f64 1710128243128\n";

        ((AbstractConnection)connection).write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        try (Statement statement = connection.createStatement()) {

            ResultSet resultSet = statement.executeQuery("select current from " + db_name + ".meters order by _ts desc limit 1");
            resultSet.next();
            Assert.assertTrue(resultSet.getFloat(1) > 10.3);
        }
    }

    @Description("test tmq use compress")
    @Test
    public void tmq() throws SQLException {
        inertRows();
        // create topic
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("create topic if not exists " + topicName + " as select * from " + tableName);
        }

        Properties properties = new Properties();
        properties.setProperty("td.connect.type", "ws");
        properties.setProperty("bootstrap.servers", "127.0.0.1:6041");
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "withBean");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapDeserializer");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION, "true");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topicName));
            for (int i = 0; i < 3; i++) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    Assert.assertEquals("California.SanFrancisco", new String((byte[]) r.value().get("location"), StandardCharsets.UTF_8));
                }
            }
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=root&password=taosdata&batchfetch=true";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.executeUpdate("drop topic if exists " + topicName);
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("CREATE STABLE " + tableName + " (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
        statement.close();
    }

    @AfterClass
    public static void after() throws SQLException {
        if (null != connection) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop topic if exists " + topicName);
                statement.executeUpdate("drop database if exists " + db_name);
            } catch (SQLException e) {
                // do nothing
            }
            connection.close();
        }
    }
}
