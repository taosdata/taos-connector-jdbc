package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RestfulCompressTest {

    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static Connection connection;
    private static final String db_name = "restful_test";
    private static final String tableName = "compress";

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
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:59.779', 10.16, 217, 0.33)");

            ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName + " order by ts desc limit 1");
            resultSet.next();
            Assert.assertTrue(resultSet.getFloat(2) > 10.15);
        }
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            String url = SpecifyAddress.getInstance().getRestUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
            }
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION, "true");
            connection = DriverManager.getConnection(url, properties);
            Statement statement = connection.createStatement();
            statement.execute("drop database if exists " + db_name);
            statement.execute("create database " + db_name);
            statement.execute("use " + db_name);
            statement.execute("CREATE STABLE " + tableName + " (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null) {
            Statement stmt = connection.createStatement();
            stmt.execute("drop database if exists " + db_name);
            stmt.close();
            connection.close();
        }
    }

}
