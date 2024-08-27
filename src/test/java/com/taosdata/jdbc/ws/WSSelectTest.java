package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class WSSelectTest {
    private static final String host = "127.0.0.1";
    private static final int port = 6041;
    private static Connection connection;
    private static final String databaseName = "driver";

    private static void testInsert() throws SQLException {
        Statement statement = connection.createStatement();
        long cur = System.currentTimeMillis();
        List<String> timeList = new ArrayList<>();
        for (long i = 0L; i < 30; i++) {
            long t = cur + i;
            timeList.add("insert into " + databaseName + ".alltype_query values(" + t + ",1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')");
        }
        for (int i = 0; i < 30; i++) {
            statement.execute(timeList.get(i));
        }
        statement.close();
    }

    @Test
    public void testWSSelect() throws SQLException {
        Statement statement = connection.createStatement();
        int count = 0;
        long start = System.nanoTime();
        for (int i = 0; i < 1; i++) {
            ResultSet resultSet = statement.executeQuery("select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 from " + databaseName + ".alltype_query limit 3000");
            while (resultSet.next()) {
                count++;
                resultSet.getTimestamp(1);
                resultSet.getBoolean(2);
                resultSet.getInt(3);
                resultSet.getInt(4);
                resultSet.getInt(5);
                resultSet.getLong(6);
                resultSet.getInt(7);
                resultSet.getInt(8);
                resultSet.getLong(9);
                resultSet.getLong(10);
                resultSet.getFloat(11);
                resultSet.getDouble(12);
                resultSet.getString(13);
                resultSet.getString(14);
            }
        }
        long d = System.nanoTime() - start;
//        System.out.println(d / 1000);
//        System.out.println(count);
        statement.close();
    }


    @Test
    public void testGetObject() throws SQLException {
        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery("select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 from " + databaseName + ".alltype_query limit 3000");
        if (resultSet.next()) {
            // Test LocalDateTime (Timestamp)
            LocalDateTime ts = resultSet.getObject("ts", LocalDateTime.class);
            assertNotNull(ts);

            // Test Boolean (TinyInt)
            Boolean c1 = resultSet.getObject("c1", Boolean.class);
            assertNotNull(c1);

            // Test Long (BigInt)
            Long c2 = resultSet.getObject("c2", Long.class);
            assertNotNull(c2);

            // Test Integer (SmallInt)
            Integer c3 = resultSet.getObject("c3", Integer.class);
            assertNotNull(c3);

            // Test Short (Int)
            Short c4 = resultSet.getObject("c4", Short.class);
            assertNotNull(c4);

            // Test Double (Float)
            Double c10 = resultSet.getObject("c10", Double.class);
            assertNotNull(c10);

            // Test Float (Double)
            Float c11 = resultSet.getObject("c11", Float.class);
            assertNotNull(c11);

            // Test BigDecimal (Float)
            BigDecimal c10BigDecimal = resultSet.getObject("c10", BigDecimal.class);
            assertNotNull(c10BigDecimal);

            // Test String (Binary)
            String c12 = resultSet.getObject("c12", String.class);
            assertEquals("test_binary", c12);

            // Test String (NChar)
            String c13 = resultSet.getObject("c13", String.class);
            assertEquals("test_nchar", c13);

            // Test Byte (TinyInt)
            Byte c2Byte = resultSet.getObject("c2", Byte.class);
            assertNotNull(c2Byte);


            // Test error condition
            try {
                resultSet.getObject("c1", LocalDateTime.class); // Invalid conversion
                fail("Expected SQLException not thrown");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Cannot convert"));
            }
        }
        resultSet.close();

        statement.execute("insert into " + databaseName + ".alltype_query values (NOW, null, null, null, null, null, null, null, null, null, null, null, null, null)");
        resultSet = statement.executeQuery("select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 from " + databaseName + ".alltype_query where c1 is null");
        if (resultSet.next()) {
            assertNull(resultSet.getObject("c1", Boolean.class));
            assertNull(resultSet.getObject("c2", Long.class));
            assertNull(resultSet.getObject("c10", BigDecimal.class));
            assertNull(resultSet.getObject("c12", String.class));
            assertNull(resultSet.getObject("c13", String.class));
        }
        resultSet.close();
        statement.close();
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "10000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + databaseName);
        statement.execute("create database " + databaseName);
        statement.execute("create table " + databaseName + ".alltype_query(ts timestamp, c1 bool,c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned, c10 float, c11 double, c12 binary(20), c13 nchar(30) )");
        statement.close();
        testInsert();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + databaseName);
        }
        connection.close();
    }
}
