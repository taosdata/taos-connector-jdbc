package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class WSPreparedStatementAllTypeTest {
    String host = "127.0.0.1";
    String db_name = "ws_prepare_type";
    String tableName = "wpt";
    Connection connection;

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        long current = System.currentTimeMillis();
        statement.setTimestamp(1, new Timestamp(current));
        statement.setByte(2, (byte) 2);
        statement.setShort(3, (short) 3);
        statement.setInt(4, 4);
        statement.setLong(5, 5L);
        statement.setFloat(6, 6.6f);
        statement.setDouble(7, 7.7);
        statement.setBoolean(8, true);
        statement.setString(9, "你好");
        statement.setNString(10, "世界");
        statement.setString(11, "hello world");
        statement.executeUpdate();

        ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName);
        resultSet.next();
        Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));
        Assert.assertEquals(resultSet.getByte(2), (byte) 2);
        Assert.assertEquals(resultSet.getShort(3), (short) 3);
        Assert.assertEquals(resultSet.getInt(4), 4);
        Assert.assertEquals(resultSet.getLong(5), 5L);
        Assert.assertEquals(resultSet.getFloat(6), 6.6f, 0.0001);
        Assert.assertEquals(resultSet.getDouble(7), 7.7, 0.0001);
        Assert.assertTrue(resultSet.getBoolean(8));
        Assert.assertEquals(resultSet.getString(9), "你好");
        Assert.assertEquals(resultSet.getString(10), "世界");
        Assert.assertEquals(resultSet.getString(11), "hello world");

        Assert.assertEquals(resultSet.getDate(1), new Date(current));
        Assert.assertEquals(resultSet.getTime(1), new Time(current));
        Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));
        Assert.assertEquals(resultSet.getBigDecimal(7).doubleValue(), 7.7, 0.000001);

        resultSet.close();
        statement.close();
    }

    @Test
    public void testExecuteCriticalValue() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setByte(2, (byte) 127);
        statement.setShort(3, (short) 32767);
        statement.setInt(4, 2147483647);
        statement.setLong(5, 9223372036854775807L);
        statement.setFloat(6, Float.MAX_VALUE);
        statement.setDouble(7, Double.MAX_VALUE);
        statement.setBoolean(8, true);
        statement.setString(9, "ABC");
        statement.setNString(10, "涛思数据");
        statement.setString(11, "陶");
        statement.executeUpdate();
        statement.close();
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&batchfetch=true";
        } else {
            url += "?user=root&password=taosdata&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name + " keep 36500");
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName +
                "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
                "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20))");
        statement.close();
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()){
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
    }
}
