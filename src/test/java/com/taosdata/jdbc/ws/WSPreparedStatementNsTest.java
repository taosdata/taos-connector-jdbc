package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;

import java.sql.*;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

@FixMethodOrder
public class WSPreparedStatementNsTest {
    String host = "127.0.0.1";
    String db_name = "ws_prepare";
    String tableName = "wpt";
    String superTable = "wpt_st";
    Connection connection;

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
//        statement.setTimestamp(1, new Timestamp(0));
        statement.setInt(2, 1);
        int i = statement.executeUpdate();
        Assert.assertEquals(1, i);
        statement.close();
    }

    @Test
    public void testExecuteBatchInsert() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            statement.setTimestamp(1, new Timestamp(System.currentTimeMillis() + i));
            statement.setInt(2, i);
            statement.addBatch();
        }
        statement.executeBatch();

        String sql1 = "select * from " + db_name + "." + tableName;
        statement = connection.prepareStatement(sql1);
        boolean b = statement.execute();
        Assert.assertTrue(b);
        ResultSet resultSet = statement.getResultSet();
        HashSet<Object> collect = Arrays.stream(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
                .collect(HashSet::new, HashSet::add, AbstractCollection::addAll);
        while (resultSet.next()) {
            Assert.assertTrue(collect.contains(resultSet.getInt(2)));
        }
        statement.close();
    }

    @Test
    public void testQuery() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            statement.setTimestamp(1, new Timestamp(System.currentTimeMillis() + i));
            statement.setInt(2, i);
            statement.addBatch();
        }
        statement.executeBatch();
        statement.close();

        sql = "select * from " + db_name + "." + tableName + " where ts > ? and ts < ?";
        statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setTimestamp(2, new Timestamp(System.currentTimeMillis() + 1000));
        ResultSet resultSet = statement.executeQuery();

        int i = 0;
        while (resultSet.next()) {
            Assert.assertEquals(resultSet.getInt(2), i++);
        }
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
        statement.execute("create database " + db_name + " PRECISION 'ns'");
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName + " (ts timestamp, c1 int)");
        statement.close();
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
    }
}