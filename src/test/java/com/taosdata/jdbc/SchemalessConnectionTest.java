package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class SchemalessConnectionTest {
    private final String host = "127.0.0.1";
    private final String db = "schemaless_connction";

    @Test
    public void testThroughJniConnectionAndUserPassword() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/";
        Connection conn = DriverManager.getConnection(url, "root", "taosdata");
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("select count(*) from " + db + ".measurement")) {
            Assert.assertNotNull(resultSet.getString(1));
        }
        conn.close();
    }

    @Test(expected = SQLException.class)
    public void testThroughWSConnectionAndUserPassword() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":6041/";
        Connection conn = DriverManager.getConnection(url, "root", "taosdata");
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
    }

    @Test
    public void testThroughJniUrl() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/";
        SchemalessWriter writer = new SchemalessWriter(url, "root", "taosdata", db);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
    }

    @Test
    public void testThroughWSUrl() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":6041/";
        SchemalessWriter writer = new SchemalessWriter(url, "root", "taosdata", db);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
    }

    @Before
    public void before() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/";
        try (Connection connection = DriverManager.getConnection(url, "root", "taosdata");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop database if exists " + db);
            statement.executeUpdate("create database " + db);
        }
    }
}
