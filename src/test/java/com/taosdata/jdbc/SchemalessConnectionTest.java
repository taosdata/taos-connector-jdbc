package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SchemalessConnectionTest {
    private static final String host = "127.0.0.1";
    private static final String db = "schemaless_connction";
    private static Connection connection;

    @Test(expected = SQLException.class)
    public void testThroughJniConnectionAndUserPassword() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/";
        Connection conn = DriverManager.getConnection(url, "root", "taosdata");
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
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
        connection = DriverManager.getConnection(url, "root", "taosdata");
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop database if exists " + db);
            statement.executeUpdate("create database " + db);
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop database if exists " + db);
        }
        connection.close();
    }
}
