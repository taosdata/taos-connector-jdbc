package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.rs.RestfulDatabaseMetaData;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class WSDatabaseMetaDataTest {

    private static final String host = "127.0.0.1";
    private static String url;
    private static Connection connection;
    private static RestfulDatabaseMetaData metaData;
    private static final String dbName = "conn_mode";

    @Test
    public void getTablesView() throws SQLException {
        //BI 模式下，VIEW返回空
        String[] types = new String[]{"VIEW"};
        ResultSet rs = metaData.getTables(dbName, "", null, types);

        Assert.assertFalse(rs.next());;
    }

    @Test
    public void getTables() throws SQLException {
        String[] types = new String[]{"TABLE"};
        ResultSet rs = metaData.getTables(dbName, "", null, types);

        Assert.assertTrue(rs.next());;
    }

    @Test
    public void testShowDatabase() throws SQLException {

        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery("show user databases");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }


    @Test
    public void testShowTables() throws SQLException {

        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery("show  "+ dbName +".tables");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }


    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC+8");
        url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&batchfetch=true&conmode=1";
        }
        //url = "jdbc:TAOS-RS://vm98:7541/?user=root&password=taosdata&batchfetch=true&conmode=1";

        connection = DriverManager.getConnection(url, properties);
        Statement stmt = connection.createStatement();
        stmt.execute("drop database if exists " + dbName);
        stmt.execute("create database if not exists " + dbName + " precision 'us'");
        stmt.execute("use " + dbName);
        stmt.execute("create table `dn` (ts TIMESTAMP,cpu_taosd FLOAT,cpu_system FLOAT,cpu_cores INT,mem_taosd FLOAT,mem_system FLOAT,mem_total INT,disk_used FLOAT,disk_total INT,band_speed FLOAT,io_read FLOAT,io_write FLOAT,req_http INT,req_select INT,req_insert INT) TAGS (dnodeid INT,fqdn BINARY(128))");
        stmt.execute("insert into dn1 using dn tags(1,'a') (ts) values(now)");

        metaData = connection.getMetaData().unwrap(RestfulDatabaseMetaData.class);
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop database if exists " + dbName);
            }
            connection.close();
        }
    }

}