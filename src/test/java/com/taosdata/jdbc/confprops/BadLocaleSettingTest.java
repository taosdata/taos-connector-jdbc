package com.taosdata.jdbc.confprops;


import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.rs.RestfulResultSetMetaDataTest;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Ignore
public class BadLocaleSettingTest {

    private static final String host = "127.0.0.1";
    private static final String dbName = TestUtils.camelToSnake(BadLocaleSettingTest.class);
    private static Connection conn;

    @Test
    public void canSetLocale() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        conn = DriverManager.getConnection(url, properties);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + dbName);
        stmt.execute("create database if not exists " + dbName);
        stmt.execute("use " + dbName);
        stmt.execute("drop table if exists weather");
        stmt.execute("create table weather(ts timestamp, temperature float, humidity int)");
        stmt.executeUpdate("insert into weather values(1624071506435, 12.3, 4)");
        stmt.close();
    }

    @Before
    public void beforeClass() {
        System.setProperty("sun.jnu.encoding", "ANSI_X3.4-1968");
        System.setProperty("file.encoding", "ANSI_X3.4-1968");
    }

    @After
    public void afterClass() throws SQLException {
        if (conn != null) {
            Statement statement = conn.createStatement();
            statement.execute("drop database " + dbName);
            statement.close();
            conn.close();
        }
    }
}