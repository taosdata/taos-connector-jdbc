package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * You need to start taosadapter before testing this method
 */
@RunWith(CatalogRunner.class)
@TestTarget(alias = "test connection with server", author = "huolibo", version = "2.0.37")
public class WSConnectionTest {
    //    private static final String host = "192.168.1.98";
    private static String host = "127.0.0.1";
    private static String port = "6041";
    private Connection connection;
    private final String db_name = "information_schema";

    @Test
    @Ignore
    @Description("normal test with websocket server")
    public void normalConnection() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":" + port + "/" + db_name + "?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
    }

    @Test
    @Description("url has no db")
    public void withoutDBConnection() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
    }

    @Test
    @Description("user and password in property")
    public void propertyUserPassConnection() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":" + port + "/";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
    }

    @Test(expected = SQLException.class)
    @Description("wrong password or user")
    public void wrongUserOrPasswordConnection() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":" + port + "/log?user=abc&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
    }

    @Test
    public void isValid() throws SQLException, IOException {
        String url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=root&password=taosdata";
        connection = DriverManager.getConnection(url);

        Assert.assertTrue(connection.isValid(10));
        Assert.assertTrue(connection.isValid(0));
    }

    @Test(expected = SQLException.class)
    public void isValidException() throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=root&password=taosdata";
        connection = DriverManager.getConnection(url);
        connection.isValid(-1);
    }

    @Test
    @Ignore
    public void bearerTokenTest() throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + port + "/?=bearerTokenTest=123";
        try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement()) {
            statement.execute("select 1");
        }
    }

    @Test
    public void testRetainHostPortPart() {
        // Test case 1: Full URL with multiple hosts, database and parameters
        String url1 = "jdbc:TAOS://host1:6030,host2:6030/mydb?user=root&password=taosdata";
        Assert.assertEquals("jdbc:TAOS://host1:6030,host2:6030",
                StringUtils.retainHostPortPart(url1));

        // Test case 2: URL with single host, database and no parameters
        String url2 = "jdbc:TAOS-WS://singlehost:6041/mydb2";
        Assert.assertEquals("jdbc:TAOS-WS://singlehost:6041",
                StringUtils.retainHostPortPart(url2));

        // Test case 3: URL with single host, database and charset parameter
        String url3 = "jdbc:TAOS-RS://h1:6030/db3?charset=utf8";
        Assert.assertEquals("jdbc:TAOS-RS://h1:6030",
                StringUtils.retainHostPortPart(url3));

        // Test case 4: URL with single host, no database and no parameters
        String url4 = "jdbc:TAOS://onlyhost:6030";
        Assert.assertEquals("jdbc:TAOS://onlyhost:6030",
                StringUtils.retainHostPortPart(url4));

        // Test case 5: URL with multiple hosts, no database and no parameters
        String url5 = "jdbc:TAOS-WS://h1:p1,h2:p2";
        Assert.assertEquals("jdbc:TAOS-WS://h1:p1,h2:p2",
                StringUtils.retainHostPortPart(url5));

        // Test case 6: URL with empty host/port (valid configuration)
        String url7 = "jdbc:TAOS-WS:///";
        Assert.assertEquals("jdbc:TAOS-WS://",
                StringUtils.retainHostPortPart(url7));

        // Test case 7: URL with host:port and empty database name
        String url8 = "jdbc:TAOS-RS://host:6030/";
        Assert.assertEquals("jdbc:TAOS-RS://host:6030",
                StringUtils.retainHostPortPart(url8));
    }

    @Test
    @Description("sleep keep connection")
    public void keepConnection() throws SQLException, InterruptedException {
        String url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
//        TimeUnit.SECONDS.sleep(20);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show databases");
        // Taosd recycles resources, if the sleep more than 30 seconds,
//        TimeUnit.SECONDS.sleep(30);
        resultSet.next();
        resultSet.close();
        statement.close();
        connection.close();
    }

    @BeforeClass
    public static void beforeClass() {
        String specifyHost = SpecifyAddress.getInstance().getHost();
        if (specifyHost != null) {
            host = specifyHost;
        }
        String specifyPort = SpecifyAddress.getInstance().getRestPort();
        if (specifyHost != null) {
            port = specifyPort;
        }
    }
}
