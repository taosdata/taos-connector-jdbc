package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Properties;


@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket master slave test", author = "yjshe", version = "3.2.11")
@FixMethodOrder
public class WSLoadBlanceTest2 {
    private static final String host = "127.0.0.1";


    @Description("query")
    @Test(expected = SQLException.class)
    public void queryBlockWithMasterSlaveDown() throws Exception  {
        TaosAdapterMock mockB = new TaosAdapterMock();
        TaosAdapterMock mockC = new TaosAdapterMock();

        mockB.start();
        mockC.start();
        mockB.stop();
        mockC.stop();

        Properties properties = new Properties();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + mockB.getListenPort() + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(mockC.getListenPort()));

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select 1;")) {
            resultSet.next();
            System.out.println(resultSet.getLong(1));
        }
    }

    @Description("query")
    @Test
    public void queryBlock() throws Exception  {
        TaosAdapterMock mockB = new TaosAdapterMock();
        TaosAdapterMock mockC = new TaosAdapterMock();

        mockB.start();
        mockC.start();

        Properties properties = new Properties();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + mockB.getListenPort() + "," + host + ":" + mockC.getListenPort() + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select 1")) {

        }
        mockB.stop();
        mockC.stop();
    }


    @Description("query")
    @Test(expected = SQLException.class)
    public void loadBalanceAndSlave() throws Exception  {
        TaosAdapterMock mockB = new TaosAdapterMock();
        TaosAdapterMock mockC = new TaosAdapterMock();

        mockB.start();
        mockC.start();

        Properties properties = new Properties();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + mockB.getListenPort() + "," + host + ":" + mockC.getListenPort() + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }

        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(mockC.getListenPort()));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select 1")) {

        }
        mockB.stop();
        mockC.stop();
    }

    @BeforeClass
    static public void before() throws SQLException, InterruptedException, IOException, URISyntaxException {
        System.setProperty("ENV_TAOS_JDBC_TEST", "false");
    }

}