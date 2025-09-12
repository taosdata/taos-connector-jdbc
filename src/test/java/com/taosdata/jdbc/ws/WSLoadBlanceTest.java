package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Properties;


@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket master slave test", author = "yjshe", version = "3.2.11")
@FixMethodOrder
public class WSLoadBlanceTest {
    private static final String host = "127.0.0.1";
    private static final int portA = 6041;
    private static final String db_name = TestUtils.camelToSnake(WSLoadBlanceTest.class);
    private static final String tableName = "meters";
    static  private Connection connection;
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
             Statement statement = connection.createStatement()) {
            ResultSet resultSet;
            for (int i = 0; i < 4; i++){
                try {
                    if (i == 2){
                        mockB.stop();
                    }
                    resultSet = statement.executeQuery("select ts from " + db_name + "." + tableName + " limit 1;");

                }catch (SQLException e){
                    if (e.getErrorCode() == TSDBErrorNumbers.ERROR_RESULTSET_CLOSED){
                        System.out.println("connection closed");
                        break;
                    }

                    if (e.getErrorCode() ==  TSDBErrorNumbers.ERROR_QUERY_TIMEOUT){
                        System.out.println("req timeout, will be continue");
                        continue;
                    }

                    System.out.println(e.getMessage());
                    continue;
                }
                resultSet.next();
                System.out.println(resultSet.getLong(1));
                Thread.sleep(2000);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        mockB.stop();
        mockC.stop();
    }

    @Description("query")
    @Test
    public void queryBlockWithOneNodeDown() throws Exception  {
        TaosAdapterMock mockB = new TaosAdapterMock();
        TaosAdapterMock mockC = new TaosAdapterMock();

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

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select ts from " + db_name + "." + tableName + " limit 1;")) {
             resultSet.next();
             System.out.println(resultSet.getLong(1));
        }

        mockB.stop();
        mockC.stop();
    }

    @Description("query")
    @Test(expected = SQLException.class)
    public void queryBlockWithAllNodeDown() throws Exception  {
        TaosAdapterMock mockB = new TaosAdapterMock();
        TaosAdapterMock mockC = new TaosAdapterMock();

        mockB.start();
        mockC.start();
        mockB.stop();
        mockC.stop();

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

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select ts from " + db_name + "." + tableName + " limit 1;")) {
            resultSet.next();
            System.out.println(resultSet.getLong(1));
        }
    }

    @BeforeClass
    static public void before() throws SQLException, InterruptedException, IOException, URISyntaxException {
        System.setProperty("ENV_TAOS_JDBC_TEST", "test");
        String url;
        url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + portA + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();

        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName + "(ts timestamp, f int)");
        statement.execute("insert into " + db_name + "." + tableName + " values (now, 1)");
        statement.close();
    }

    @AfterClass
    static public void after() throws SQLException {
        if (null != connection) {
            Statement statement = connection.createStatement();
            statement.execute("drop database if exists " + db_name);
            statement.close();
            connection.close();
        }
    }
}