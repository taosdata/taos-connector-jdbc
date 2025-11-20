package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import org.junit.After;
import org.junit.Before;
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
public class WSConFailOverTest {
    private static final String hostA = "127.0.0.1";
    private static final int portA = 6041;

    private static final String hostB = "127.0.0.1";
    private static final int portB = 9041;
    private final String db_name = TestUtils.camelToSnake(WSConFailOverTest.class);
    private static final String tableName = "meters";
    private Connection connection;
    private TaosAdapterMock taosAdapterMock;

    @Description("query")
    @Test
    public void queryBlock() throws Exception  {
        try (Statement statement = connection.createStatement()) {

            ResultSet resultSet;
            for (int i = 0; i < 4; i++){
                try {
                    if (i == 2){
                        taosAdapterMock.stop();
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
    }

    @Before
    public void before() throws SQLException, InterruptedException, IOException, URISyntaxException {
        taosAdapterMock = new TaosAdapterMock(9041);
        taosAdapterMock.start();

        String url;
        url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + hostA + ":" + portA + "/?user=root&password=taosdata";
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
        connection.close();

        url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + hostB + ":" + portB + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, hostA);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(portA));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        connection = DriverManager.getConnection(url, properties);
    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            Statement statement = connection.createStatement();
            statement.execute("drop database if exists " + db_name);
            statement.close();
            connection.close();
        }
    }
}