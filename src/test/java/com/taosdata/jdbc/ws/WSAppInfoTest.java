package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.common.BaseTest;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket query test", author = "huolibo", version = "2.0.38")
@FixMethodOrder
public class WSAppInfoTest extends BaseTest {
    private static final String host = "127.0.0.1";
    private static final int port = 6041;
    private Connection connection;
    private static final String appName = "jdbc_appName";
    private static final String appIp = "192.168.1.1";

    @Test
    public void AppInfoTest() throws SQLException, InterruptedException {

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("show connections")) {
                while (resultSet.next()) {

                    String name = resultSet.getString("user_app");
                    String ip = resultSet.getString("user_ip");

                    System.out.println("name: " + name + " ip: " + ip);
                    if (appName.equals(name)
                            && appIp.equals(ip)) {
                        return;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw e;
            }
        }
        Assert.fail("App info not found in show connections");
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "Asia/Shanghai");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_NAME, appName);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_IP, appIp);
        connection = DriverManager.getConnection(url, properties);
   }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            connection.close();
        }
    }
}
