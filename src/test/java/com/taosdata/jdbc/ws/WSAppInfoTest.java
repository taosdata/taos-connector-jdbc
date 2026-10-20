/*
 * Copyright (c) 2024 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket query test", author = "huolibo", version = "2.0.38")
@FixMethodOrder
public class WSAppInfoTest {

        static final String HOST = TestEnvUtil.getHost();
        static final int PORT = TestEnvUtil.getWsPort();
    private Connection connection;
    private static final String APP_NAME = "jdbc_appName";
    private static final String APP_IP = "192.168.1.1";

    @Test
    public void AppInfoTest() throws SQLException, InterruptedException {

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("show connections")) {
                while (resultSet.next()) {

                    String name = resultSet.getString("user_app");
                    String ip = resultSet.getString("user_ip");
                    String connectionInfo = resultSet.getString("connector_info");

                    System.out.println("name: " + name + ", ip: " + ip + ", connectionInfo: " + connectionInfo);
                    if (APP_NAME.equals(name)
                            && APP_IP.equals(ip)
                            && connectionInfo.split("-").length == 4
                            && connectionInfo.length() >= "jdbc-ws-v3.0.0-ncid000".length()) {
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
        TestUtils.runInMain();
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "";
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "Asia/Shanghai");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_NAME, APP_NAME);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_IP, APP_IP);
        connection = DriverManager.getConnection(url, properties);
    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            connection.close();
        }
    }
}

