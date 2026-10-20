/*
 * Copyright (c) 2025 TAOS Data, Inc.
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
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;

/**
    * You need to start taosadapter before testing this method
    */
@RunWith(CatalogRunner.class)
@TestTarget(alias = "test connection with server", author = "huolibo", version = "2.0.37")
public class WSConnectionEnterpriseTest {
    private static String host = "localhost";
    private static String port = "6041";
    private Connection connection;

    @Test
//    @Ignore
    public void bearerTokenTest() throws SQLException {
        // create token
        String token = "";
        try (Connection conn = DriverManager.getConnection("jdbc:TAOS-WS://" + host + ":" + port + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword());
                Statement statement = conn.createStatement();){
                ResultSet rs = statement.executeQuery("CREATE TOKEN jdbc_token FROM user root ENABLE 1 PROVIDER 'root' ttl 1");
            TestUtils.waitTransactionDone(conn);
            rs.next();
            token = rs.getString(1);

            System.out.println("token = " + token);
            rs.close();
        }

        if (StringUtils.isEmpty(token)) {
            Assert.fail("no token created");
        }

        String url = "jdbc:TAOS-WS://" + host + ":" + port + "/?bearerToken=" + token;
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("show databases")) {

            while (rs.next()) {
                System.out.println(rs.getString(1));
                return;
            }
        } finally {
            // revoke token
            try (Connection conn = DriverManager.getConnection("jdbc:TAOS-WS://" + host + ":" + port + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword());
                    Statement statement = conn.createStatement();){
                statement.executeUpdate("drop token jdbc_token");
                TestUtils.waitTransactionDone(conn);
            }
        }

        Assert.fail("Not implemented yet");
    }

    @Test
    @Description("sleep keep connection")
    public void keepConnection() throws SQLException, InterruptedException {
        String url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show databases");
        resultSet.next();
        resultSet.close();
        statement.close();
        connection.close();
    }

    @BeforeClass
    public static void beforeClass() {
        TestUtils.runInEnterprise();
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

