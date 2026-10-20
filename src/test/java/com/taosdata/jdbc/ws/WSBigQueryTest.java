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

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.util.HashSet;
import java.util.Properties;

@FixMethodOrder
public class WSBigQueryTest {
    final String host = TestEnvUtil.getHost();
    final String db_name = "ws_prepare";
    final String tableName = TestUtils.camelToSnake(WSBigQueryTest.class);
    String superTable = "wpt_st";
    Connection connection;

    @Test
    public void testExecuteBatchInsert() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " (ts, c1) values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        HashSet<Object> collect = new HashSet<>();

        for (int i = 0; i < 20000; i++) {
            collect.add(i);
            statement.setTimestamp(1, new Timestamp(System.currentTimeMillis() + i));
            statement.setInt(2, i);
            statement.addBatch();
        }
        statement.executeBatch();

        String sql1 = "select * from " + db_name + "." + tableName;
        statement = connection.prepareStatement(sql1);
        boolean b = statement.execute();
        Assert.assertTrue(b);
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            Assert.assertTrue(collect.contains(resultSet.getInt(2)));
        }
        statement.close();
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName + " (ts timestamp, c1 int)");
        statement.close();
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
    }
}