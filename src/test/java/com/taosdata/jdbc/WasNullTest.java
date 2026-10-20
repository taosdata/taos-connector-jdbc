/*
 * Copyright (c) 2022 TAOS Data, Inc.
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

package com.taosdata.jdbc;

import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@TestTarget(author = "huolibo", version = "3.0.1")
public class WasNullTest {
    static final String HOST = TestEnvUtil.getHost();
    static final String DB_NAME = TestUtils.camelToSnake(WasNullTest.class);
    static final String TABLE_NAME = "null_t";
    static Connection connection;
    static Statement statement;

    @Description("when get a null value, the wasNull will return true. and then is next is not null, wasNull is not null. Int")
    @Test
    public void nextWasNullInt() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from " + TABLE_NAME);
        resultSet.next();
        resultSet.getInt("c1");
        Assert.assertFalse(resultSet.wasNull());
        resultSet.next();
        resultSet.getInt("c1");
        Assert.assertTrue(resultSet.wasNull());
        resultSet.next();
        resultSet.getInt("c1");
        Assert.assertFalse(resultSet.wasNull());
    }

    @Description("when get a null value, the wasNull return true. and then is next is not null, wasNull is not null. String")
    @Test
    public void nextWasNullString() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from " + TABLE_NAME);
        resultSet.next();
        resultSet.getString("c2");
        Assert.assertFalse(resultSet.wasNull());
        resultSet.next();
        resultSet.getString("c2");
        Assert.assertTrue(resultSet.wasNull());
        resultSet.next();
        resultSet.getString("c2");
        Assert.assertFalse(resultSet.wasNull());
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + DB_NAME);
        statement.executeUpdate("create database if not exists " + DB_NAME);
        statement.executeUpdate("use " + DB_NAME);
        statement.executeUpdate("create table " + TABLE_NAME + " (ts timestamp, c1 int, c2 nchar(10))");
        statement.executeUpdate("insert into " + TABLE_NAME + " values(now, 1, 'peking')");
        statement.executeUpdate("insert into " + TABLE_NAME + " (ts) values(now+1s)");
        statement.executeUpdate("insert into " + TABLE_NAME + " (ts, c1, c2) values(now+2s, 2, 'chicago')");
    }

    @AfterClass
    public static void after() {
        try {
            if (statement != null) {
                statement.executeUpdate("drop database if exists " + DB_NAME);
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

