/*
 * Copyright (c) 2023 TAOS Data, Inc.
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

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@SuppressWarnings("java:S1874")
public class SchemalessConnectionTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB = TestUtils.camelToSnake(SchemalessConnectionTest.class);
    private static Connection connection;

    @Test(expected = SQLException.class)
    public void testThroughJniConnectionAndUserPassword() throws SQLException {
        String url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/";
        Connection conn = DriverManager.getConnection(url, TestEnvUtil.getUser(), TestEnvUtil.getPassword());
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
    }

    @Test(expected = SQLException.class)
    public void testThroughWSConnectionAndUserPassword() throws SQLException {
        String url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/";
        Connection conn = DriverManager.getConnection(url, TestEnvUtil.getUser(), TestEnvUtil.getPassword());
        SchemalessWriter writer = new SchemalessWriter(conn);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
    }

    @Test
    public void testThroughJniUrl() throws SQLException {
        String url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/";
        SchemalessWriter writer = new SchemalessWriter(url, TestEnvUtil.getUser(), TestEnvUtil.getPassword(), DB);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
    }

    @Test
    public void testThroughWSUrl() throws SQLException {
        String url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/";
        SchemalessWriter writer = new SchemalessWriter(url, TestEnvUtil.getUser(), TestEnvUtil.getPassword(), DB);
        writer.write("measurement,host=host1 field1=2i,field2=2.0 1577837300000", SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/";
        connection = DriverManager.getConnection(url, TestEnvUtil.getUser(), TestEnvUtil.getPassword());
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop database if exists " + DB);
            statement.executeUpdate("create database " + DB);
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        String url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop database if exists " + DB);
        }
        connection.close();
    }
}

