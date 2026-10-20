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

//package com.taosdata.jdbc;
//
//import com.taosdata.jdbc.utils.TestUtils;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
//
//import java.math.BigDecimal;
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//import static org.junit.Assert.*;
//
//public class DatabaseMetaDataResultSetTest {
//    static final String HOST = "127.0.0.1";
//    static final String DB_NAME = TestUtils.camelToSnake(DatabaseMetaDataResultSetTest.class);
//    static final String TABLE_NAME = "test";
//    static Connection connection;
//    static Statement statement;
//    static DatabaseMetaDataResultSet resultSet;
//
//
//
//    @Before
//    public void setUp() {
//        DatabaseMetaDataResultSet resultSet;
//        List<TSDBResultSetRowData> dataList = new ArrayList<>();
//        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
//
//        dataList.add(new TSDBResultSetRowData(2));
//        dataList.add(new TSDBResultSetRowData(2));
//
//        ColumnMetaData columnMetaData = new ColumnMetaData();
//        columnMetaDataList.add(columnMetaData);
//
//        resultSet = new DatabaseMetaDataResultSet();
//
//        resultSet.setRowDataList(dataList);
//        resultSet.setColumnMetaDataList(columnMetaDataList);
//    }
//
//    @Test
//    public void testNext() throws SQLException {
//        assertTrue(resultSet.next());
//        assertTrue(resultSet.next());
//        assertFalse(resultSet.next());
//    }
//
//    @Test
//    public void testGetString() throws SQLException {
//        resultSet.next();
//        assertEquals("col1", resultSet.getString("COLUMN_NAME"));
//        assertEquals("text", resultSet.getString("TYPE_NAME"));
//    }
//
//    @Test
//    public void testGetInt() throws SQLException {
//        resultSet.next();
//        assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
//    }
//
//    @Test
//    public void testGetBigDecimal() throws SQLException {
//        resultSet.next();
//        assertEquals(new BigDecimal("1"), resultSet.getBigDecimal("ORDINAL_POSITION"));
//    }
//
//    @Test
//    public void testIsBeforeFirst() throws SQLException {
//        assertTrue(resultSet.isBeforeFirst());
//        resultSet.next();
//        assertFalse(resultSet.isBeforeFirst());
//    }
//
//    @Test
//    public void testIsAfterLast() throws SQLException {
//        assertFalse(resultSet.isAfterLast());
//        resultSet.next();
//        resultSet.next();
//        resultSet.next();
//        assertTrue(resultSet.isAfterLast());
//    }
//
//    @Test(expected = SQLException.class)
//    public void testGetStringInvalidColumn() throws SQLException {
//        resultSet.next();
//        resultSet.getString("INVALID_COLUMN");
//    }
//
//
//    @BeforeClass
//    public static void before() throws SQLException {
//        String url = SpecifyAddress.getInstance().getJniUrl();
//        if (url == null) {
//            url = "jdbc:TAOS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
//        }
//        Properties properties = new Properties();
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
//        connection = DriverManager.getConnection(url, properties);
//        statement = connection.createStatement();
//        statement.executeUpdate("drop database if exists " + DB_NAME);
//        statement.executeUpdate("create database if not exists " + DB_NAME);
//        statement.executeUpdate("use " + DB_NAME);
//        statement.executeUpdate("create table " + TABLE_NAME + " (ts timestamp, c1 varbinary(20))  tags(t1 varbinary(20))");
//    }
//
//    @AfterClass
//    public static void after() {
//        try {
//            if (statement != null && !statement.isClosed()) {
//                statement.executeUpdate("drop database if exists " + DB_NAME);
//                statement.close();
//            }
//            if (connection != null && !connection.isClosed()) {
//                connection.close();
//            }
//        } catch (SQLException e) {
//            // ignore
//        }
//    }
//}

