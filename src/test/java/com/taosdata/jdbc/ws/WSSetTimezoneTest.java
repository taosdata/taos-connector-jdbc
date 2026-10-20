/*
 * Copyright (c) 2021 TAOS Data, Inc.
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

import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * Integration tests for runtime timezone switching on WebSocket connections.
 * Requires a live TDengine with taosadapter (>= 3.3.5.0).
 */
@TestTarget(alias = "websocket set timezone test", author = "sheyj", version = "3.8.0")
public class WSSetTimezoneTest {

    static final String HOST = TestEnvUtil.getHost();
    static final int PORT = TestEnvUtil.getWsPort();

    // 2024-01-01T00:00:00Z in epoch milliseconds
    private static final long FIXED_EPOCH_MS = 1704067200000L;

    private final String dbName = TestUtils.camelToSnake(WSSetTimezoneTest.class) + "_" + UUID.randomUUID().toString().replace("-", "_");
    private Connection connection;

    @Before
    public void setUp() throws SQLException {
        String url = "jdbc:TAOS-WS://" + HOST + ":" + PORT + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        connection = DriverManager.getConnection(url);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("create database if not exists " + dbName + " precision 'ms'");
            stmt.execute("create table if not exists " + dbName + ".t1 (ts timestamp, v int)");
            stmt.execute("insert into " + dbName + ".t1 values (" + FIXED_EPOCH_MS + ", 1)");
        }
    }

    @After
    public void tearDown() throws SQLException {
        if (connection == null) {
            return;
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
        } finally {
            connection.close();
        }
    }

    @Test
    public void testSetTimezoneTakesEffectOnNextExecute() throws SQLException {
        WSConnection wsConnection = (WSConnection) connection;
        Assert.assertNull(wsConnection.getTimezone());

        Statement stmt = connection.createStatement();
        try {
            wsConnection.setTimezone("Asia/Tokyo");
            Assert.assertEquals("Asia/Tokyo", wsConnection.getTimezone());

            // the same statement picks up the new timezone on the next execute
            try (ResultSet rs = stmt.executeQuery("select ts from " + dbName + ".t1 limit 1")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("2024-01-01 09:00:00.0", rs.getString("ts"));
                // getTimestamp is zone-independent: the epoch always matches the stored value
                Assert.assertEquals(FIXED_EPOCH_MS, rs.getTimestamp("ts").getTime());
            }

            wsConnection.setTimezone("Asia/Shanghai");
            try (ResultSet rs = stmt.executeQuery("select ts from " + dbName + ".t1 limit 1")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("2024-01-01 08:00:00.0", rs.getString("ts"));
            }
        } finally {
            stmt.close();
        }
    }

    @Test
    public void testOpenResultSetKeepsItsTimezone() throws SQLException {
        WSConnection wsConnection = (WSConnection) connection;
        wsConnection.setTimezone("Asia/Tokyo");

        Statement stmt1 = connection.createStatement();
        Statement stmt2 = connection.createStatement();
        try {
            ResultSet rs1 = stmt1.executeQuery("select ts from " + dbName + ".t1 limit 1");
            Assert.assertTrue(rs1.next());

            // change the timezone and run a new query on another statement
            wsConnection.setTimezone("Asia/Shanghai");
            try (ResultSet rs2 = stmt2.executeQuery("select ts from " + dbName + ".t1 limit 1")) {
                Assert.assertTrue(rs2.next());
                Assert.assertEquals("2024-01-01 08:00:00.0", rs2.getString("ts"));
            }

            // the already open result set still formats with the old timezone
            Assert.assertEquals("2024-01-01 09:00:00.0", rs1.getString("ts"));
            rs1.close();
        } finally {
            stmt1.close();
            stmt2.close();
        }
    }

    @Test
    public void testTimezoneAffectsSqlTimeStringParsing() throws SQLException {
        // FIXED_EPOCH_MS is 2024-01-01T00:00:00Z, i.e. 09:00 in Tokyo and 08:00 in Shanghai
        WSConnection wsConnection = (WSConnection) connection;
        String sql = "select v from " + dbName + ".t1 where ts = '2024-01-01 09:00:00'";

        wsConnection.setTimezone("Asia/Tokyo");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue("time string literal must be parsed in the session timezone", rs.next());
        }

        wsConnection.setTimezone("Asia/Shanghai");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertFalse("the same time string literal denotes a different instant in another timezone", rs.next());
        }
    }

    @Test
    public void testInvalidTimezone() throws SQLException {
        WSConnection wsConnection = (WSConnection) connection;

        try {
            wsConnection.setTimezone("+08:00");
            Assert.fail("expected SQLException for offset timezone");
        } catch (SQLException expected) {
            // expected
        }
        try {
            wsConnection.setTimezone("Invalid/Zone");
            Assert.fail("expected SQLException for unknown timezone");
        } catch (SQLException expected) {
            // expected
        }
        Assert.assertNull(wsConnection.getTimezone());
    }

    @Test
    public void testClearTimezone() throws SQLException {
        WSConnection wsConnection = (WSConnection) connection;
        wsConnection.setTimezone("Asia/Tokyo");
        Assert.assertEquals("Asia/Tokyo", wsConnection.getTimezone());

        wsConnection.setTimezone(null);
        Assert.assertNull(wsConnection.getTimezone());

        // falls back to the JVM default timezone for formatting
        String expected = new Timestamp(FIXED_EPOCH_MS).toString();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select ts from " + dbName + ".t1 limit 1")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expected, rs.getString("ts"));
        }
    }
}
