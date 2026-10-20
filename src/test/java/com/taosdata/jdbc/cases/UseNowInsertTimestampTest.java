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

package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UseNowInsertTimestampTest {
    private static String url ;
    private static final String DB_NAME = TestUtils.camelToSnake(UseNowInsertTimestampTest.class);

    @Test
    public void millisec() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME + " precision 'ms'");
            stmt.execute("use " + DB_NAME);
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            Timestamp ts = rs.getTimestamp("ts");
            assertEquals(13, Long.toString(ts.getTime()).length());

            int nanos = ts.getNanos();
            assertEquals(0, nanos % 1000_000);

            stmt.execute("drop database if exists " + DB_NAME);
        }
    }

    @Test
    public void microsec() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME + " precision 'us'");
            stmt.execute("use " + DB_NAME);
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            Timestamp ts = rs.getTimestamp("ts");
            int nanos = ts.getNanos();

            assertEquals(0, nanos % 1000);

            stmt.execute("drop database if exists " + DB_NAME);
        }
    }

    @Test
    public void nanosec() throws SQLException {
        long now_time = System.currentTimeMillis() * 1000_000L + 123456;
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME + " precision 'ns'");
            stmt.execute("use " + DB_NAME);
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(" + now_time + ", 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();

            Timestamp ts = rs.getTimestamp("ts");

            int nanos = ts.getNanos();
            assertTrue(nanos % 1000 != 0);

            stmt.execute("drop database if exists " + DB_NAME);
        }
    }

    @BeforeClass
    public static void beforeClass(){
        url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
    }

    @AfterClass
    public static void afterClass() {
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}

