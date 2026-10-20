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

import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;

public class GetLongWithDifferentTimestampPrecisionTest {
    private static final String DB_NAME = TestUtils.camelToSnake(GetLongWithDifferentTimestampPrecisionTest.class);

    private final String host = "127.0.0.1";

    @Description("rest return value only for time, have no precision info")
    @Test
    @Ignore
    public void testRestful() throws SQLException {
        // given
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/";
        }
        Connection conn = DriverManager.getConnection(url, TestEnvUtil.getUser(), TestEnvUtil.getPassword());
        long ts = System.currentTimeMillis();

        // when and then
        assertResultSet(conn, "ms", ts, ts);
        assertResultSet(conn, "us", ts, ts * 1000);
        assertResultSet(conn, "ns", ts, ts * 1000_000);
    }

    @Test
    public void testJni() throws SQLException {
        // given
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/";
        }
        Connection conn = DriverManager.getConnection(url, TestEnvUtil.getUser(), TestEnvUtil.getPassword());
        long ts = System.currentTimeMillis();

        // when and then
        assertResultSet(conn, "ms", ts, ts);
        assertResultSet(conn, "us", ts, ts * 1000);
        assertResultSet(conn, "ns", ts, ts * 1000_000);
    }

    private void assertResultSet(Connection conn, String precision, long timestamp, long expect) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME + " precision '" + precision + "'");
            stmt.execute("use " + DB_NAME);
            stmt.execute("create table weather(ts timestamp, f1 int)");

            String dateTimeStr = sdf.format(new Date(timestamp));
            stmt.execute("insert into weather values('" + dateTimeStr + "', 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            long actual = rs.getLong("ts");
            Assert.assertEquals(expect, actual);
            stmt.execute("drop database if exists " + DB_NAME);
        }
    }

}

