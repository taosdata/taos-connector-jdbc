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

package com.taosdata.jdbc.cloud;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotEquals;
@Ignore
public class CloudTest {
    final String[] strings = {"abc", "涛思数据"};
    final String[] types = {"BINARY", "TIMESTAMP"};

    @Test
    public void connectCloudService() throws Exception {

        String url = System.getenv("TDENGINE_CLOUD_URL");
        if (url == null || "".equals(url.trim())) {
            System.out.println("Environment variable for CloudTest not set properly");
            return;
        }

        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select server_version()");
        rs.next();
        String version = rs.getString(1);
        assertNotEquals(version, null);
        stmt.execute("insert into javatest.t0 values(now, 'abc')(now+1s, '涛思数据')");
        rs = stmt.executeQuery("select * from javatest.t0");
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertTrue(Arrays.stream(types).collect(Collectors.toSet()).contains(meta.getColumnTypeName(2)));
        while (rs.next()) {
            Assert.assertTrue(Arrays.stream(strings).collect(Collectors.toSet()).contains(rs.getString(2)));
        }
    }
}
