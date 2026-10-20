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

package com.taosdata.jdbc.cloud;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

@Ignore
public class CloudSchemalessNewTest {
    String url = null;
    public static Connection connection;
    final String dbName = "javatest";

    @Before
    public void before() throws SQLException {
        url = System.getenv("TDENGINE_CLOUD_URL");
        if (url == null || "".equals(url.trim())) {
            System.out.println("Environment variable for CloudTest not set properly");
            return;
        }
        connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + dbName);
    }

    @Test
    public void testLine() throws SQLException {
        if (url == null || "".equals(url.trim())) {
            return;
        }

        // given
        long curTime = System.currentTimeMillis();
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\",ts=" + curTime + " c1=3i64,c3=L\"passit\",c2=false,c4=4f64 " + curTime};

        // when
        ((AbstractConnection)connection).write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
        // then
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from javatest.st order by _ts DESC limit 1");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        while (rs.next()) {
            Assert.assertEquals(curTime, rs.getLong("_ts"));
        }
        rs.close();
        statement.close();
    }
}
