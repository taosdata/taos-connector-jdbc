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

package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class CharsetTest {

    static final String HOST = TestEnvUtil.getHost();
            private final String dbName = TestUtils.camelToSnake(CharsetTest.class);

    @Test
    public void test() throws SQLException {
        // given
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");

        try (Connection conn = DriverManager.getConnection(url, props);
                Statement stmt = conn.createStatement()) {

            // when
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName);
            stmt.execute("use " + dbName);
            stmt.execute("create table weather(ts timestamp, temperature nchar(10))");
            stmt.execute("insert into weather values(now, '北京')");

            // then
            ResultSet rs = stmt.executeQuery("select * from weather");
            while (rs.next()) {
                Object value = rs.getObject("temperature");
                Assert.assertTrue(value instanceof String);
                Assert.assertEquals("北京", value.toString());
            }
        }
    }

    @After
    public void afterClass(){
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");

        try (Connection conn = DriverManager.getConnection(url, props);
                Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

