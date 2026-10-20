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

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Ignore
public class HttpKeepAliveTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(HttpKeepAliveTest.class);
    private static Connection connection;

    @Test
    public void test() throws SQLException {
        // given
        int multi = 4000;
        AtomicInteger exceptionCount = new AtomicInteger();

        // when
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("create database if not exists " + DB_NAME);
        }
        List<Thread> threads = IntStream.range(0, multi).mapToObj(i -> new Thread(
                () -> {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute("insert into " + DB_NAME + ".tb_not_exists values(now, 1)");
                        stmt.execute("select last(*) from " + DB_NAME + ".dn");
                    } catch (SQLException throwables) {
                        exceptionCount.getAndIncrement();
                    }
                })).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // then
        Assert.assertEquals(multi, exceptionCount.get());
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties props = new Properties();
        props.setProperty("httpKeepAlive", "false");
        props.setProperty("httpPoolSize", "20");

        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        connection = DriverManager.getConnection(url, props);
    }

    @AfterClass
    public static void afterClass() {
        try (Statement statement = connection.createStatement()){
            statement.executeUpdate("drop database if exists " + DB_NAME);
        } catch (SQLException e) {
            // do nothing
        }
    }
}

