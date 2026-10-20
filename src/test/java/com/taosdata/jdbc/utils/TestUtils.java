/*
 * Copyright (c) 2025 TAOS Data, Inc.
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

package com.taosdata.jdbc.utils;

import org.junit.Assume;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

public class TestUtils {
    private static final AtomicInteger counter = new AtomicInteger(0);
    public static String camelToSnake(Class<?> clazz) {
        String className = clazz.getSimpleName();
        String temp = className.replaceAll("(?<=[a-z])([A-Z])", "_$1").toLowerCase();
        return temp +  counter.incrementAndGet();
    }

    public static void waitTransactionDone(Connection connection) {
        while (true) {
            try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("show transactions")){
                if (resultSet.next()) {
                    continue;
                }
                break;
            } catch (SQLException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
            }
        }
    }

    public static void runIn336(){
        String env = System.getenv("TD_3360_TEST");
        Assume.assumeTrue("true".equals(env));
    }

    public static void runInMain(){
        String env = System.getenv("TD_3360_TEST");
        Assume.assumeFalse("true".equals(env));
    }

    public static void runInEnterprise(){
        String env = System.getenv("TD_ENTERPRISE_TEST");
        Assume.assumeTrue("true".equals(env));
    }
}