/*
 * Copyright (c) 2020 TAOS Data, Inc.
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

import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.concurrent.TimeUnit;

@Ignore
public class FailOverTest {

    @Test
    public void testFailOver() throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        final String url = "jdbc:TAOS://:/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        long end = System.currentTimeMillis() + 1000 * 60 * 5;
        while (System.currentTimeMillis() < end) {
            try (Connection conn = DriverManager.getConnection(url); Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("show dnodes");
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                    }
                    System.out.println();
                }
                System.out.println("=======================");
                rs.close();
                TimeUnit.SECONDS.sleep(5);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}

