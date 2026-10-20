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

package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.sql.*;
import java.util.Properties;

@FixMethodOrder
public class WsPstmtStbNsTest {
            final String db_name = TestUtils.camelToSnake(WsPstmtStbNsTest.class);
    final String tableName = "wpt";
    String superTable = "wpt_st";
    Connection connection;

    @Test
    public void testQuery() throws SQLException {
        String sql = "insert into ? using " + db_name + "." + tableName + " tags(?) values(?, ?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= 1; i++) {
                // set table name
                pstmt.setString(1, "d_bind_" + i);

                // set tags
                pstmt.setInt(2, i);

                Timestamp ts = new Timestamp(System.currentTimeMillis());
                ts.setNanos(10000 + i);
                pstmt.setTimestamp(3, ts);
                pstmt.setInt(4, i + 10);

                pstmt.addBatch();
            }
            // execute column
            pstmt.executeBatch();
            // you can check exeResult here
            System.out.println("Successfully inserted " + 10 + " rows to power.meters.");
        }
    }

    private void initDbAndTable(String precision) throws SQLException, InterruptedException{
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        TestUtils.waitTransactionDone(connection);
        statement.execute("create database " + db_name + " PRECISION '" + precision + "'");
        TestUtils.waitTransactionDone(connection);
        statement.execute("use " + db_name);
        statement.execute("create stable if not exists " + db_name + "." + tableName + "(ts timestamp, c1 int) tags(t1 int)");

        statement.close();
    }

    @Before
    public void before() throws SQLException, InterruptedException {
        connection = DriverManager.getConnection(WsStmtWriteTestSupport.traditionalWebSocketUrl(), new Properties());
        initDbAndTable("ns");
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
            TestUtils.waitTransactionDone(connection);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        connection.close();
    }

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}