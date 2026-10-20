/*
 * Copyright (c) 2026 TAOS Data, Inc.
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TSWSPreparedStatementWrite336Test {
    private final String dbName = WsStmtWriteTestSupport.dbName(TSWSPreparedStatementWrite336Test.class);
    private final String tableName = "wpt";
    private Connection connection;

    @Test
    public void testExecuteUpdate_roundTripsAllTypesWithoutBlobAndDecimal() throws SQLException {
        long current = System.currentTimeMillis();
        try (PreparedStatement statement = prepareTSWSStatement(
                WsStmtWriteTestSupport.tableInsertSql336(dbName, tableName))) {
            WsStmtWriteTestSupport.bindAllTypes336(statement, 1, current);
            Assert.assertEquals(1, statement.executeUpdate());

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                WsStmtWriteTestSupport.assertAllTypeRow336(resultSet, current);
            }
        }
    }

    @Test
    public void testExecuteUpdate_roundTripsNullsWithoutBlobAndDecimal() throws SQLException {
        long current = System.currentTimeMillis();
        try (PreparedStatement statement = prepareTSWSStatement(
                WsStmtWriteTestSupport.tableInsertSql336(dbName, tableName))) {
            WsStmtWriteTestSupport.bindNullTypes336(statement, 1, current);
            Assert.assertEquals(1, statement.executeUpdate());

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                WsStmtWriteTestSupport.assertNullTypeRow336(resultSet, current);
            }
        }
    }

    private PreparedStatement prepareTSWSStatement(String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        Assert.assertTrue("Expected TSWSPreparedStatement, got " + statement.getClass().getName(),
                statement instanceof TSWSPreparedStatement);
        return statement;
    }

    @Before
    public void before() throws SQLException {
        connection = WsStmtWriteTestSupport.openWebSocketConnection("traditional", false);
        WsStmtWriteTestSupport.recreateDatabase(connection, dbName);
        try (Statement statement = connection.createStatement()) {
            WsStmtWriteTestSupport.createAllTypeTable336(statement, dbName, tableName);
        }
    }

    @After
    public void after() throws SQLException {
        WsStmtWriteTestSupport.dropDatabase(connection, dbName);
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeClass
    public static void setUp() {
        TestUtils.runIn336();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}
