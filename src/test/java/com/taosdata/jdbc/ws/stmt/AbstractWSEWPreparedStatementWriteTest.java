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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractWSEWPreparedStatementWriteTest {
    protected final String dbName = WsStmtWriteTestSupport.dbName(getClass());
    protected final String stableName = "swpt";
    protected Connection connection;
    private final String stmt2BindMode;
    private final Class<?> expectedStatementClass;

    protected AbstractWSEWPreparedStatementWriteTest(String stmt2BindMode, Class<?> expectedStatementClass) {
        this.stmt2BindMode = stmt2BindMode;
        this.expectedStatementClass = expectedStatementClass;
    }

    protected String stmt2BindMode() {
        return stmt2BindMode;
    }

    protected Class<?> expectedStatementClass() {
        return expectedStatementClass;
    }

    protected String expectedRouteName() {
        return expectedStatementClass().getSimpleName();
    }

    @Test
    public void testAutoCreateSubtable_roundTripsAllTypes() throws SQLException {
        String subTable = expectedRouteName().toLowerCase() + "_all_type";
        long current = System.currentTimeMillis();
        try (PreparedStatement statement = prepareWSEWStatement(asyncInsertSql())) {
            statement.setString(1, subTable);
            statement.setInt(2, 1);
            bindAllTypes(statement, 3, current);
            statement.addBatch();

            int[] result = statement.executeBatch();
            Assert.assertEquals(1, result.length);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, result[0]);
            Assert.assertEquals(1, statement.executeUpdate());
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from " + dbName + ".`" + subTable + "`")) {
            assertAllTypeRow(resultSet, current);
        }
    }

    @Test
    public void testPrecreatedSubtable_roundTripsNulls() throws SQLException {
        String subTable = expectedRouteName().toLowerCase() + "_nulls";
        long current = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            statement.execute("create table if not exists " + subTable
                    + " using " + dbName + "." + stableName + " tags(1)");
        }

        try (PreparedStatement statement = prepareWSEWStatement(stableInsertSql())) {
            statement.setString(1, subTable);
            bindNullTypes(statement, 2, current);
            statement.addBatch();

            int[] result = statement.executeBatch();
            Assert.assertEquals(1, result.length);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, result[0]);
            Assert.assertEquals(1, statement.executeUpdate());
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from " + dbName + ".`" + subTable + "`")) {
            assertNullTypeRow(resultSet, current);
        }
    }

    @Test
    public void testMultiSubtableBatch_roundTripsRowCounts() throws SQLException {
        int tableCount = 3;
        int rowsPerTable = 2;
        int totalRows = tableCount * rowsPerTable;
        String subTablePrefix = expectedRouteName().toLowerCase() + "_multi_";
        long current = System.currentTimeMillis();

        try (PreparedStatement statement = prepareWSEWStatement(asyncInsertSql())) {
            for (int table = 0; table < tableCount; table++) {
                for (int row = 0; row < rowsPerTable; row++) {
                    statement.setString(1, subTablePrefix + table);
                    statement.setInt(2, table);
                    bindAllTypes(statement, 3, current + table * 1000L + row);
                    statement.addBatch();
                }
            }

            int[] result = statement.executeBatch();
            Assert.assertEquals(totalRows, result.length);
            for (int item : result) {
                Assert.assertEquals(Statement.SUCCESS_NO_INFO, item);
            }
            Assert.assertEquals(totalRows, statement.executeUpdate());
        }

        assertRowCount(dbName + "." + stableName, totalRows);
        for (int table = 0; table < tableCount; table++) {
            assertRowCount(dbName + ".`" + subTablePrefix + table + "`", rowsPerTable);
        }
    }

    protected PreparedStatement prepareWSEWStatement(String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        Assert.assertTrue("Expected " + expectedRouteName() + ", got " + statement.getClass().getName(),
                expectedStatementClass().isInstance(statement));
        return statement;
    }

    @Before
    public void before() throws SQLException {
        connection = WsStmtWriteTestSupport.openWebSocketConnection(stmt2BindMode(), true);
        if ("column".equals(stmt2BindMode())) {
            WsStmtWriteTestSupport.assumeBindExecSupported(connection, expectedRouteName() + " write test");
        }
        WsStmtWriteTestSupport.recreateDatabase(connection, dbName);
        try (Statement statement = connection.createStatement()) {
            createStable(statement);
        }
    }

    @org.junit.After
    public void after() throws SQLException {
        WsStmtWriteTestSupport.dropDatabase(connection, dbName);
        if (connection != null) {
            connection.close();
        }
    }

    protected String stableInsertSql() {
        return WsStmtWriteTestSupport.stableInsertSql(dbName, stableName);
    }

    protected String asyncInsertSql() {
        return WsStmtWriteTestSupport.asyncInsertSql(dbName, stableName);
    }

    protected void createStable(Statement statement) throws SQLException {
        WsStmtWriteTestSupport.createAllTypeStable(statement, dbName, stableName);
    }

    protected void bindAllTypes(PreparedStatement statement, int startIndex, long current) throws SQLException {
        WsStmtWriteTestSupport.bindAllTypes(statement, startIndex, current);
    }

    protected void bindNullTypes(PreparedStatement statement, int startIndex, long current) throws SQLException {
        WsStmtWriteTestSupport.bindNullTypes(statement, startIndex, current);
    }

    protected void assertAllTypeRow(ResultSet resultSet, long current) throws SQLException {
        WsStmtWriteTestSupport.assertAllTypeRow(resultSet, current);
    }

    protected void assertNullTypeRow(ResultSet resultSet, long current) throws SQLException {
        WsStmtWriteTestSupport.assertNullTypeRow(resultSet, current);
    }

    private void assertRowCount(String tableExpression, int expectedRows) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select count(*) from " + tableExpression)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(expectedRows, resultSet.getInt(1));
        }
    }
}
