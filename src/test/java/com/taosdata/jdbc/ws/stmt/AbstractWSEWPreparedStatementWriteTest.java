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

    protected abstract String stmt2BindMode();

    protected abstract Class<?> expectedStatementClass();

    protected String expectedRouteName() {
        return expectedStatementClass().getSimpleName();
    }

    @Test
    public void testAutoCreateSubtable_roundTripsAllTypes() throws SQLException {
        String subTable = expectedRouteName().toLowerCase() + "_all_type";
        long current = System.currentTimeMillis();
        try (PreparedStatement statement = prepareWSEWStatement(
                WsStmtWriteTestSupport.asyncInsertSql(dbName, stableName))) {
            statement.setString(1, subTable);
            statement.setInt(2, 1);
            WsStmtWriteTestSupport.bindAllTypes(statement, 3, current);
            statement.addBatch();

            int[] result = statement.executeBatch();
            Assert.assertEquals(1, result.length);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, result[0]);
            Assert.assertEquals(1, statement.executeUpdate());
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from " + dbName + ".`" + subTable + "`")) {
            WsStmtWriteTestSupport.assertAllTypeRow(resultSet, current);
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

        try (PreparedStatement statement = prepareWSEWStatement(
                WsStmtWriteTestSupport.stableInsertSql(dbName, stableName))) {
            statement.setString(1, subTable);
            WsStmtWriteTestSupport.bindNullTypes(statement, 2, current);
            statement.addBatch();

            int[] result = statement.executeBatch();
            Assert.assertEquals(1, result.length);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, result[0]);
            Assert.assertEquals(1, statement.executeUpdate());
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from " + dbName + ".`" + subTable + "`")) {
            WsStmtWriteTestSupport.assertNullTypeRow(resultSet, current);
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
            WsStmtWriteTestSupport.createAllTypeStable(statement, dbName, stableName);
        }
    }

    @org.junit.After
    public void after() throws SQLException {
        WsStmtWriteTestSupport.dropDatabase(connection, dbName);
        if (connection != null) {
            connection.close();
        }
    }
}
