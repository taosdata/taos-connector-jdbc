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
}
