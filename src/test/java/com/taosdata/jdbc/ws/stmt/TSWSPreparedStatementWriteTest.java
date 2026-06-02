package com.taosdata.jdbc.ws.stmt;

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

public class TSWSPreparedStatementWriteTest {
    private final String dbName = WsStmtWriteTestSupport.dbName(TSWSPreparedStatementWriteTest.class);
    private final String tableName = "wpt";
    private Connection connection;

    @Test
    public void testExecuteUpdate_roundTripsAllTypes() throws SQLException {
        long current = System.currentTimeMillis();
        try (PreparedStatement statement = prepareTSWSStatement(
                WsStmtWriteTestSupport.tableInsertSql(dbName, tableName))) {
            WsStmtWriteTestSupport.bindAllTypes(statement, 1, current);
            Assert.assertEquals(1, statement.executeUpdate());

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                WsStmtWriteTestSupport.assertAllTypeRow(resultSet, current);
            }
        }
    }

    @Test
    public void testExecuteUpdate_roundTripsNulls() throws SQLException {
        long current = System.currentTimeMillis();
        try (PreparedStatement statement = prepareTSWSStatement(
                WsStmtWriteTestSupport.tableInsertSql(dbName, tableName))) {
            WsStmtWriteTestSupport.bindNullTypes(statement, 1, current);
            Assert.assertEquals(1, statement.executeUpdate());

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                WsStmtWriteTestSupport.assertNullTypeRow(resultSet, current);
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
            WsStmtWriteTestSupport.createAllTypeTable(statement, dbName, tableName);
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
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}
