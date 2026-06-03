package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

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

    @Test
    public void testSetObjectWithSqlType_traditionalRoute_roundTripsAllTypes() throws SQLException {
        long current = System.currentTimeMillis();
        try (PreparedStatement statement = prepareTSWSStatement(
                WsStmtWriteTestSupport.tableInsertSql(dbName, tableName))) {
            statement.setObject(1, new Timestamp(current), Types.TIMESTAMP);
            statement.setObject(2, (byte) 2, Types.TINYINT);
            statement.setObject(3, (short) 3, Types.SMALLINT);
            statement.setObject(4, 4, Types.INTEGER);
            statement.setObject(5, 5L, Types.BIGINT);
            statement.setObject(6, 6.6f, Types.FLOAT);
            statement.setObject(7, 7.7, Types.DOUBLE);
            statement.setObject(8, true, Types.BOOLEAN);
            statement.setObject(9, "hello", Types.VARCHAR);
            statement.setObject(10, "世界", Types.NCHAR);
            statement.setObject(11, "hello world", Types.VARCHAR);
            statement.setObject(12, WsStmtWriteTestSupport.EXPECTED_VAR_BINARY, Types.VARBINARY);
            statement.setObject(13, WsStmtWriteTestSupport.EXPECTED_GEOMETRY, Types.VARBINARY);
            statement.setObject(14, TSDBConstants.MAX_UNSIGNED_BYTE, Types.SMALLINT);
            statement.setObject(15, TSDBConstants.MAX_UNSIGNED_SHORT, Types.INTEGER);
            statement.setObject(16, TSDBConstants.MAX_UNSIGNED_INT, Types.BIGINT);
            statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.setObject(18, new TDBlob(WsStmtWriteTestSupport.EXPECTED_BLOB, true), Types.BLOB);
            statement.setObject(19, new BigDecimal(WsStmtWriteTestSupport.DECIMAL_VALUE_1), Types.DECIMAL);
            statement.setObject(20, new BigDecimal(WsStmtWriteTestSupport.DECIMAL_VALUE_2), Types.DECIMAL);

            Assert.assertEquals(1, statement.executeUpdate());

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                WsStmtWriteTestSupport.assertAllTypeRow(resultSet, current);
            }
        }
    }

    @Test
    public void testSetObjectWithSqlType_traditionalRoute_coercesBooleanAcrossNumericTypes() throws SQLException {
        long current = System.currentTimeMillis();
        String sql = "insert into " + dbName + "." + tableName
                + " (ts, c1, c2, c3, c4, c5, c6) values(?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = prepareTSWSStatement(sql)) {
            statement.setObject(1, new Timestamp(current), Types.TIMESTAMP);
            statement.setObject(2, Boolean.TRUE, Types.TINYINT);
            statement.setObject(3, Boolean.TRUE, Types.SMALLINT);
            statement.setObject(4, Boolean.TRUE, Types.INTEGER);
            statement.setObject(5, Boolean.TRUE, Types.BIGINT);
            statement.setObject(6, Boolean.TRUE, Types.FLOAT);
            statement.setObject(7, Boolean.TRUE, Types.DOUBLE);

            Assert.assertEquals(1, statement.executeUpdate());

            try (ResultSet resultSet = statement.executeQuery(
                    "select ts, c1, c2, c3, c4, c5, c6 from " + dbName + "." + tableName)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals((byte) 1, resultSet.getByte(2));
                Assert.assertEquals((short) 1, resultSet.getShort(3));
                Assert.assertEquals(1, resultSet.getInt(4));
                Assert.assertEquals(1L, resultSet.getLong(5));
                Assert.assertEquals(1f, resultSet.getFloat(6), 0.0001);
                Assert.assertEquals(1d, resultSet.getDouble(7), 0.0001);
                Assert.assertFalse(resultSet.next());
            }
        }
    }

    @Test
    public void testSetObjectWithSqlType_traditionalRoute_normalizesTemporalInputs() throws SQLException {
        long current = System.currentTimeMillis();
        String sql = "insert into " + dbName + "." + tableName + " (ts) values(?)";
        try (PreparedStatement statement = prepareTSWSStatement(sql)) {
            statement.setObject(1, new Date(current), Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, new Time(current + 1), Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, Instant.ofEpochMilli(current + 2), Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, DateTimeUtils.getLocalDateTime(Instant.ofEpochMilli(current + 3), null),
                    Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, DateTimeUtils.getOffsetDateTime(Instant.ofEpochMilli(current + 4), null),
                    Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, DateTimeUtils.getZonedDateTime(Instant.ofEpochMilli(current + 5), null),
                    Types.TIMESTAMP);
            statement.execute();

            try (ResultSet resultSet = statement.executeQuery(
                    "select ts from " + dbName + "." + tableName + " order by ts")) {
                for (int i = 0; i < 6; i++) {
                    Assert.assertTrue(resultSet.next());
                    Assert.assertEquals(new Timestamp(current + i), resultSet.getTimestamp(1));
                }
                Assert.assertFalse(resultSet.next());
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
        TestUtils.runInMain();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}
