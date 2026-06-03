package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.WSConnection;
import org.junit.Assert;
import org.junit.Assume;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;

final class WsStmtWriteTestSupport {
    static final byte[] EXPECTED_VAR_BINARY = StringUtils.hexToBytes("20160601");
    static final byte[] EXPECTED_GEOMETRY =
            StringUtils.hexToBytes("0101000000000000000000F03F0000000000000040");
    static final byte[] EXPECTED_BLOB = new byte[]{1, 2, 3, 4};
    static final String DECIMAL_VALUE_1 = "12.32";
    static final String DECIMAL_VALUE_2 = "1234567890111.12345678";

    private WsStmtWriteTestSupport() {
    }

    static String dbName(Class<?> testClass) {
        return TestUtils.camelToSnake(testClass);
    }

    static Connection openWebSocketConnection(String stmt2BindMode, boolean asyncWrite) throws SQLException {
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort()
                    + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        if (stmt2BindMode != null) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_STMT_BIND_MODE, stmt2BindMode);
        }
        if (asyncWrite) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "stmt");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "100");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "5");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "50000");
        }
        return DriverManager.getConnection(url, properties);
    }

    static void assumeBindExecSupported(Connection connection, String reason) throws SQLException {
        Assume.assumeTrue(reason + " requires WSConnection", connection instanceof WSConnection);
        Assume.assumeTrue(reason + " requires stmt2_bind_exec support",
                ((WSConnection) connection).supportsStmt2BindExec());
    }

    static void recreateDatabase(Connection connection, String dbName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
            statement.execute("create database " + dbName + " keep 36500");
            statement.execute("use " + dbName);
        }
    }

    static void dropDatabase(Connection connection, String dbName) throws SQLException {
        if (connection == null) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
        }
    }

    static void createAllTypeTable(Statement statement, String dbName, String tableName) throws SQLException {
        statement.execute("create table if not exists " + dbName + "." + tableName
                + "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, "
                + "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), "
                + "c11 varbinary(100), c12 geometry(100), c13 tinyint unsigned, c14 smallint unsigned, "
                + "c15 int unsigned, c16 bigint unsigned, c17 blob, c18 decimal(4,2), "
                + "c19 decimal(30,10))");
    }

    static void createAllTypeTable336(Statement statement, String dbName, String tableName) throws SQLException {
        statement.execute("create table if not exists " + dbName + "." + tableName
                + "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, "
                + "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), "
                + "c11 varbinary(100), c12 geometry(100), c13 tinyint unsigned, c14 smallint unsigned, "
                + "c15 int unsigned, c16 bigint unsigned)");
    }

    static void createAllTypeStable(Statement statement, String dbName, String stableName) throws SQLException {
        statement.execute("create stable if not exists " + dbName + "." + stableName
                + "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, "
                + "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), "
                + "c11 varbinary(100), c12 geometry(100), c13 tinyint unsigned, c14 smallint unsigned, "
                + "c15 int unsigned, c16 bigint unsigned, c17 blob, c18 decimal(4,2), "
                + "c19 decimal(30,10)) tags (groupId int)");
    }

    static void createAllTypeStable336(Statement statement, String dbName, String stableName) throws SQLException {
        statement.execute("create stable if not exists " + dbName + "." + stableName
                + "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, "
                + "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), "
                + "c11 varbinary(100), c12 geometry(100), c13 tinyint unsigned, c14 smallint unsigned, "
                + "c15 int unsigned, c16 bigint unsigned) tags (groupId int)");
    }

    static String tableInsertSql(String dbName, String tableName) {
        return "insert into " + dbName + "." + tableName
                + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    static String tableInsertSql336(String dbName, String tableName) {
        return "insert into " + dbName + "." + tableName
                + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    static String stableInsertSql(String dbName, String stableName) {
        return "insert into " + dbName + "." + stableName
                + "(tbname, ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, "
                + "c13, c14, c15, c16, c17, c18, c19) "
                + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    static String stableInsertSql336(String dbName, String stableName) {
        return "insert into " + dbName + "." + stableName
                + "(tbname, ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, "
                + "c13, c14, c15, c16) "
                + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    static String asyncInsertSql(String dbName, String stableName) {
        return "ASYNC_INSERT INTO ? USING " + dbName + "." + stableName
                + " TAGS(?) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    static String asyncInsertSql336(String dbName, String stableName) {
        return "ASYNC_INSERT INTO ? USING " + dbName + "." + stableName
                + " TAGS(?) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    static void bindAllTypes(PreparedStatement statement, int startIndex, long current) throws SQLException {
        statement.setTimestamp(startIndex, new Timestamp(current));
        statement.setByte(startIndex + 1, (byte) 2);
        statement.setShort(startIndex + 2, (short) 3);
        statement.setInt(startIndex + 3, 4);
        statement.setLong(startIndex + 4, 5L);
        statement.setFloat(startIndex + 5, 6.6f);
        statement.setDouble(startIndex + 6, 7.7);
        statement.setBoolean(startIndex + 7, true);
        statement.setString(startIndex + 8, "hello");
        statement.setNString(startIndex + 9, "世界");
        statement.setString(startIndex + 10, "hello world");
        statement.setBytes(startIndex + 11, EXPECTED_VAR_BINARY);
        statement.setBytes(startIndex + 12, EXPECTED_GEOMETRY);
        statement.setShort(startIndex + 13, TSDBConstants.MAX_UNSIGNED_BYTE);
        statement.setInt(startIndex + 14, TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setLong(startIndex + 15, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(startIndex + 16, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
        statement.setBlob(startIndex + 17, new TDBlob(EXPECTED_BLOB, true));
        statement.setBigDecimal(startIndex + 18, new BigDecimal(DECIMAL_VALUE_1));
        statement.setBigDecimal(startIndex + 19, new BigDecimal(DECIMAL_VALUE_2));
    }

    static void bindAllTypes336(PreparedStatement statement, int startIndex, long current) throws SQLException {
        statement.setTimestamp(startIndex, new Timestamp(current));
        statement.setByte(startIndex + 1, (byte) 2);
        statement.setShort(startIndex + 2, (short) 3);
        statement.setInt(startIndex + 3, 4);
        statement.setLong(startIndex + 4, 5L);
        statement.setFloat(startIndex + 5, 6.6f);
        statement.setDouble(startIndex + 6, 7.7);
        statement.setBoolean(startIndex + 7, true);
        statement.setString(startIndex + 8, "hello");
        statement.setNString(startIndex + 9, "世界");
        statement.setString(startIndex + 10, "hello world");
        statement.setBytes(startIndex + 11, EXPECTED_VAR_BINARY);
        statement.setBytes(startIndex + 12, EXPECTED_GEOMETRY);
        statement.setShort(startIndex + 13, TSDBConstants.MAX_UNSIGNED_BYTE);
        statement.setInt(startIndex + 14, TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setLong(startIndex + 15, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(startIndex + 16, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
    }

    static void bindNullTypes(PreparedStatement statement, int startIndex, long current) throws SQLException {
        statement.setTimestamp(startIndex, new Timestamp(current));
        statement.setNull(startIndex + 1, Types.TINYINT);
        statement.setNull(startIndex + 2, Types.SMALLINT);
        statement.setNull(startIndex + 3, Types.INTEGER);
        statement.setNull(startIndex + 4, Types.BIGINT);
        statement.setNull(startIndex + 5, Types.FLOAT);
        statement.setNull(startIndex + 6, Types.DOUBLE);
        statement.setNull(startIndex + 7, Types.BOOLEAN);
        statement.setString(startIndex + 8, null);
        statement.setNString(startIndex + 9, null);
        statement.setString(startIndex + 10, null);
        statement.setNull(startIndex + 11, Types.VARBINARY);
        statement.setNull(startIndex + 12, Types.VARBINARY);
        statement.setNull(startIndex + 13, Types.SMALLINT);
        statement.setNull(startIndex + 14, Types.INTEGER);
        statement.setNull(startIndex + 15, Types.BIGINT);
        statement.setNull(startIndex + 16, Types.BIGINT);
        statement.setNull(startIndex + 17, Types.BLOB);
        statement.setNull(startIndex + 18, Types.DECIMAL);
        statement.setNull(startIndex + 19, Types.DECIMAL);
    }

    static void bindNullTypes336(PreparedStatement statement, int startIndex, long current) throws SQLException {
        statement.setTimestamp(startIndex, new Timestamp(current));
        statement.setNull(startIndex + 1, Types.TINYINT);
        statement.setNull(startIndex + 2, Types.SMALLINT);
        statement.setNull(startIndex + 3, Types.INTEGER);
        statement.setNull(startIndex + 4, Types.BIGINT);
        statement.setNull(startIndex + 5, Types.FLOAT);
        statement.setNull(startIndex + 6, Types.DOUBLE);
        statement.setNull(startIndex + 7, Types.BOOLEAN);
        statement.setString(startIndex + 8, null);
        statement.setNString(startIndex + 9, null);
        statement.setString(startIndex + 10, null);
        statement.setNull(startIndex + 11, Types.VARBINARY);
        statement.setNull(startIndex + 12, Types.VARBINARY);
        statement.setNull(startIndex + 13, Types.SMALLINT);
        statement.setNull(startIndex + 14, Types.INTEGER);
        statement.setNull(startIndex + 15, Types.BIGINT);
        statement.setNull(startIndex + 16, Types.BIGINT);
    }

    static void assertAllTypeRow(ResultSet resultSet, long current) throws SQLException {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
        Assert.assertEquals((byte) 2, resultSet.getByte(2));
        Assert.assertEquals((short) 3, resultSet.getShort(3));
        Assert.assertEquals(4, resultSet.getInt(4));
        Assert.assertEquals(5L, resultSet.getLong(5));
        Assert.assertEquals(6.6f, resultSet.getFloat(6), 0.0001);
        Assert.assertEquals(7.7, resultSet.getDouble(7), 0.0001);
        Assert.assertTrue(resultSet.getBoolean(8));
        Assert.assertEquals("hello", resultSet.getString(9));
        Assert.assertEquals("世界", resultSet.getString(10));
        Assert.assertEquals("hello world", resultSet.getString(11));
        Assert.assertArrayEquals(EXPECTED_VAR_BINARY, resultSet.getBytes(12));
        Assert.assertArrayEquals(EXPECTED_GEOMETRY, resultSet.getBytes(13));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, resultSet.getShort(14));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, resultSet.getInt(15));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, resultSet.getLong(16));
        Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), resultSet.getObject(17));
        Assert.assertArrayEquals(EXPECTED_BLOB, resultSet.getBlob(18).getBytes(1, EXPECTED_BLOB.length));
        Assert.assertEquals(0, resultSet.getBigDecimal(19).compareTo(new BigDecimal(DECIMAL_VALUE_1)));
        Assert.assertEquals(0, resultSet.getBigDecimal(20).compareTo(new BigDecimal(DECIMAL_VALUE_2)));
        Assert.assertFalse(resultSet.next());
    }

    static void assertAllTypeRow336(ResultSet resultSet, long current) throws SQLException {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
        Assert.assertEquals((byte) 2, resultSet.getByte(2));
        Assert.assertEquals((short) 3, resultSet.getShort(3));
        Assert.assertEquals(4, resultSet.getInt(4));
        Assert.assertEquals(5L, resultSet.getLong(5));
        Assert.assertEquals(6.6f, resultSet.getFloat(6), 0.0001);
        Assert.assertEquals(7.7, resultSet.getDouble(7), 0.0001);
        Assert.assertTrue(resultSet.getBoolean(8));
        Assert.assertEquals("hello", resultSet.getString(9));
        Assert.assertEquals("世界", resultSet.getString(10));
        Assert.assertEquals("hello world", resultSet.getString(11));
        Assert.assertArrayEquals(EXPECTED_VAR_BINARY, resultSet.getBytes(12));
        Assert.assertArrayEquals(EXPECTED_GEOMETRY, resultSet.getBytes(13));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, resultSet.getShort(14));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, resultSet.getInt(15));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, resultSet.getLong(16));
        Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), resultSet.getObject(17));
        Assert.assertFalse(resultSet.next());
    }

    static void assertNullTypeRow(ResultSet resultSet, long current) throws SQLException {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
        for (int i = 2; i <= 8; i++) {
            resultSet.getObject(i);
            Assert.assertTrue("column " + i + " should be null", resultSet.wasNull());
        }
        Assert.assertNull(resultSet.getString(9));
        Assert.assertNull(resultSet.getString(10));
        Assert.assertNull(resultSet.getString(11));
        Assert.assertNull(resultSet.getBytes(12));
        Assert.assertNull(resultSet.getBytes(13));
        for (int i = 14; i <= 17; i++) {
            resultSet.getObject(i);
            Assert.assertTrue("column " + i + " should be null", resultSet.wasNull());
        }
        Assert.assertNull(resultSet.getBlob(18));
        Assert.assertNull(resultSet.getBigDecimal(19));
        Assert.assertNull(resultSet.getBigDecimal(20));
        Assert.assertFalse(resultSet.next());
    }

    static void assertNullTypeRow336(ResultSet resultSet, long current) throws SQLException {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
        for (int i = 2; i <= 8; i++) {
            resultSet.getObject(i);
            Assert.assertTrue("column " + i + " should be null", resultSet.wasNull());
        }
        Assert.assertNull(resultSet.getString(9));
        Assert.assertNull(resultSet.getString(10));
        Assert.assertNull(resultSet.getString(11));
        Assert.assertNull(resultSet.getBytes(12));
        Assert.assertNull(resultSet.getBytes(13));
        for (int i = 14; i <= 17; i++) {
            resultSet.getObject(i);
            Assert.assertTrue("column " + i + " should be null", resultSet.wasNull());
        }
        Assert.assertFalse(resultSet.next());
    }
}
