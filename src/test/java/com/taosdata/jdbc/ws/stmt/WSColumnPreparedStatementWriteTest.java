package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TaosPrepareStatement;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.*;
import com.taosdata.jdbc.ws.WSColumnPreparedStatement;
import com.taosdata.jdbc.ws.WSConnection;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class WSColumnPreparedStatementWriteTest {
    private final String dbName = TestUtils.camelToSnake(WSColumnPreparedStatementWriteTest.class);
    private final String tableName = "wpt";
    private final String stableName = "swpt_ext";
    private final String blobTableName = "wpt_blob";
    private final String tableName2 = "unsigned_stable";
    private Connection connection;

    private static final String TEST_STR = "20160601";
    private static final byte[] EXPECTED_VAR_BINARY = StringUtils.hexToBytes(TEST_STR);
    private static final byte[] EXPECTED_GEOMETRY =
            StringUtils.hexToBytes("0101000000000000000000F03F0000000000000040");
    private static final byte[] EXPECTED_BLOB = new byte[]{1, 2, 3, 4};
    private static final String DECIMAL_VALUE_1 = "12.32";
    private static final String DECIMAL_VALUE_2 = "1234567890111.12345678";

    @Test
    public void testExecuteUpdate_columnRoute_roundTripsAllTypes() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName
                + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = prepareColumnStatement(sql)) {
            ParameterMetaData parameterMetaData = statement.getParameterMetaData();
            Assert.assertEquals(20, parameterMetaData.getParameterCount());
            Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData.isNullable(1));
            Assert.assertTrue(parameterMetaData.isSigned(2));
            Assert.assertEquals(0, parameterMetaData.getPrecision(2));
            Assert.assertEquals(0, parameterMetaData.getScale(2));
            Assert.assertEquals(Types.TINYINT, parameterMetaData.getParameterType(2));
            Assert.assertEquals("TINYINT", parameterMetaData.getParameterTypeName(2));
            Assert.assertEquals("java.lang.Byte", parameterMetaData.getParameterClassName(2));
            Assert.assertEquals(ParameterMetaData.parameterModeIn, parameterMetaData.getParameterMode(2));

            long current = System.currentTimeMillis();
            statement.setTimestamp(1, new Timestamp(current));
            statement.setByte(2, (byte) 2);
            statement.setShort(3, (short) 3);
            statement.setInt(4, 4);
            statement.setLong(5, 5L);
            statement.setFloat(6, 6.6f);
            statement.setDouble(7, 7.7);
            statement.setBoolean(8, true);
            statement.setString(9, "你好");
            statement.setNString(10, "世界");
            statement.setString(11, "hello world");
            statement.setBytes(12, EXPECTED_VAR_BINARY);
            statement.setBytes(13, EXPECTED_GEOMETRY);
            statement.setShort(14, TSDBConstants.MAX_UNSIGNED_BYTE);
            statement.setInt(15, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setLong(16, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.setBlob(18, new TDBlob(EXPECTED_BLOB, true));
            statement.setBigDecimal(19, new BigDecimal(DECIMAL_VALUE_1));
            statement.setBigDecimal(20, new BigDecimal(DECIMAL_VALUE_2));

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals((byte) 2, resultSet.getByte(2));
                Assert.assertEquals((short) 3, resultSet.getShort(3));
                Assert.assertEquals(4, resultSet.getInt(4));
                Assert.assertEquals(5L, resultSet.getLong(5));
                Assert.assertEquals(6.6f, resultSet.getFloat(6), 0.0001);
                Assert.assertEquals(7.7, resultSet.getDouble(7), 0.0001);
                Assert.assertTrue(resultSet.getBoolean(8));
                Assert.assertEquals("你好", resultSet.getString(9));
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
                Assert.assertEquals(new Date(current), resultSet.getDate(1));
                Assert.assertEquals(new Time(current), resultSet.getTime(1));
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals(7.7, resultSet.getBigDecimal(7).doubleValue(), 0.000001);
            }
        }
    }

    @Test
    public void testExecuteUpdate_columnRoute_roundTripsNulls() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName
                + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = prepareColumnStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setTimestamp(1, new Timestamp(current));
            statement.setNull(2, Types.TINYINT);
            statement.setNull(3, Types.SMALLINT);
            statement.setNull(4, Types.INTEGER);
            statement.setNull(5, Types.BIGINT);
            statement.setNull(6, Types.FLOAT);
            statement.setNull(7, Types.DOUBLE);
            statement.setNull(8, Types.BOOLEAN);
            statement.setString(9, null);
            statement.setNString(10, null);
            statement.setString(11, null);
            statement.setNull(12, Types.VARBINARY);
            statement.setNull(13, Types.VARBINARY);
            statement.setNull(14, Types.SMALLINT);
            statement.setNull(15, Types.INTEGER);
            statement.setNull(16, Types.BIGINT);
            statement.setNull(17, Types.BIGINT);
            statement.setNull(18, Types.BLOB);
            statement.setNull(19, Types.DECIMAL);
            statement.setNull(20, Types.DECIMAL);

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));

                resultSet.getByte(2);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getShort(3);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getInt(4);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getLong(5);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getFloat(6);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getDouble(7);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getBoolean(8);
                Assert.assertTrue(resultSet.wasNull());

                Assert.assertNull(resultSet.getString(9));
                Assert.assertNull(resultSet.getString(10));
                Assert.assertNull(resultSet.getString(11));
                Assert.assertNull(resultSet.getBytes(12));
                Assert.assertNull(resultSet.getBytes(13));

                resultSet.getShort(14);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getInt(15);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getLong(16);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getObject(17);
                Assert.assertTrue(resultSet.wasNull());

                Assert.assertNull(resultSet.getBlob(18));
                Assert.assertNull(resultSet.getBigDecimal(19));
                Assert.assertNull(resultSet.getBigDecimal(20));
            }
        }
    }

    @Test
    public void testSetObject_columnRoute_roundTripsAllTypes() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName
                + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = prepareColumnStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setObject(1, new Timestamp(current));
            statement.setObject(2, (byte) 2);
            statement.setObject(3, (short) 3);
            statement.setObject(4, 4);
            statement.setObject(5, 5L);
            statement.setObject(6, 6.6f);
            statement.setObject(7, 7.7);
            statement.setObject(8, true);
            statement.setObject(9, "你好");
            statement.setObject(10, "世界");
            statement.setObject(11, "hello world");
            statement.setObject(12, EXPECTED_VAR_BINARY);
            statement.setObject(13, EXPECTED_GEOMETRY);
            statement.setObject(14, TSDBConstants.MAX_UNSIGNED_BYTE);
            statement.setObject(15, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setObject(16, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.setObject(18, new TDBlob(EXPECTED_BLOB, true));
            statement.setObject(19, new BigDecimal(DECIMAL_VALUE_1));
            statement.setObject(20, new BigDecimal(DECIMAL_VALUE_2));

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals((byte) 2, resultSet.getByte(2));
                Assert.assertEquals((short) 3, resultSet.getShort(3));
                Assert.assertEquals(4, resultSet.getInt(4));
                Assert.assertEquals(5L, resultSet.getLong(5));
                Assert.assertEquals(6.6f, resultSet.getFloat(6), 0.0001);
                Assert.assertEquals(7.7, resultSet.getDouble(7), 0.0001);
                Assert.assertTrue(resultSet.getBoolean(8));
                Assert.assertEquals("你好", resultSet.getString(9));
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
                Assert.assertEquals(new Date(current), resultSet.getDate(1));
                Assert.assertEquals(new Time(current), resultSet.getTime(1));
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals(7.7, resultSet.getBigDecimal(7).doubleValue(), 0.000001);
            }
        }
    }

    @Test
    public void testSetObjectWithSqlType_columnRoute_roundTripsAllTypes() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName
                + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = prepareColumnStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setObject(1, new Timestamp(current), Types.TIMESTAMP);
            statement.setObject(2, (byte) 2, Types.TINYINT);
            statement.setObject(3, (short) 3, Types.SMALLINT);
            statement.setObject(4, 4, Types.INTEGER);
            statement.setObject(5, 5L, Types.BIGINT);
            statement.setObject(6, 6.6f, Types.FLOAT);
            statement.setObject(7, 7.7, Types.DOUBLE);
            statement.setObject(8, true, Types.BOOLEAN);
            statement.setObject(9, "你好", Types.VARCHAR);
            statement.setObject(10, "世界", Types.NCHAR);
            statement.setObject(11, "hello world", Types.VARCHAR);
            statement.setObject(12, EXPECTED_VAR_BINARY, Types.VARBINARY);
            statement.setObject(13, EXPECTED_GEOMETRY, Types.VARBINARY);
            statement.setObject(14, TSDBConstants.MAX_UNSIGNED_BYTE, Types.SMALLINT);
            statement.setObject(15, TSDBConstants.MAX_UNSIGNED_SHORT, Types.INTEGER);
            statement.setObject(16, TSDBConstants.MAX_UNSIGNED_INT, Types.BIGINT);
            statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.setObject(18, new TDBlob(EXPECTED_BLOB, true), Types.BLOB);
            statement.setObject(19, new BigDecimal(DECIMAL_VALUE_1), Types.DECIMAL);
            statement.setObject(20, new BigDecimal(DECIMAL_VALUE_2), Types.DECIMAL);

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals((byte) 2, resultSet.getByte(2));
                Assert.assertEquals((short) 3, resultSet.getShort(3));
                Assert.assertEquals(4, resultSet.getInt(4));
                Assert.assertEquals(5L, resultSet.getLong(5));
                Assert.assertEquals(6.6f, resultSet.getFloat(6), 0.0001);
                Assert.assertEquals(7.7, resultSet.getDouble(7), 0.0001);
                Assert.assertTrue(resultSet.getBoolean(8));
                Assert.assertEquals("你好", resultSet.getString(9));
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
                Assert.assertEquals(new Date(current), resultSet.getDate(1));
                Assert.assertEquals(new Time(current), resultSet.getTime(1));
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals(7.7, resultSet.getBigDecimal(7).doubleValue(), 0.000001);
            }
        }
    }

    @Test
    public void testColumnDataExtension_columnRoute_roundTripsAllTypes() throws SQLException {
        String sql = "insert into ? using " + dbName + "." + stableName
                + " tags (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (TaosPrepareStatement statement = (TaosPrepareStatement) prepareColumnStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setTableName("ext_all_types_1");
            statement.setTagTimestamp(0, new Timestamp(current));
            statement.setTagByte(1, (byte) 2);
            statement.setTagShort(2, (short) 3);
            statement.setTagInt(3, 4);
            statement.setTagLong(4, 5L);
            statement.setTagFloat(5, 6.6f);
            statement.setTagDouble(6, 7.7);
            statement.setTagBoolean(7, true);
            statement.setTagString(8, "hello");
            statement.setTagNString(9, "世界");
            statement.setTagString(10, "hello world");
            statement.setTagVarbinary(11, EXPECTED_VAR_BINARY);
            statement.setTagGeometry(12, EXPECTED_GEOMETRY);
            statement.setTagShort(13, TSDBConstants.MAX_UNSIGNED_BYTE);
            statement.setTagInt(14, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setTagLong(15, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setTagBigInteger(16, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

            statement.setTimestamp(0, Collections.singletonList(current));
            statement.setByte(1, Collections.singletonList((byte) 2));
            statement.setShort(2, Collections.singletonList((short) 3));
            statement.setInt(3, Collections.singletonList(4));
            statement.setLong(4, Collections.singletonList(5L));
            statement.setFloat(5, Collections.singletonList(6.6f));
            statement.setDouble(6, Collections.singletonList(7.7));
            statement.setBoolean(7, Collections.singletonList(true));
            statement.setString(8, Collections.singletonList("hello"), 10);
            statement.setNString(9, Collections.singletonList("世界"), 10);
            statement.setString(10, Collections.singletonList("hello world"), 20);
            statement.setVarbinary(11, Collections.singletonList(EXPECTED_VAR_BINARY), 100);
            statement.setGeometry(12, Collections.singletonList(EXPECTED_GEOMETRY), 100);
            statement.setShort(13, Collections.singletonList((short) TSDBConstants.MAX_UNSIGNED_BYTE));
            statement.setInt(14, Collections.singletonList(TSDBConstants.MAX_UNSIGNED_SHORT));
            statement.setLong(15, Collections.singletonList(TSDBConstants.MAX_UNSIGNED_INT));
            statement.setBigInteger(16, Collections.singletonList(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG)));
            statement.setBlob(17, Collections.<Blob>singletonList(new TDBlob(EXPECTED_BLOB, true)), 100);
            statement.setBigDecimal(18, Collections.singletonList(new BigDecimal(DECIMAL_VALUE_1)));
            statement.setBigDecimal(19, Collections.singletonList(new BigDecimal(DECIMAL_VALUE_2)));

            statement.columnDataAddBatch();
            statement.columnDataExecuteBatch();

            try (ResultSet resultSet = statement.executeQuery(
                    "select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, "
                            + "c13, c14, c15, c16, c17, c18, c19 from " + dbName + "." + stableName)) {
                WsStmtWriteTestSupport.assertAllTypeRow(resultSet, current);
            }
            try (ResultSet resultSet = statement.executeQuery(
                    "select t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, "
                            + "t14, t15, t16, t17 from " + dbName + "." + stableName)) {
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
        }
    }

    @Test
    public void testSetObject_columnRoute_coercesBooleanAcrossNumericTypes() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName + " (ts, c1, c2, c3, c4, c5, c6) values(?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = prepareColumnStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setObject(1, new Timestamp(current), Types.TIMESTAMP);
            statement.setObject(2, Boolean.TRUE, Types.TINYINT);
            statement.setObject(3, Boolean.TRUE, Types.SMALLINT);
            statement.setObject(4, Boolean.TRUE, Types.INTEGER);
            statement.setObject(5, Boolean.TRUE, Types.BIGINT);
            statement.setObject(6, Boolean.TRUE, Types.FLOAT);
            statement.setObject(7, Boolean.TRUE, Types.DOUBLE);

            statement.executeUpdate();

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
            }
        }
    }

    @Test
    public void testSetObject_columnRoute_normalizesTemporalInputs() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName + " (ts) values(?)";
        try (PreparedStatement statement = prepareColumnStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setObject(1, new Date(current), Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, new Time(current + 1), Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, Instant.ofEpochMilli(current + 2), Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, DateTimeUtils.getLocalDateTime(Instant.ofEpochMilli(current + 3), null), Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, DateTimeUtils.getOffsetDateTime(Instant.ofEpochMilli(current + 4), null), Types.TIMESTAMP);
            statement.executeUpdate();
            statement.setObject(1, DateTimeUtils.getZonedDateTime(Instant.ofEpochMilli(current + 5), null), Types.TIMESTAMP);
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

    @Test
    public void testExecuteUpdate_columnRoute_roundTripsBlob() throws SQLException {
        String sql = "insert into " + dbName + "." + blobTableName + " values(?, ?)";
        try (PreparedStatement statement = prepareColumnStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setTimestamp(1, new Timestamp(current));
            statement.setBlob(2, new TDBlob(EXPECTED_BLOB, true));

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + blobTableName)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertArrayEquals(EXPECTED_BLOB, resultSet.getBlob(2).getBytes(1, EXPECTED_BLOB.length));
                Assert.assertFalse(resultSet.next());
            }
        }
    }

    @Test
    public void testExecuteCriticalValue_columnRoute() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName
                + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = prepareColumnStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setByte(2, (byte) 127);
            statement.setShort(3, (short) 32767);
            statement.setInt(4, Integer.MAX_VALUE);
            statement.setLong(5, Long.MAX_VALUE);
            statement.setFloat(6, Float.MAX_VALUE);
            statement.setDouble(7, Double.MAX_VALUE);
            statement.setBoolean(8, true);
            statement.setString(9, "ABC");
            statement.setNString(10, "涛思数据");
            statement.setString(11, "陶");
            statement.setBytes(12, EXPECTED_VAR_BINARY);
            statement.setBytes(13, EXPECTED_GEOMETRY);
            statement.setShort(14, TSDBConstants.MAX_UNSIGNED_BYTE);
            statement.setInt(15, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setLong(16, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.setBlob(18, new TDBlob(EXPECTED_BLOB, true));
            statement.setBigDecimal(19, new BigDecimal(DECIMAL_VALUE_1));
            statement.setBigDecimal(20, new BigDecimal(DECIMAL_VALUE_2));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUtinyIntOutOfRange_columnRoute() throws SQLException {
        try (PreparedStatement statement = prepareColumnStatement(unsignedSql())) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) (TSDBConstants.MAX_UNSIGNED_BYTE + 1));
            statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUtinyIntOutOfRange2_columnRoute() throws SQLException {
        try (PreparedStatement statement = prepareColumnStatement(unsignedSql())) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) -1);
            statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUShortOutOfRange_columnRoute() throws SQLException {
        try (PreparedStatement statement = prepareColumnStatement(unsignedSql())) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT + 1);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUShortOutOfRange2_columnRoute() throws SQLException {
        try (PreparedStatement statement = prepareColumnStatement(unsignedSql())) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, -1);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUIntOutOfRange_columnRoute() throws SQLException {
        try (PreparedStatement statement = prepareColumnStatement(unsignedSql())) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, 0);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT + 1);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUIntOutOfRange2_columnRoute() throws SQLException {
        try (PreparedStatement statement = prepareColumnStatement(unsignedSql())) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, 0);
            statement.setLong(4, -1L);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testULongOutOfRange_columnRoute() throws SQLException {
        try (PreparedStatement statement = prepareColumnStatement(unsignedSql())) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, 0);
            statement.setLong(4, 0);
            statement.setObject(5, new BigInteger("18446744073709551616"));
            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testULongOutOfRange2_columnRoute() throws SQLException {
        try (PreparedStatement statement = prepareColumnStatement(unsignedSql())) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, 0);
            statement.setLong(4, 0);
            statement.setObject(5, new BigInteger("-1"));
            statement.executeUpdate();
        }
    }

    private PreparedStatement prepareColumnStatement(String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        Assert.assertTrue("Expected WSColumnPreparedStatement, got " + statement.getClass().getName(),
                statement instanceof WSColumnPreparedStatement);
        return statement;
    }

    private String unsignedSql() {
        return "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort()
                    + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_STMT_BIND_MODE, "column");
        connection = DriverManager.getConnection(url, properties);
        Assert.assertTrue("Column all-type test requires WSConnection", connection instanceof WSConnection);
        Assert.assertTrue("stmtBindMode=column should force WSColumnPreparedStatement route",
                ((WSConnection) connection).supportsStmt2BindExec());
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
            statement.execute("create database " + dbName + " keep 36500");
            statement.execute("use " + dbName);
            statement.execute("create table if not exists " + dbName + "." + tableName
                    + "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, "
                    + "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), "
                    + "c11 varbinary(100), c12 geometry(100), c13 tinyint unsigned, c14 smallint unsigned, "
                    + "c15 int unsigned, c16 bigint unsigned, c17 blob, c18 decimal(4,2), "
                    + "c19 decimal(30,10))");
            statement.execute("create stable if not exists " + dbName + "." + stableName
                    + "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, "
                    + "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), "
                    + "c11 varbinary(100), c12 geometry(100), c13 tinyint unsigned, c14 smallint unsigned, "
                    + "c15 int unsigned, c16 bigint unsigned, c17 blob, c18 decimal(4,2), "
                    + "c19 decimal(30,10)) tags (t1 timestamp, t2 tinyint, t3 smallint, t4 int, t5 bigint, "
                    + "t6 float, t7 double, t8 bool, t9 binary(10), t10 nchar(10), t11 varchar(20), "
                    + "t12 varbinary(100), t13 geometry(100), t14 tinyint unsigned, "
                    + "t15 smallint unsigned, t16 int unsigned, t17 bigint unsigned)");
            statement.execute("create table if not exists " + dbName + "." + blobTableName
                    + "(ts timestamp, c1 blob)");
            statement.execute("create table if not exists " + dbName + "." + tableName2
                    + "(ts timestamp, c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned)");
        }
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
        }
        connection.close();
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
