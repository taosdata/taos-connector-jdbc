package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.math.BigInteger;
import java.sql.*;
import java.util.Collections;
import java.util.Properties;

public class WsPstmtAllTypeTest {
    String host = "127.0.0.1";
    String db_name = TestUtils.camelToSnake(WsPstmtAllTypeTest.class);
    String tableName = "wpt";
    String stableName = "swpt";

    String tableName2 = "unsigned_stable";
    Connection connection;
    static String testStr = "20160601";
    static byte[] expectedVarBinary = StringUtils.hexToBytes(testStr);
    static byte[] expectedGeometry = StringUtils.hexToBytes("0101000000000000000000F03F0000000000000040");

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
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
        statement.setBytes(12, expectedVarBinary);
        statement.setBytes(13, expectedGeometry);

        statement.setShort(14, TSDBConstants.MAX_UNSIGNED_BYTE);
        statement.setInt(15, TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setLong(16, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();

        ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName);
        resultSet.next();
        Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));
        Assert.assertEquals(resultSet.getByte(2), (byte) 2);
        Assert.assertEquals(resultSet.getShort(3), (short) 3);
        Assert.assertEquals(resultSet.getInt(4), 4);
        Assert.assertEquals(resultSet.getLong(5), 5L);
        Assert.assertEquals(resultSet.getFloat(6), 6.6f, 0.0001);
        Assert.assertEquals(resultSet.getDouble(7), 7.7, 0.0001);
        Assert.assertTrue(resultSet.getBoolean(8));
        Assert.assertEquals(resultSet.getString(9), "你好");
        Assert.assertEquals(resultSet.getString(10), "世界");
        Assert.assertEquals(resultSet.getString(11), "hello world");
        Assert.assertArrayEquals(resultSet.getBytes(12), expectedVarBinary);
        Assert.assertArrayEquals(resultSet.getBytes(13), expectedGeometry);

        Assert.assertEquals(resultSet.getShort(14), TSDBConstants.MAX_UNSIGNED_BYTE);
        Assert.assertEquals(resultSet.getInt(15), TSDBConstants.MAX_UNSIGNED_SHORT);
        Assert.assertEquals(resultSet.getLong(16), TSDBConstants.MAX_UNSIGNED_INT);
        Assert.assertEquals(resultSet.getObject(17), new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));


        Date date = new Date(current);
        Date date1 = resultSet.getDate(1);
        Assert.assertEquals(resultSet.getDate(1), new Date(current));
        Assert.assertEquals(resultSet.getTime(1), new Time(current));
        Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));
        Assert.assertEquals(resultSet.getBigDecimal(7).doubleValue(), 7.7, 0.000001);

        resultSet.close();
        statement.close();
    }


    @Test
    public void testExecuteUpdate2() throws SQLException {
        String sql = "insert into stb_1 using " + db_name + "." + stableName + " tags (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) values (?, ?)";
        TSWSPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class);
        long current = System.currentTimeMillis();
        statement.setTagTimestamp(0, new Timestamp(current));
        statement.setTagByte(1, (byte) 2);
        statement.setTagShort(2, (short) 3);
        statement.setTagInt(3, 4);
        statement.setTagLong(4, 5L);
        statement.setTagFloat(5, 6.6f);
        statement.setTagDouble(6, 7.7);
        statement.setTagBoolean(7, true);
        statement.setTagString(8, "你好");
        statement.setTagNString(9, "世界");
        statement.setTagString(10, "hello world");
        statement.setTagVarbinary(11, expectedVarBinary);
        statement.setTagGeometry(12, expectedGeometry);
        statement.setTagShort(13, TSDBConstants.MAX_UNSIGNED_BYTE);
        statement.setTagInt(14,  TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setTagLong(15,  TSDBConstants.MAX_UNSIGNED_INT);
        statement.setTagBigInteger(16, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));


        statement.setTimestamp(0, Collections.singletonList(current));
        statement.setByte(1, Collections.singletonList((byte) 2));
        statement.columnDataAddBatch();
        statement.columnDataExecuteBatch();

        ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + stableName);
        resultSet.next();
        Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));
        Assert.assertEquals(resultSet.getByte(2), (byte) 2);

        Assert.assertEquals(resultSet.getTimestamp(3), new Timestamp(current));
        Assert.assertEquals(resultSet.getByte(4), (byte) 2);
        Assert.assertEquals(resultSet.getShort(5), (short) 3);
        Assert.assertEquals(resultSet.getInt(6), 4);
        Assert.assertEquals(resultSet.getLong(7), 5L);
        Assert.assertEquals(resultSet.getFloat(8), 6.6f, 0.0001);
        Assert.assertEquals(resultSet.getDouble(9), 7.7, 0.0001);
        Assert.assertTrue(resultSet.getBoolean(10));
        Assert.assertEquals(resultSet.getString(11), "你好");
        Assert.assertEquals(resultSet.getString(12), "世界");
        Assert.assertEquals(resultSet.getString(13), "hello world");

        Assert.assertArrayEquals(resultSet.getBytes(14), expectedVarBinary);
        Assert.assertArrayEquals(resultSet.getBytes(15), expectedGeometry);

        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, resultSet.getShort(16));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, resultSet.getInt(17));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, resultSet.getLong(18));
        Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), resultSet.getObject(19));

        resultSet.close();
        statement.close();
    }

    @Test
    public void testExecuteCriticalValue() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setByte(2, (byte) 127);
        statement.setShort(3, (short) 32767);
        statement.setInt(4, 2147483647);
        statement.setLong(5, 9223372036854775807L);
        statement.setFloat(6, Float.MAX_VALUE);
        statement.setDouble(7, Double.MAX_VALUE);
        statement.setBoolean(8, true);
        statement.setString(9, "ABC");
        statement.setNString(10, "涛思数据");
        statement.setString(11, "陶");
        statement.setBytes(12, expectedVarBinary);
        statement.setBytes(13, expectedGeometry);
        statement.setShort(14, TSDBConstants.MAX_UNSIGNED_BYTE);
        statement.setInt(15, TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setLong(16, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUtinyIntOutOfRange() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short)(TSDBConstants.MAX_UNSIGNED_BYTE + 1));
        statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }
    @Test(expected = SQLException.class)
    public void testUtinyIntOutOfRange2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) -1);
        statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUShortOutOfRange() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT + 1);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUShortOutOfRange2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, -1);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUIntOutOfRange() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, 0);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT + 1);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUIntOutOfRange2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, 0);
        statement.setLong(4, -1L);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testULongOutOfRange() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, 0);
        statement.setLong(4, 0);
        statement.setObject(5, new BigInteger("18446744073709551616"));

        statement.executeUpdate();
        statement.close();
    }
    @Test(expected = SQLException.class)
    public void testULongOutOfRange2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, 0);
        statement.setLong(4, 0);
        statement.setObject(5, new BigInteger("-1"));

        statement.executeUpdate();
        statement.close();
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&batchfetch=true";
        } else {
            url += "?user=root&password=taosdata&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name + " keep 36500");
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName +
                "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
                "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), c11 varbinary(100), c12 geometry(100)," +
                "c13 tinyint unsigned, c14 smallint unsigned, c15 int unsigned, c16 bigint unsigned)");

        statement.execute("create stable if not exists " + db_name + "." + stableName +
                "(ts timestamp, c1 tinyint) tags (t1 timestamp, t2 tinyint, t3 smallint, t4 int, t5 bigint, " +
                "t6 float, t7 double, t8 bool, t9 binary(10), t10 nchar(10), t11 varchar(20), t12 varbinary(100), t13 geometry(100)," +
                "t14 tinyint unsigned, t15 smallint unsigned, t16 int unsigned, t17 bigint unsigned)");

        statement.execute("create table if not exists " + db_name + "." + tableName2 +
                "(ts timestamp, " +
                "c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned)");

        statement.close();
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()){
            statement.execute("drop database if exists " + db_name);
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
