package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.AbsWSPreparedStatement;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import com.taosdata.jdbc.ws.WSEWColumnPreparedStatement;
import com.taosdata.jdbc.ws.WSConnection;
import com.taosdata.jdbc.ws.loadbalance.RebalanceManager;
import com.taosdata.jdbc.ws.loadbalance.RebalanceTestUtil;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Properties;
import java.util.Random;

@FixMethodOrder
public class WsEfficientWritingTest {
    private final String host = "localhost";
    private final String db_name = TestUtils.camelToSnake(WsEfficientWritingTest.class);
    private final String tableName = "wpt";
    private final String tableNameCopyData = "wpt_cp";
    private final String tableReconnect = "wpt_rc";
    private final String tableNcharTag = "wpt_nchar";
    private final String tableAllType = "wpt_all_type";
    private final String asyncSqlTable = tableReconnect;
    private Connection connection;
    private TaosAdapterMock taosAdapterMock;
    private final int numOfSubTable = 100;
    private final int numOfRow = 100;
    private final int proxyPort = 8041;
    private final int proxyPort2 = 8042;
    private static final Random random = new Random(System.currentTimeMillis());
    private static final byte[] EXPECTED_VAR_BINARY = StringUtils.hexToBytes("20160601");
    private static final byte[] EXPECTED_GEOMETRY =
            StringUtils.hexToBytes("0101000000000000000000F03F0000000000000040");
    private static final byte[] EXPECTED_BLOB = new byte[]{1, 2, 3, 4};
    private static final String DECIMAL_VALUE_1 = "12.32";
    private static final String DECIMAL_VALUE_2 = "1234567890111.12345678";

    public void createSubTable() throws SQLException{
        // create sub table first
        String createSql = "create table";
        for (int i = 1; i <= numOfSubTable; i++) {
            createSql += " if not exists d_bind_" + i + " using " + db_name + "." + tableName + " tags(" + i + ", \"location_" + i + "\")";
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSql);
        } catch (Exception ex) {
            System.out.printf("Failed to create sub table, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    public void testStmt2InsertStdApiNoTag() throws SQLException {

        String sql = "INSERT INTO " + db_name + "." + tableName + "(tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)";

        long current = System.currentTimeMillis();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "d_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setFloat(3, random.nextFloat() * 30);
                    pstmt.setInt(4, random.nextInt(300));
                    pstmt.setFloat(5, random.nextFloat());
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection, db_name + "." + tableName));
    }

    @Test
    public void testCopyData() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableNameCopyData + "(tbname, ts, b, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        Timestamp ts = new Timestamp(current);
        byte[] bytes = new byte[10];

        try (Connection con= getConnection(true);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "cpd_bind_" + i);
                    pstmt.setTimestamp(2, ts);
                    pstmt.setBytes(3, bytes);
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }

                ts.setTime(ts.getTime() + 1000); // 增加 1 s
                bytes[0] = (byte)(bytes[0] + 1);

                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection, db_name + "." + tableNameCopyData));
        Assert.assertEquals(numOfSubTable, Utils.getSqlRows(connection, db_name + "." + tableNameCopyData + " where b = '\\x63000000000000000000'"));
    }

    @Test
    public void testReconnect() throws SQLException, InterruptedException, IOException {
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
        taosAdapterMock = new TaosAdapterMock(proxyPort2);
        taosAdapterMock.start();
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(host, proxyPort2, false));

        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnection(false, false, proxyPort2);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "rc_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }

                if (j == 1){
                    taosAdapterMock.stop();
                    Thread.sleep(1000);
                    taosAdapterMock = new TaosAdapterMock(proxyPort2);
                    taosAdapterMock.start();
                    RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(host, proxyPort2, false));
                }

                int affectedRows = pstmt.executeUpdate();
                Assert.assertEquals(numOfSubTable, affectedRows);
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection, db_name + "." + tableReconnect));

        if (taosAdapterMock != null) {
            taosAdapterMock.stop();
        }
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
    }

    @Test
    public void testWriteException() throws SQLException, InterruptedException, IOException {
        taosAdapterMock = new TaosAdapterMock(proxyPort);
        taosAdapterMock.start();

        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnection(false, false, proxyPort);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            WSConnection wscon = (WSConnection) con;
            wscon.getParam().setReconnectIntervalMs(10);

            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "rc_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }

                if (j == 0){
                    taosAdapterMock.stop();
                    Thread.sleep(1000);
                }

                try {
                    pstmt.executeUpdate();
                    Assert.fail("Should throw exception");
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage().startsWith("InsertedRows: "));
                    break;
                }
            }
        }
    }

    @Test
    @Ignore
    public void manualTest() throws SQLException, InterruptedException {

        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnection(false, false, 6041);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "rc_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }

                int affectedRows = pstmt.executeUpdate();
                Assert.assertEquals(numOfSubTable, affectedRows);
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection,db_name + "." + tableReconnect));
    }

    @Test
    public void testAsyncSql() throws SQLException {
        String sql = "ASYNC_INSERT INTO " + db_name + "." + asyncSqlTable + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnectionNormal();
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "rc_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(Statement.SUCCESS_NO_INFO, ele);
                }

                int affectedRows = pstmt.executeUpdate();
                Assert.assertEquals(affectedRows, numOfSubTable);
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection, db_name + "." + asyncSqlTable));
    }

    @Test
    public void testColumnModeAutoCreateSubtableRoutesToWSEWColumnPreparedStatement() throws SQLException {
        assumeBindExecSupported();
        String subTable = "column_auto_bind_1";
        String sql = "ASYNC_INSERT INTO ? USING " + db_name + "." + tableName + " TAGS(?,?) VALUES (?,?,?,?)";

        try (Connection con = getColumnModeConnection();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            assertWSEWColumnPreparedStatement(pstmt);

            long current = System.currentTimeMillis();
            for (int i = 0; i < 3; i++) {
                pstmt.setString(1, subTable);
                pstmt.setInt(2, 901);
                pstmt.setString(3, "column_auto_location");
                pstmt.setTimestamp(4, new Timestamp(current + i));
                pstmt.setFloat(5, 10.5f + i);
                pstmt.setInt(6, 220 + i);
                pstmt.setFloat(7, 0.1f + i);
                pstmt.addBatch();
            }

            int[] exeResult = pstmt.executeBatch();
            Assert.assertEquals(3, exeResult.length);
            for (int ele : exeResult) {
                Assert.assertEquals(Statement.SUCCESS_NO_INFO, ele);
            }
            Assert.assertEquals(3, pstmt.executeUpdate());
        }

        Assert.assertEquals(3, Utils.getSqlRows(connection, db_name + ".`" + subTable + "`"));
    }

    @Test
    public void testColumnModePrecreatedSubtableRoundTripsTypeMatrix() throws SQLException {
        assumeBindExecSupported();
        String subTable = "column_all_type_1";
        try (Statement statement = connection.createStatement()) {
            statement.execute("create stable if not exists " + db_name + "." + tableAllType
                    + "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, "
                    + "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), "
                    + "c11 varbinary(100), c12 geometry(100), c13 tinyint unsigned, c14 smallint unsigned, "
                    + "c15 int unsigned, c16 bigint unsigned, c17 blob, c18 decimal(4,2), "
                    + "c19 decimal(30,10)) "
                    + "TAGS (groupId INT)");
            statement.execute("create table if not exists " + subTable
                    + " using " + db_name + "." + tableAllType + " tags(1)");
        }

        String sql = "INSERT INTO " + db_name + "." + tableAllType
                + "(tbname, ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, "
                + "c13, c14, c15, c16, c17, c18, c19) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getColumnModeConnection();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            assertWSEWColumnPreparedStatement(pstmt);

            pstmt.setString(1, subTable);
            pstmt.setTimestamp(2, new Timestamp(current));
            pstmt.setByte(3, (byte) 2);
            pstmt.setShort(4, (short) 3);
            pstmt.setInt(5, 4);
            pstmt.setLong(6, 5L);
            pstmt.setFloat(7, 6.6f);
            pstmt.setDouble(8, 7.7);
            pstmt.setBoolean(9, true);
            pstmt.setString(10, "hello");
            pstmt.setNString(11, "世界");
            pstmt.setString(12, "hello world");
            pstmt.setBytes(13, EXPECTED_VAR_BINARY);
            pstmt.setBytes(14, EXPECTED_GEOMETRY);
            pstmt.setShort(15, TSDBConstants.MAX_UNSIGNED_BYTE);
            pstmt.setInt(16, TSDBConstants.MAX_UNSIGNED_SHORT);
            pstmt.setLong(17, TSDBConstants.MAX_UNSIGNED_INT);
            pstmt.setObject(18, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            pstmt.setBlob(19, new TDBlob(EXPECTED_BLOB, true));
            pstmt.setBigDecimal(20, new BigDecimal(DECIMAL_VALUE_1));
            pstmt.setBigDecimal(21, new BigDecimal(DECIMAL_VALUE_2));
            pstmt.addBatch();

            int[] exeResult = pstmt.executeBatch();
            Assert.assertEquals(1, exeResult.length);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, exeResult[0]);
            Assert.assertEquals(1, pstmt.executeUpdate());
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from " + db_name + ".`" + subTable + "`")) {
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
    }

    @Test(expected = SQLException.class)
    public void testStrictCheck() throws SQLException, InterruptedException {
        String sql = "INSERT INTO " + db_name + "." + tableNameCopyData + "(tbname, ts, b, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        Timestamp ts = new Timestamp(current);
        byte[] bytes = new byte[100];

        try (Connection con= getConnection(true, true);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "cpd_bind_" + i);
                    pstmt.setTimestamp(2, ts);
                    pstmt.setBytes(3, bytes);
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }

                ts.setTime(ts.getTime() + 1000); // 增加 1 s
                bytes[0] = (byte)(bytes[0] + 1);

                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection,db_name + "." + tableNameCopyData));
        Assert.assertEquals(numOfSubTable, Utils.getSqlRows(connection, db_name + "." + tableNameCopyData + " where b = '\\x63000000000000000000'"));

    }

    @Test(expected = SQLException.class)
    public void testGetMetaDataThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.getMetaData();
        }
    }

    @Test(expected = SQLException.class)
    public void testColumnDataAddBatchThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                AbsWSPreparedStatement pstmt = (AbsWSPreparedStatement) con.prepareStatement(sql)) {
            pstmt.columnDataAddBatch();
        }
    }

    @Test(expected = SQLException.class)
    public void testColumnDataExecuteBatchThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                AbsWSPreparedStatement pstmt = (AbsWSPreparedStatement) con.prepareStatement(sql)) {
            pstmt.columnDataExecuteBatch();
        }
    }

    @Test(expected = SQLException.class)
    public void testColumnDataCloseBatchThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                AbsWSPreparedStatement pstmt = (AbsWSPreparedStatement) con.prepareStatement(sql)) {
            pstmt.columnDataCloseBatch();
        }
    }
    @Test
    public void testNcharTag() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableNcharTag + "(tbname, ts, i, tag_nchar) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            long current = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                pstmt.setString(1, "ncahr_bind_1");
                pstmt.setTimestamp(2, new Timestamp(current + i));
                pstmt.setInt(3, 100);
                pstmt.setNString(4, "中国人");
                pstmt.addBatch();
                pstmt.executeBatch();
            }
            pstmt.executeUpdate();
        }
    }

    @Before
    public void before() throws SQLException {
        connection = getConnection(false);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create stable if not exists " + db_name + "." + tableName + " (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        statement.execute("create stable if not exists " + db_name + "." + tableNameCopyData + " (ts TIMESTAMP, b varbinary(10)) TAGS (groupId INT)");
        statement.execute("create stable if not exists " + db_name + "." + tableReconnect + " (ts TIMESTAMP, i INT) TAGS (groupId INT)");
        statement.execute("create stable if not exists " + db_name + "." + tableNcharTag + " (ts TIMESTAMP, i INT) TAGS (tag_nchar nchar(100))");

        statement.close();

        createSubTable();
    }

    private Connection getConnection(boolean copyData) throws SQLException {
        return getConnection(copyData, false, TestEnvUtil.getWsPort());
    }

    private Connection getConnection(boolean copyData, boolean strictCheck) throws SQLException {
        return getConnection(copyData, strictCheck, TestEnvUtil.getWsPort());
    }
    private Connection getConnectionNormal() throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        Properties properties = new Properties();
        return DriverManager.getConnection(url, properties);
    }

    private Connection getConnection(boolean copyData, boolean strictCheck, int port) throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "stmt");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "100");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "5");

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "50000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "1");

        if (copyData) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_COPY_DATA, "true");
        }

        if (strictCheck){
            properties.setProperty(TSDBDriver.PROPERTY_KEY_STRICT_CHECK, "true");
        }
        return DriverManager.getConnection(url, properties);
    }

    private Connection getColumnModeConnection() throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "stmt");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "100");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "5");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_STMT2_BIND_MODE, "column");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "50000");
        Connection con = DriverManager.getConnection(url, properties);
        boolean success = false;
        try (Statement statement = con.createStatement()) {
            statement.execute("use " + db_name);
            success = true;
            return con;
        } finally {
            if (!success) {
                con.close();
            }
        }
    }

    private void assumeBindExecSupported() throws SQLException {
        Assume.assumeTrue("WSEW column test requires WSConnection", connection instanceof WSConnection);
        Assume.assumeTrue("WSEW column test requires stmt2_bind_exec support",
                ((WSConnection) connection).supportsStmt2BindExec());
    }

    private void assertWSEWColumnPreparedStatement(PreparedStatement pstmt) {
        Assert.assertTrue("Expected WSEWColumnPreparedStatement, got " + pstmt.getClass().getName(),
                pstmt instanceof WSEWColumnPreparedStatement);
    }

    @After
    public void after() throws SQLException, InterruptedException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
    }

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
    }
}
