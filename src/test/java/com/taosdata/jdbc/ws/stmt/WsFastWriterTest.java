package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import com.taosdata.jdbc.ws.WSFWPreparedStatement;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

@FixMethodOrder
public class WsFastWriterTest {
    private String host = "localhost";
    private String db_name = "ws_fw";
    private String tableName = "wpt";
    private  String tableNameCopyData = "wpt_cp";
    private String tableReconnect = "wpt_rc";
    private String asyncSqlTable = "wpt_async";
    private Connection connection;
    private TaosAdapterMock taosAdapterMock;
    private final int numOfSubTable = 100;
    private final int numOfRow = 100;
    private final int proxyPort = 8041;
    private static final Random random = new Random(System.currentTimeMillis());

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

        Assert.assertEquals(numOfSubTable * numOfRow, getSqlRows("select count(*) from " + db_name + "." + tableName));
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

        Assert.assertEquals(numOfSubTable * numOfRow, getSqlRows("select count(*) from " + db_name + "." + tableNameCopyData));
        Assert.assertEquals(numOfSubTable, getSqlRows("select count(*) from " + db_name + "." + tableNameCopyData + " where b = '\\x63000000000000000000'"));
    }


    @Test
    public void testReconnect() throws SQLException, InterruptedException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnection(false, proxyPort);
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
                    taosAdapterMock.stopServer();
                    Thread.sleep(1000);
                    taosAdapterMock = new TaosAdapterMock(proxyPort);
                    taosAdapterMock.start();
                }

                int affectedRows = pstmt.executeUpdate();
                Assert.assertEquals(affectedRows, numOfSubTable);
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, getSqlRows("select count(*) from " + db_name + "." + tableReconnect));
    }



    @Test
    public void testAsyncSql() throws SQLException, InterruptedException {
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
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }

                int affectedRows = pstmt.executeUpdate();
                Assert.assertEquals(affectedRows, numOfSubTable);
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, getSqlRows("select count(*) from " + db_name + "." + asyncSqlTable));
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
             WSFWPreparedStatement pstmt = (WSFWPreparedStatement)con.prepareStatement(sql)) {
            pstmt.columnDataAddBatch();
        }
    }

    @Test(expected = SQLException.class)
    public void testColumnDataExecuteBatchThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
             WSFWPreparedStatement pstmt = (WSFWPreparedStatement)con.prepareStatement(sql)) {
            pstmt.columnDataExecuteBatch();
        }
    }

    @Test(expected = SQLException.class)
    public void testColumnDataCloseBatchThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
             WSFWPreparedStatement pstmt = (WSFWPreparedStatement)con.prepareStatement(sql)) {
            pstmt.columnDataCloseBatch();
        }
    }

    private int getSqlRows(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(sql);
        ResultSet rs = statement.getResultSet();
        rs.next();
        int count = rs.getInt(1);
        rs.close();
        statement.close();
        return count;
    }


    @Before
    public void before() throws SQLException {
        taosAdapterMock = new TaosAdapterMock(proxyPort);
        taosAdapterMock.start();

        connection = getConnection(false);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create stable if not exists " + db_name + "." + tableName + " (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        statement.execute("create stable if not exists " + db_name + "." + tableNameCopyData + " (ts TIMESTAMP, b varbinary(10)) TAGS (groupId INT)");
        statement.execute("create stable if not exists " + db_name + "." + tableReconnect + " (ts TIMESTAMP, i INT) TAGS (groupId INT)");
        statement.execute("create stable if not exists " + db_name + "." + asyncSqlTable + " (ts TIMESTAMP, i INT) TAGS (groupId INT)");
        statement.close();

        createSubTable();
    }

    private Connection getConnection(boolean copydata) throws SQLException {
        return getConnection(copydata, 6041);
    }

    private Connection getConnectionNormal() throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + 6041 + "/?user=root&password=taosdata";
        Properties properties = new Properties();
        return DriverManager.getConnection(url, properties);
    }

    private Connection getConnection(boolean copydata, int port) throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "stmt");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "100");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "1");

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        if (copydata) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_COPY_DATA, "true");
        }
        return DriverManager.getConnection(url, properties);
    }

    @After
    public void after() throws SQLException, InterruptedException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();

        if (taosAdapterMock != null) {
            taosAdapterMock.stopServer();
        }
    }
}
