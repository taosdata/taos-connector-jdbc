package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

@FixMethodOrder
public class WsFastWriterTest {
    String host = "vm98";
    String db_name = "ws_fw";
    String tableName = "wpt";
    String superTable = "wpt_st";
    Connection connection;

    int numOfSubTable = 1000;
    int numOfRow = 100;
    private static final Random random = new Random(System.currentTimeMillis());


    @Test
    public void testStmt2InsertStdApiNoTag() throws SQLException {
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

        String sql = "INSERT INTO " + db_name + "." + tableName + "(tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)";

        long start = System.currentTimeMillis();
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

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            // you can check exeResult here
            System.out.println("Successfully inserted " + (numOfSubTable * numOfRow) + " rows to power.meters.");
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to insert to table meters using stmt, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }

        long end = System.currentTimeMillis();
        System.out.println("Time cost: " + (end - start) + "ms");

        String sql2 = "select * from " + db_name + "." + tableName + " where ts > ? order by ts desc limit ?";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql2).unwrap(TSWSPreparedStatement.class)) {
            pstmt.setTimestamp(0, new Timestamp(System.currentTimeMillis() - 1000));
            pstmt.setInt(2, 5);
            ResultSet rs = pstmt.executeQuery();

            int rows = 0;
            while (rs.next()) {
                rows++;
                System.out.println("ts-1: " + rs.getTimestamp(1) + ", current: " + rs.getFloat(2) + ", voltage: " + rs.getInt(3) + ", phase: " + rs.getFloat(4));
            }
            Assert.assertEquals(6, rows);
        } catch (Exception ex) {
            System.out.printf("Failed to query table meters using stmt, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            ex.printStackTrace();
            throw ex;
        }

    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":6041/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata&batchfetch=true";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "stmt");

        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "500");
        //properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "1");

        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create stable if not exists " + db_name + "." + tableName + " (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        statement.close();
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
    }
}
