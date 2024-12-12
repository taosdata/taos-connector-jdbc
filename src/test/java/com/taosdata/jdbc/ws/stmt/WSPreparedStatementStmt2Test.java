package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBPreparedStatement;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import org.junit.*;

import java.sql.*;
import java.util.*;
import java.util.Random;

@FixMethodOrder
public class WSPreparedStatementStmt2Test {
    String host = "127.0.0.1";
    String db_name = "ws_prepare";
    String tableName = "wpt";
    String superTable = "wpt_st";
    Connection connection;

    int numOfSubTable = 10;
    int numOfRow = 10;
    private static final Random random = new Random(System.currentTimeMillis());

    @Test
    public void testStmt2Insert() throws SQLException {
        String sql = "INSERT INTO ? USING " + db_name + "." + tableName + " TAGS(?,?) VALUES (?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                //set table name
                pstmt.setTableName("d_bind_" + i);

                // set tags
                pstmt.setTagInt(0, i);
                pstmt.setTagString(1, "location_" + i);

                // set column ts
                ArrayList<Long> tsList = new ArrayList<>();
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++)
                    tsList.add(current + j);
                pstmt.setTimestamp(0, tsList);

                // set column current
                ArrayList<Float> currentList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    currentList.add(random.nextFloat() * 30);
                pstmt.setFloat(1, currentList);

                // set column voltage
                ArrayList<Integer> voltageList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    voltageList.add(random.nextInt(300));
                pstmt.setInt(2, voltageList);

                // set column phase
                ArrayList<Float> phaseList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    phaseList.add(random.nextFloat());
                pstmt.setFloat(3, phaseList);
                // add column
                pstmt.columnDataAddBatch();
            }
            // execute column
            pstmt.columnDataExecuteBatch();
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
    }

    @Test
    public void testStmt2InsertExtend() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableName + " (tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                //set table name
                pstmt.setTableName("d_bind_" + i);

                // set column ts
                ArrayList<Long> tsList = new ArrayList<>();
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++)
                    tsList.add(current + j);
                pstmt.setTimestamp(0, tsList);

                // set column current
                ArrayList<Float> currentList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    currentList.add(random.nextFloat() * 30);
                pstmt.setFloat(1, currentList);

                // set column voltage
                ArrayList<Integer> voltageList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    voltageList.add(random.nextInt(300));
                pstmt.setInt(2, voltageList);

                // set column phase
                ArrayList<Float> phaseList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    phaseList.add(random.nextFloat());
                pstmt.setFloat(3, phaseList);
                // add column
                pstmt.columnDataAddBatch();
            }
            // execute column
            pstmt.columnDataExecuteBatch();
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
    }

    @Test
    public void testStmt2InsertMixedApi() throws SQLException {
        String sql = "INSERT INTO ? USING " + db_name + "." + tableName + " TAGS(?,?) VALUES (?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                //set table name
                pstmt.setTableName("d_bind_" + i);

                // set tags
                pstmt.setTagInt(0, i);
                pstmt.setTagString(1, "location_" + i);

                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setTimestamp(1, new Timestamp(current + j));
                    pstmt.setFloat(2, random.nextFloat() * 30);
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setFloat(4, random.nextFloat());
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
    }

    @Test
    public void testStmt2InsertSubTable() throws SQLException {
        String sql = "INSERT INTO stb1 USING " + db_name + "." + tableName + " TAGS(?,?) VALUES (?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= 1; i++) {
                // set tags
                pstmt.setTagInt(0, 1);
                pstmt.setTagString(1, "location_" + 1);

                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setTimestamp(1, new Timestamp(current + j));
                    pstmt.setFloat(2, random.nextFloat() * 30);
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setFloat(4, random.nextFloat());
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
    }

    @Test
    public void testStmt2InsertStdApi() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableName + "(tbname, groupId, location, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setString(1, "d_bind_" + i);
                    pstmt.setInt(2, i);
                    pstmt.setString(3, "location_" + i);

                    pstmt.setTimestamp(4, new Timestamp(current + j));
                    pstmt.setFloat(5, random.nextFloat() * 30);
                    pstmt.setInt(6, random.nextInt(300));
                    pstmt.setFloat(7, random.nextFloat());
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
    }

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

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setString(1, "d_bind_" + i);

                    pstmt.setTimestamp(2, new Timestamp(current + j));
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
