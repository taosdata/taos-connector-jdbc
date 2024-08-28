package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TSDBPreparedStatementPerfTest {

    private static final String host = "localhost";
    private static final String dbName = "meters_perf_test";
    private static final String superTable = "meters";
    private static Connection connection;
    private static Statement statement;
    private static int g_tableNum = 1000;
    private static int g_RowsNum = 100;
    private static int g_threadNum = 5;
    private static final ArrayList<Connection> g_ConnectionList = new ArrayList<>();
    private static final ArrayList<TSDBPreparedStatement> g_StatementList = new ArrayList<>();
    private static final ArrayList<ArrayList<String>> g_sqls = new ArrayList<>();

    public static void prepareMeta() throws Exception {

        for (int threadNum = 0; threadNum < g_threadNum; threadNum++) {
            g_sqls.add(new ArrayList<>());

            Connection connection = genConnection();
            String sql = "INSERT INTO ? USING " + superTable + " TAGS(?,?) VALUES (?,?,?,?)";
            TSDBPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSDBPreparedStatement.class);

            statement.execute("use " + dbName);


            g_ConnectionList.add(connection);
            g_StatementList.add(statement);
        }
    }

    public static void testInsert(int threadId) throws Exception {
        long b = System.currentTimeMillis();
        Random random = new Random(System.currentTimeMillis());
        TSDBPreparedStatement pstmt = g_StatementList.get(threadId);

        int loopTime = 0;
        for (int i = threadId; i < g_tableNum; i += g_threadNum) {
            loopTime++;

            // set table name
            pstmt.setTableName("c" + i);

            // set tags
            pstmt.setTagInt(0, i);
            pstmt.setTagString(1, "location_" + i);

            // set column ts
            ArrayList<Long> tsList = new ArrayList<>();
            long current = System.currentTimeMillis();
            for (int j = 0; j < g_RowsNum; j++)
                tsList.add(current + j);
            pstmt.setTimestamp(0, tsList);

            // set column current
            ArrayList<Float> currentList = new ArrayList<>();
            for (int j = 0; j < g_RowsNum; j++)
                currentList.add(random.nextFloat() * 30);
            pstmt.setFloat(1, currentList);

            // set column voltage
            ArrayList<Integer> voltageList = new ArrayList<>();
            for (int j = 0; j < g_RowsNum; j++)
                voltageList.add(random.nextInt(300));
            pstmt.setInt(2, voltageList);

            // set column phase
            ArrayList<Float> phaseList = new ArrayList<>();
            for (int j = 0; j < g_RowsNum; j++)
                phaseList.add(random.nextFloat());
            pstmt.setFloat(3, phaseList);
            // add column
            pstmt.columnDataAddBatch();
            // execute column
            pstmt.columnDataExecuteBatch();
        }

        long e = System.currentTimeMillis();
        System.out.println("thread " + threadId + " inserted " + loopTime * g_RowsNum + " rows cost " + (e - b) + " ms");

    }

    public static Connection genConnection() throws SQLException {
        //String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        return connection;
    }


    @Test
    public void testStmtPerf() throws Exception {
        prepareMeta();
        System.out.println("prepareMeta successfully");

        // 创建一个固定大小的线程池
        ExecutorService executor = Executors.newFixedThreadPool(g_threadNum);

        final CountDownLatch latchB = new CountDownLatch(g_threadNum);
        long b = System.currentTimeMillis();
        for (int i = 0; i < g_threadNum; i++) {
            int taskId = i;
            // 提交任务到线程池
            executor.submit(() -> {
                // 调用任务方法
                try {
                    testInsert(taskId);
                    latchB.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        latchB.await();
        System.out.println("threads started successfully.");

        // shutdown the executor, it will not accept new tasks, but will continue to execute the existing tasks
        executor.shutdown();

        try {
            // wait for the executor to terminate
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                System.out.println("thread terminate timeout!");
                executor.shutdownNow();
            } else {
                System.out.println("all tasks executed completely.");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        long e = System.currentTimeMillis();
        System.out.println(g_RowsNum * g_tableNum + " rows cost " + (e - b) + " ms, " + (long)(g_RowsNum * g_tableNum / ((e - b)/1000.0)) + " rows/s");


        // check data
        statement.execute("use " + dbName);
        ResultSet resultSet = statement.executeQuery("select count(*) from " + superTable);
        resultSet.next();
        Assert.assertEquals(g_RowsNum * g_tableNum, resultSet.getInt(1));
        resultSet.close();
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = genConnection();
        statement = connection.createStatement();

        statement.execute("drop database if exists " + dbName);
        statement.execute("create database if not exists " + dbName + " vgroups 30");
        statement.execute("use " + dbName);
        statement.execute("create stable if not exists " + superTable
                + " (ts timestamp, current float, voltage int, phase float) tags(location varchar(64), group_id int)");

        StringBuilder sql = new StringBuilder();
        sql.append("create table");
        int i = 0;
        for (int tableNum = 0; tableNum < g_tableNum; tableNum++) {
            sql.append(" if not exists c" + tableNum + " using " + superTable + " tags(" + "\"location_" + tableNum + "\"" + ", " + tableNum + ")");

            if (i < 1000){
                i++;
            }else {
                statement.execute(sql.toString());
                sql = new StringBuilder();
                sql.append("create table");
                i = 0;
            }
        }
        if (sql.length() > "create table".length()) {
            statement.execute(sql.toString());
        }

        System.out.println("creat db,stable,sub tables successfully");
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null) {
            if (statement != null) {
                statement.close();
            }
            connection.close();
        }

        for (Statement statement: g_StatementList){
            statement.close();
        }

        for (Connection connection: g_ConnectionList){
            connection.close();
        }
    }

}