package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class BatchInsertTest {

    static final String host = "127.0.0.1";
    static final String dbName = TestUtils.camelToSnake(BatchInsertTest.class);
    static final String stbName = "meters";
    static final int numOfTables = 30;
    static final int NUM_OF_RECORDS_PER_TABLE = 1000;
    static final long ts = 1496732686000L;
    static final String TABLE_PREFIX = "t";
    private Connection connection;

    @Before
    public void before() {
        try {
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            String url = SpecifyAddress.getInstance().getJniWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + host + ":0/";
            }
            connection = DriverManager.getConnection(url, properties);

            Statement statement = connection.createStatement();
            statement.executeUpdate("drop database if exists " + dbName);
            statement.executeUpdate("create database if not exists " + dbName);
            statement.executeUpdate("use " + dbName);
            // create stable
            String createTableSql = "create table " + stbName + "(ts timestamp, f1 int, f2 int, f3 int) tags(areaid int, loc binary(20))";
            statement.executeUpdate(createTableSql);
            // create tables
            for (int i = 0; i < numOfTables; i++) {
                String loc = i % 2 == 0 ? "beijing" : "shanghai";
                String createSubTalbesSql = "create table " + TABLE_PREFIX + i + " using " + stbName + " tags(" + i + ", '" + loc + "')";
                statement.executeUpdate(createSubTalbesSql);
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBatchInsert() {
        ExecutorService executorService = Executors.newFixedThreadPool(numOfTables);
        for (int i = 0; i < numOfTables; i++) {
            final int index = i;
            executorService.execute(() -> {
                try {
                    Statement statement = connection.createStatement(); // get statement
                    StringBuilder sb = new StringBuilder();
                    sb.append("INSERT INTO " + TABLE_PREFIX + index + " VALUES");
                    Random rand = new Random();
                    for (int j = 1; j <= NUM_OF_RECORDS_PER_TABLE; j++) {
                        sb.append("(" + (ts + j) + ", ");
                        sb.append(rand.nextInt(100) + ", ");
                        sb.append(rand.nextInt(100) + ", ");
                        sb.append(rand.nextInt(100) + ")");
                    }
                    statement.addBatch(sb.toString());
                    statement.executeBatch();
                    connection.commit();
                    statement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from meters");
            int num = 0;
            while (rs.next()) {
                num++;
            }
            assertEquals(num, numOfTables * NUM_OF_RECORDS_PER_TABLE);
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (connection != null) {
                Statement statement = connection.createStatement();
                statement.execute("drop database if exists " + dbName);
                statement.close();
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
