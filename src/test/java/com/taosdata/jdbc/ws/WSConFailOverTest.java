package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeoutException;


@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket master slave test", author = "yjshe", version = "3.2.11")
@FixMethodOrder
public class WSConFailOverTest {
    private static final String hostA = "127.0.0.1";
    private static final int portA = 6041;

    private static final String hostB = "127.0.0.1";
    private static final int portB = 8041;
    private static final String db_name = "test";
    private static final String tableName = "meters";
    private Connection connection;

    @Description("query")
    @Test
    public void queryBlock() throws Exception  {
        try (Statement statement = connection.createStatement()) {

            ResultSet resultSet;
            for (int i = 0; i < 4; i++){
                try {
                    resultSet = statement.executeQuery("select ts from " + db_name + "." + tableName + " limit 1;");
                    if (i == 2){
                        int port = 8041;
                        ProcessBuilder pb = new ProcessBuilder("sudo", "lsof", "-t", "-i:" + port);
                        Process process = pb.start();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                        String pid;
                        while ((pid = reader.readLine()) != null) {
                            ProcessBuilder killPb = new ProcessBuilder("sudo", "kill", "-9", pid);
                            Process killProcess = killPb.start();
                            killProcess.waitFor();
                            System.out.println("Killed process with PID: " + pid);
                        }
                        reader.close();
                    }
                }catch (SQLException e){
                    if (e.getErrorCode() == TSDBErrorNumbers.ERROR_RESULTSET_CLOSED){
                        System.out.println("connection closed");
                        break;
                    }

                    if (e.getErrorCode() ==  TSDBErrorNumbers.ERROR_QUERY_TIMEOUT){
                        System.out.println("req timeout, will be continue");
                        continue;
                    }

                    System.out.println(e.getMessage());
                    continue;
                }
                resultSet.next();
                System.out.println(resultSet.getLong(1));
                Thread.sleep(2000);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    @Description("consumer")
    @Test
    public void consumerException() {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, hostB);
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(portB));

        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");

        try {
            TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties);
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Before
    public void before() throws SQLException, InterruptedException, IOException {

        Runtime.getRuntime().exec("sudo taosadapter -P 8041 &");

        Thread.sleep(3000);

        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + hostA + ":" + portA + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");

        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, hostB);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(portB));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");


        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName + "(ts timestamp, f int)");
        statement.execute("insert into " + db_name + "." + tableName + " values (now, 1)");
        statement.close();
    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            connection.close();
        }
    }
}
