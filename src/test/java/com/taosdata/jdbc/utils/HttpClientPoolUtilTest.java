//package com.taosdata.jdbc.utils;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.taosdata.jdbc.TSDBDriver;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import java.io.UnsupportedEncodingException;
//import java.net.URLEncoder;
//import java.nio.charset.StandardCharsets;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.List;
//import java.util.Properties;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//@Ignore
//public class HttpClientPoolUtilTest {
//
//    String user = "root";
//    String password = "taosdata";
//    static String host = "127.0.0.1";
//    private static Connection connection;
//    private static String db_name = "http_test";
//
//    @Test
//    public void useLog() {
//        // given
//        int multi = 10;
//
//        // when
//        List<Thread> threads = IntStream.range(0, multi).mapToObj(i -> new Thread(() -> {
//            try {
//                String token = login(multi);
//                executeOneSql("use log", token);
//            } catch (SQLException | UnsupportedEncodingException | JsonProcessingException e) {
//                e.printStackTrace();
//            }
//        })).collect(Collectors.toList());
//
//        threads.forEach(Thread::start);
//
//        for (Thread thread : threads) {
//            try {
//                thread.join();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    @Test
//    public void tableNotExist() {
//        // given
//        int multi = 20;
//
//        // when
//        List<Thread> threads = IntStream.range(0, multi * 25).mapToObj(i -> new Thread(() -> {
//            try {
////                String token = "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04";
//                String token = login(multi);
//                executeOneSql("insert into " + db_name + ".tb_not_exist values(now, 1)", token);
//                executeOneSql("select last(*) from " + db_name + ".dn", token);
//            } catch (SQLException | UnsupportedEncodingException | JsonProcessingException e) {
//                e.printStackTrace();
//            }
//        })).collect(Collectors.toList());
//
//        threads.forEach(Thread::start);
//
//        for (Thread thread : threads) {
//            try {
//                thread.join();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    private String login(int connPoolSize) throws SQLException, UnsupportedEncodingException, JsonProcessingException {
//        user = URLEncoder.encode(user, StandardCharsets.UTF_8.displayName());
//        password = URLEncoder.encode(password, StandardCharsets.UTF_8.displayName());
//        String loginUrl;
//        String specifyHost = SpecifyAddress.getInstance().getHost();
//        if (null == specifyHost) {
//            loginUrl = "http://" + host + ":" + 6041 + "/rest/login/" + user + "/" + password;
//        } else {
//            loginUrl = "http://" + specifyHost + ":" + 6041 + "/rest/login/" + user + "/" + password;
//        }
//        Properties properties = new Properties();
//        properties.setProperty(TSDBDriver.HTTP_POOL_SIZE, String.valueOf(connPoolSize));
//        properties.setProperty(TSDBDriver.HTTP_KEEP_ALIVE, "false");
//        HttpClientPoolUtil.init(properties);
//        String result = HttpClientPoolUtil.execute(loginUrl);
//        JsonNode jsonResult = JsonUtil.getObjectMapper().readTree(result);
//        String token = jsonResult.get("desc").asText();
//        if (jsonResult.get("code").asInt() != 0) {
//            throw new SQLException(jsonResult.get("desc").asText());
//        }
//        return "Basic " + token;
//    }
//
//    private boolean executeOneSql(String sql, String token) throws SQLException, JsonProcessingException {
//
//        String url;
//        String specifyHost = SpecifyAddress.getInstance().getHost();
//        if (null == specifyHost) {
//            url = "http://" + host + ":6041/rest/sql";
//        } else {
//            url = "http://" + specifyHost + ":6041/rest/sql";
//        }
//        String result = HttpClientPoolUtil.execute(url, sql, token, null);
//        JsonNode resultJson =  JsonUtil.getObjectMapper().readTree(result);
//        if (resultJson.get("code").asInt() == 0) {
////            HttpClientPoolUtil.reset();
////            throw TSDBError.createSQLException(resultJson.getInteger("code"), resultJson.getString("desc"));
//            return false;
//        }
//        return true;
//    }
//
//
//    @BeforeClass
//    public static void beforeClass() throws SQLException {
//        Properties properties = new Properties();
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
//        String url = SpecifyAddress.getInstance().getRestUrl();
//        if (url == null) {
//            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
//        }
//        connection = DriverManager.getConnection(url, properties);
//        // create test database for test cases
//        try (Statement stmt = connection.createStatement()) {
//            stmt.execute("create database if not exists " + db_name);
//        }
//
//    }
//
//    @AfterClass
//    public static void afterClass() throws SQLException {
//        if (connection != null) {
//            Statement statement = connection.createStatement();
//            statement.execute("drop database if exists " + db_name);
//            statement.close();
//            connection.close();
//        }
//    }
//
//}