package com.taosdata.jdbc.ws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SchemalessTest {
    private static final String host = "127.0.0.1";
    private static final String dbName = "test_schemaless_ws";
    private static final String dbName_ttl = "test_schemaless_ws_ttl";
    public static SchemalessWriter writer;
    public static Connection connection;

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        }
        connection = DriverManager.getConnection(url);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop database if exists " + dbName);
            statement.executeUpdate("create database " + dbName);
        }
        writer = new SchemalessWriter(url, "root", "taosdata", dbName);
    }

    @Test
    public void testLine() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};

        // when
        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);
        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + dbName);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(lines.length, rowCnt);
        rs.close();
        statement.close();
    }

    @Test
    public void testLineTtl() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};

        // when
        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, dbName_ttl, 1000);
    }


    @Test
    public void telnetInsert() throws SQLException {
        // given
        String[] lines = new String[]{
                "stb0_0 1626006833 4 host=host0 interface=eth0",
                "stb0_1 1626006833 4 host=host0 interface=eth0",
                "stb0_2 1626006833 4 host=host0 interface=eth0 id=\"special_name\"",
        };

        // when

        writer.write(lines, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + dbName);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(lines.length, rowCnt);
        rs.close();
        statement.close();
    }

    @Test
    public void jsonInsert() throws SQLException {
        // given
        String json = "[\n" +
                "  {\n" +
                "    \"metric\": \"cpu_load_1\",\n" +
                "    \"timestamp\": 1626006833,\n" +
                "    \"value\": 55.5,\n" +
                "    \"tags\": {\n" +
                "      \"host\": \"ubuntu\",\n" +
                "      \"interface\": \"eth1\",\n" +
                "      \"Id\": \"tb1\"\n" +
                "    }\n" +
                "  },\n" +
                "  {\n" +
                "    \"metric\": \"cpu_load_2\",\n" +
                "    \"timestamp\": 1626006834,\n" +
                "    \"value\": 55.5,\n" +
                "    \"tags\": {\n" +
                "      \"host\": \"ubuntu\",\n" +
                "      \"interface\": \"eth2\",\n" +
                "      \"Id\": \"tb2\"\n" +
                "    }\n" +
                "  }\n" +
                "]";

        // when
        writer.write(json, SchemalessProtocolType.JSON, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + dbName);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }

        Assert.assertEquals(((JSONArray) JSONObject.parse(json)).size(), rowCnt);
        rs.close();
        statement.close();
    }

    @Test
    public void telnetListInsert() throws SQLException {
        // given
        List<String> list = new ArrayList<>();
        list.add("stb0_0 1626006833 4 host=host0 interface=eth0");
        list.add("stb0_1 1626006833 4 host=host0 interface=eth0");
        list.add("stb0_2 1626006833 4 host=host0 interface=eth0 id=\"special_name\"");
        // when

        writer.write(list, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);

        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + dbName);
        ResultSet rs = statement.executeQuery("show tables");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        int rowCnt = 0;
        while (rs.next()) {
            rowCnt++;
        }
        Assert.assertEquals(list.size(), rowCnt);
        rs.close();
        statement.close();
    }

    @AfterClass
    public static void after() {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
