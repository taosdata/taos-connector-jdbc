package com.taosdata.jdbc.cloud;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotEquals;

public class CloudTest {
    String[] strings = {"abc", "涛思数据"};
    String[] types = {"BINARY", "TIMESTAMP"};

    @Test
    public void connectCloudService() throws Exception {

//        export TDENGINE_JDBC_URL="jdbc:TAOS-RS://gw.us-east-1.aws.cloud.tdengine.com?usessl=true&token=6363827614de80e382473d2b2febd642b0bae37e"
        String jdbcUrl = System.getenv("TDENGINE_JDBC_URL");
        if (jdbcUrl == null) {
            jdbcUrl = "jdbc:TAOS-RS://gw.us-east-1.aws.cloud.tdengine.com?usessl=true&token=6363827614de80e382473d2b2febd642b0bae37e";
        }

        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select server_version()");
        rs.next();
        String version = rs.getString(1);
        assertNotEquals(version, null);
//        stmt.execute("create database if not exists cloudtest");
        stmt.execute("create table if not exists cloudtest.t0(ts timestamp, c0 binary(20))");
        stmt.execute("insert into cloudtest.t0 values(now, 'abc')(now+1s, '涛思数据')");
        rs = stmt.executeQuery("select * from cloudtest.t0");
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertTrue(Arrays.stream(types).collect(Collectors.toSet()).contains(meta.getColumnTypeName(2)));
        while (rs.next()) {
            Assert.assertTrue(Arrays.stream(strings).collect(Collectors.toSet()).contains(rs.getString(2)));
        }
    }
}
