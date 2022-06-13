package com.taosdata.jdbc.cloud;


import com.taosdata.jdbc.rs.RestfulConnection;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertNotEquals;

public class CloudTest {

    @Test
    public void connectCloudService() throws Exception {
        String jdbcUrl = System.getenv("TDENGINE_JDBC_URL");
        if (jdbcUrl == null) {
            System.out.println("Environment variable for CloudTest not set properly");
            return;
        }
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select server_version()");
        rs.next();
        String version = rs.getString(1);
        System.out.println(version);
        assertNotEquals(version, null);
        stmt.execute("create database if not exists cloudtest");
        stmt.execute("create table if not exists cloudtest.t0(ts timestamp, c0 binary(20))");
        stmt.execute("insert into cloudtest.t0 values(now, 'abc')(now+1s, '涛思数据')");
        rs = stmt.executeQuery("select * from cloudtest.t0");
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            System.out.println(meta.getColumnLabel(i) + " " + meta.getColumnTypeName(i));
        }
        while (rs.next()) {
            Timestamp v1 = rs.getTimestamp(1);
            String v2 = rs.getString(2);
            System.out.println(v1 + " " + v2);
        }
        stmt.execute("drop database if exists cloudtest");
    }
}
