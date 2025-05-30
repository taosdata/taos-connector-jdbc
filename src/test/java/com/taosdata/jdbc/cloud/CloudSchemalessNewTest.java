package com.taosdata.jdbc.cloud;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.confprops.HttpKeepAliveTest;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;


public class CloudSchemalessNewTest {
    String url = null;
    public static Connection connection;
    String dbName = "javatest";

    @Before
    public void before() throws SQLException {
        url = System.getenv("TDENGINE_CLOUD_URL");
        if (url == null || "".equals(url.trim())) {
            System.out.println("Environment variable for CloudTest not set properly");
            return;
        }
        connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + dbName);
    }

    @Test
    public void testLine() throws SQLException {
        if (url == null || "".equals(url.trim())) {
            return;
        }

        // given
        long cur_time = System.currentTimeMillis();
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\",ts=" + cur_time + " c1=3i64,c3=L\"passit\",c2=false,c4=4f64 " + cur_time};

        // when
        ((AbstractConnection)connection).write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
        // then
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from javatest.st order by _ts DESC limit 1");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        while (rs.next()) {
            Assert.assertEquals(cur_time, rs.getLong("_ts"));
        }
        rs.close();
        statement.close();
    }
}
