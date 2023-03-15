package com.taosdata.jdbc.cloud;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class SchemalessTest {
    String url = null;
    public static SchemalessWriter writer;
    public static Connection connection;
    String dbName = "javatest";

    @Before
    public void before() throws SQLException {
        String url = System.getenv("TDENGINE_CLOUD_URL");
        System.out.println("cloud url:" + url);
        if (url == null || "".equals(url.trim())) {
            System.out.println("Environment variable for CloudTest not set properly");
            return;
        }
//        connection = DriverManager.getConnection(url);
//        writer = new SchemalessWriter(url, null, null, dbName);
    }

    @Test
    @Ignore
    public void testLine() throws SQLException {
        // given
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};

        // when
        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);
        // then
//        Statement statement = connection.createStatement();
//        statement.executeUpdate("use " + dbName);
//        ResultSet rs = statement.executeQuery("show tables");
//        Assert.assertNotNull(rs);
//        ResultSetMetaData metaData = rs.getMetaData();
//        Assert.assertTrue(metaData.getColumnCount() > 0);
//        int rowCnt = 0;
//        while (rs.next()) {
//            rowCnt++;
//        }
//        Assert.assertEquals(lines.length, rowCnt);
//        rs.close();
//        statement.close();
    }
}
