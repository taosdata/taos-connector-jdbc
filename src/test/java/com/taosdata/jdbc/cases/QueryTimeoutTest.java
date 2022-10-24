package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBStatement;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;


public class QueryTimeoutTest {
    //    private String host = "127.0.0.1";
    private String host = "master";
    private Connection conn;

    @Ignore
    @Test(expected = SQLTimeoutException.class)
    public void execute() throws SQLException {
        // given
        final int timeout = 1;
        final String sql = "select server_status()";
        TSDBStatement stmt = (TSDBStatement) conn.createStatement();

        // when and Then
        stmt.setQueryTimeout(timeout);
        boolean hasResult = stmt.execute(sql);
        if (hasResult) {
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String value = rs.getString(i);
                    Assert.assertEquals("server_status()", meta.getColumnLabel(i));
                    Assert.assertEquals("1", value);
                }
            }
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        conn = DriverManager.getConnection(url);
    }

}
