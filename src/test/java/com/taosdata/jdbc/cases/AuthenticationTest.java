package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.BaseTest;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

public class AuthenticationTest extends BaseTest {

    private static final String host = "127.0.0.1";
    private static final String user = "root";
    private static final String password = "taos?data";
    private Connection conn;

    @Test
    public void connectWithoutUserByJni() {
        try {
            String url = SpecifyAddress.getInstance().getJniWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + host + ":0/?";
            }
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (0x2319): user is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutUserByRestful() {
        try {
            String url = SpecifyAddress.getInstance().getRestWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + host + ":6041/?";
            }
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (0x2319): user is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutPasswordByJni() {
        try {
            String url = SpecifyAddress.getInstance().getJniWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + host + ":0/?user=root";
            } else {
                url += "?user=root";
            }
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (0x231a): password is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutPasswordByRestful() {
        try {
            String url = SpecifyAddress.getInstance().getRestWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + host + ":6041/?user=root";
            } else {
                url += "?user=root";
            }
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (0x231a): password is required", e.getMessage());
        }
    }

    @Ignore
    @Test
    public void test() throws SQLException {
        // change password
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=" + user + "&password=taosdata";
        }
        conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("alter user " + user + " pass '" + password + "'");
        stmt.close();
        conn.close();

        // use new to login and execute query
        url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=" + user + "&password=" + password;
        } else {
            url += "?user=" + user + "&password=" + password;
        }
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        stmt.execute("show databases");
        ResultSet rs = stmt.getResultSet();
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.print(meta.getColumnLabel(i) + ":" + rs.getString(i) + "\t");
            }
            System.out.println();
        }

        // change password back
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        stmt.execute("alter user " + user + " pass 'taosdata'");
        stmt.close();
        conn.close();
    }

}
