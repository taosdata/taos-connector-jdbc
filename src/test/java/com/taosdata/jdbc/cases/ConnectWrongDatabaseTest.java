package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectWrongDatabaseTest {

    @Test(expected = SQLException.class)
    public void connectByJni() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://localhost:6030/wrong_db?user=root&password=taosdata";
        } else {
            url += "wrong_db?user=root&password=taosdata";
        }
        DriverManager.getConnection(url);
    }

    @Test(expected = SQLException.class)
    public void connectByRestful() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://localhost:6041/wrong_db?user=root&password=taosdata";
        } else {
            url += "wrong_db?user=root&password=taosdata";
        }
        Connection connection = DriverManager.getConnection(url);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("show databases");
        }
    }

}
