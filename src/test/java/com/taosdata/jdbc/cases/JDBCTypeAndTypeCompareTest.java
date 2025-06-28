package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

@Ignore
public class JDBCTypeAndTypeCompareTest {
    private static Connection conn;
    private static final String dbname = TestUtils.camelToSnake(JDBCTypeAndTypeCompareTest.class);

    @Test
    public void test() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://127.0.0.1:6030/";
        }
        conn = DriverManager.getConnection(url, "root", "taosdata");
        Statement stmt = conn.createStatement();

        stmt.execute("drop database if exists " + dbname);
        stmt.execute("create database if not exists " + dbname);
        stmt.execute("use " + dbname);
        stmt.execute("create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(10), f9 nchar(10) )");
        stmt.execute("insert into weather values(now, 1, 2, 3.0, 4.0, 5, 6, true, 'test','test')");

        ResultSet rs = stmt.executeQuery("select * from weather");
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String columnName = meta.getColumnName(i);
                String columnTypeName = meta.getColumnTypeName(i);
                Object value = rs.getObject(i);
                System.out.printf("columnName : %s, columnTypeName: %s, JDBCType: %s\n", columnName, columnTypeName, value.getClass().getName());
            }
        }

        stmt.close();
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (null != conn) {
                Statement statement = conn.createStatement();
                statement.execute("drop database if exists " + dbname);
                statement.close();
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
