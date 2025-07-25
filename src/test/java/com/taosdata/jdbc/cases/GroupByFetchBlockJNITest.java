package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.common.BaseTest;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class GroupByFetchBlockJNITest extends BaseTest {
    private static Connection connection;
    private static Statement statement;

    private String host = "127.0.0.1";
    private String dbName = TestUtils.camelToSnake(GroupByFetchBlockJNITest.class);
    private String tName = "st";

    @Test
    public void groupbyTest() throws SQLException {
        String sql = "select symbol,max(high) from " + dbName + "." + tName + " where kline_type = '1m' group by symbol;";

        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        Assert.assertEquals("1m", resultSet.getString(1));
        Assert.assertEquals(2.2, resultSet.getDouble(2), 0);
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        connection = DriverManager.getConnection(url);
        statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database " + dbName);
        statement.execute("use " + dbName);
        statement.execute("create table " + tName + " (ts timestamp,open_time bigint, low double, open double, high double, close double,close_time bigint, volume double, trades bigint,\n" +
                "quote_volume double, changed double,amplitude double,ticket_id bigint) tags(symbol nchar(50), kline_type nchar(20))");
        statement.execute("insert into t1 using " + tName + " tags ('1m','1m') values (now, 1, 1.1, 1.1, 1.1, 1.1, 1, 1.1, 1, 1.1,1.1,1.1,1);");
        statement.execute("insert into t1 using " + tName + " tags ('1m','1m') values (now, 2, 2.2, 2.2, 2.2, 2.2, 2, 2.2, 2, 2.2,2.2,2.2,2)");
    }

    @After
    public void after() throws SQLException {
        statement.execute("drop database if exists " + dbName);
        statement.close();
        connection.close();
    }
}
