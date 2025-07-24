package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.TimeZoneResetRule;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverAutoloadTest {

    private Properties properties;
    private final String host = "127.0.0.1";

    @Rule
    public TimeZoneResetRule timeZoneResetRule = new TimeZoneResetRule();
    @Test
    public void testRestful() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        }
        Connection conn = DriverManager.getConnection(url, properties);
        Assert.assertNotNull(conn);
        conn.close();
    }

    @Test
    public void testJni() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        }
        Connection conn = DriverManager.getConnection(url, properties);
        Assert.assertNotNull(conn);
        conn.close();
    }


    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
    }

}
