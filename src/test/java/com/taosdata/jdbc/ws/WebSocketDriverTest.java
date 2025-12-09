package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.rs.RestfulDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class WebSocketDriverTest {
    private static final String HOST = "127.0.0.1";

    @Test
    public void acceptsURL() throws SQLException {
        Driver driver = new WebSocketDriver();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":6041";
        }
        boolean isAccept = driver.acceptsURL(url);
        Assert.assertTrue(isAccept);
        String specifyHost = SpecifyAddress.getInstance().getHost();
        if (specifyHost == null) {
            url = "jdbc:TAOS://" + HOST + ":6041";
        } else {
            url = "jdbc:TAOS://" + specifyHost + ":6041";
        }
        isAccept = driver.acceptsURL(url);
        Assert.assertFalse(isAccept);
    }

    @Test
    public void getPropertyInfo() throws SQLException {
        Driver driver = new WebSocketDriver();
        final String url = "";
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, null);
        Assert.assertNotNull(propertyInfo);
    }

    @Test
    @SuppressWarnings("java:S1874")
    public void getMajorVersion() {
        Assert.assertEquals(3, new RestfulDriver().getMajorVersion());
    }

    @Test
    @SuppressWarnings("java:S1874")
    public void getMinorVersion() {
        Assert.assertEquals(0, new RestfulDriver().getMinorVersion());
    }

    @Test
    @SuppressWarnings("java:S1874")
    public void jdbcCompliant() {
        Assert.assertFalse(new RestfulDriver().jdbcCompliant());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getParentLogger() throws SQLFeatureNotSupportedException {
        new WebSocketDriver().getParentLogger();
    }
}
