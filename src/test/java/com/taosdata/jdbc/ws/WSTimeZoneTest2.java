package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@TestTarget(alias = "websocket timezon test", author = "sheyj", version = "3.7.0")
public class WSTimeZoneTest2 {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6041;



    @Test
    public void UTC8Test() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + PORT + "/?user=root&password=taosdata";
        } else {
            url += "?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "Asia/Shanghai");
        try ( Connection connection = DriverManager.getConnection(url, properties)) {
            WSConnection wsConnection = (WSConnection) connection;
            Assert.assertEquals("Asia/Shanghai", wsConnection.getParam().getTz());
        }
    }
}
