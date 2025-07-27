package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

@TestTarget(alias = "websocket timezon test", author = "sheyj", version = "3.7.0")
public class WSTimeZoneTest2 {

    private static final String host = "127.0.0.1";
    private static final int port = 6041;



    @Test
    public void UTC8Test() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=root&password=taosdata";
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
