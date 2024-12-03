package com.taosdata.jdbc.rs;
import com.taosdata.jdbc.TSDBDriver;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.*;

public class RestfulConnectionParamTest {
    private ConnectionParam connectionParam;

    @Before
    public void setUp() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        connectionParam = ConnectionParam.getParam(properties);
    }

    @Test
    public void testSettersAndGetters() {
        // Test host
        connectionParam.setHost("newHost");
        assertEquals("newHost", connectionParam.getHost());

        // Test port
        connectionParam.setPort("9090");
        assertEquals("9090", connectionParam.getPort());

        // Test database
        connectionParam.setDatabase("testDB");
        assertEquals("testDB", connectionParam.getDatabase());

        // Test cloudToken
        connectionParam.setCloudToken("testToken");
        assertEquals("testToken", connectionParam.getCloudToken());

        // Test user
        connectionParam.setUser("testUser");
        assertEquals("testUser", connectionParam.getUser());

        // Test password
        connectionParam.setPassword("testPassword");
        assertEquals("testPassword", connectionParam.getPassword());

        // Test timezone
        connectionParam.setTz("UTC");
        assertEquals("UTC", connectionParam.getTz());

        // Test useSsl
        connectionParam.setUseSsl(true);
        assertTrue(connectionParam.isUseSsl());

        // Test maxRequest
        connectionParam.setMaxRequest(100);
        assertEquals(100, connectionParam.getMaxRequest());

        // Test connectTimeout
        connectionParam.setConnectTimeout(5000);
        assertEquals(5000, connectionParam.getConnectTimeout());

        // Test requestTimeout
        connectionParam.setRequestTimeout(3000);
        assertEquals(3000, connectionParam.getRequestTimeout());

        // Test connectMode
        connectionParam.setConnectMode(1);
        assertEquals(1, connectionParam.getConnectMode());

        // Test enableCompression
        connectionParam.setEnableCompression(true);
        assertTrue(connectionParam.isEnableCompression());

        // Test slaveClusterHost
        connectionParam.setSlaveClusterHost("slaveHost");
        assertEquals("slaveHost", connectionParam.getSlaveClusterHost());

        // Test slaveClusterPort
        connectionParam.setSlaveClusterPort("8081");
        assertEquals("8081", connectionParam.getSlaveClusterPort());

        // Test reconnectIntervalMs
        connectionParam.setReconnectIntervalMs(2000);
        assertEquals(2000, connectionParam.getReconnectIntervalMs());

        // Test reconnectRetryCount
        connectionParam.setReconnectRetryCount(5);
        assertEquals(5, connectionParam.getReconnectRetryCount());

        // Test enableAutoConnect
        connectionParam.setEnableAutoConnect(true);
        assertTrue(connectionParam.isEnableAutoConnect());

        // Test disableSslCertValidation
        connectionParam.setDisableSslCertValidation(true);
        assertTrue(connectionParam.isDisableSslCertValidation());
    }
}