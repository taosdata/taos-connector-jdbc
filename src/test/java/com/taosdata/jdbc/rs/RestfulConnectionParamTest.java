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
        connectionParam.setEndpoints(null);
        assertEquals(null, connectionParam.getEndpoints());

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

        // Test appName
        connectionParam.setAppName("testAppName");
        assertEquals("testAppName", connectionParam.getAppName());

        // Test appIp
        connectionParam.setAppIp("testAppIp");
        assertEquals("testAppIp", connectionParam.getAppIp());

        // Test copyData
        connectionParam.setCopyData(true);
        assertTrue(connectionParam.isCopyData());

        // Test batchSizeByRow
        connectionParam.setBatchSizeByRow(100);
        assertEquals(100, connectionParam.getBatchSizeByRow());

        // Test cacheSizeByRow
        connectionParam.setCacheSizeByRow(1000);
        assertEquals(1000, connectionParam.getCacheSizeByRow());

        // Test backendWriteThreadNum
        connectionParam.setBackendWriteThreadNum(4);
        assertEquals(4, connectionParam.getBackendWriteThreadNum());

        // Test strictCheck
        connectionParam.setStrictCheck(true);
        assertTrue(connectionParam.isStrictCheck());

        // Test retryTimes
        connectionParam.setRetryTimes(3);
        assertEquals(3, connectionParam.getRetryTimes());

        // Test asyncWrite
        connectionParam.setAsyncWrite("stmt");
        assertEquals("stmt", connectionParam.getAsyncWrite());

        // Test pbsMode
        connectionParam.setPbsMode("line");
        assertEquals("line", connectionParam.getPbsMode());
    }
    @Test(expected = SQLException.class)
    public void testInvalidConnectModeNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidConnectModeOutOfRange() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, "2");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidReconnectIntervalMs() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidReconnectRetryCount() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidBatchSizeByRow() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidCacheSizeByRow() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidCacheSizeNotMultipleOfBatchSize() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "100");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW, "150");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidBackendWriteThreadNum() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRetryTimes() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RETRY_TIMES, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidAsyncWrite() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "invalid");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidPbsMode() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PBS_MODE, "invalid");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidAppNameLength() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_NAME, "ThisAppNameIsWayTooLongForValidation");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidAppIp() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_IP, "invalid_ip");
        ConnectionParam.getParam(properties);
    }

}