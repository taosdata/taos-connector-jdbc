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
        // Original parameter tests
        connectionParam.setEndpoints(null);
        assertNull(connectionParam.getEndpoints());

        connectionParam.setDatabase("testDB");
        assertEquals("testDB", connectionParam.getDatabase());

        connectionParam.setCloudToken("testToken");
        assertEquals("testToken", connectionParam.getCloudToken());

        connectionParam.setUser("testUser");
        assertEquals("testUser", connectionParam.getUser());

        connectionParam.setPassword("testPassword");
        assertEquals("testPassword", connectionParam.getPassword());

        connectionParam.setTz("UTC");
        assertEquals("UTC", connectionParam.getTz());

        connectionParam.setUseSsl(true);
        assertTrue(connectionParam.isUseSsl());

        connectionParam.setMaxRequest(100);
        assertEquals(100, connectionParam.getMaxRequest());

        connectionParam.setConnectTimeout(5000);
        assertEquals(5000, connectionParam.getConnectTimeout());

        connectionParam.setRequestTimeout(3000);
        assertEquals(3000, connectionParam.getRequestTimeout());

        connectionParam.setConnectMode(1);
        assertEquals(1, connectionParam.getConnectMode());

        connectionParam.setEnableCompression(true);
        assertTrue(connectionParam.isEnableCompression());

        connectionParam.setSlaveClusterHost("slaveHost");
        assertEquals("slaveHost", connectionParam.getSlaveClusterHost());

        connectionParam.setSlaveClusterPort(8081);
        assertEquals(8081, connectionParam.getSlaveClusterPort());

        connectionParam.setReconnectIntervalMs(2000);
        assertEquals(2000, connectionParam.getReconnectIntervalMs());

        connectionParam.setReconnectRetryCount(5);
        assertEquals(5, connectionParam.getReconnectRetryCount());

        connectionParam.setEnableAutoConnect(true);
        assertTrue(connectionParam.isEnableAutoConnect());

        connectionParam.setDisableSslCertValidation(true);
        assertTrue(connectionParam.isDisableSslCertValidation());

        connectionParam.setAppName("testAppName");
        assertEquals("testAppName", connectionParam.getAppName());

        connectionParam.setAppIp("testAppIp");
        assertEquals("testAppIp", connectionParam.getAppIp());

        connectionParam.setCopyData(true);
        assertTrue(connectionParam.isCopyData());

        connectionParam.setBatchSizeByRow(100);
        assertEquals(100, connectionParam.getBatchSizeByRow());

        connectionParam.setCacheSizeByRow(1000);
        assertEquals(1000, connectionParam.getCacheSizeByRow());

        connectionParam.setBackendWriteThreadNum(4);
        assertEquals(4, connectionParam.getBackendWriteThreadNum());

        connectionParam.setStrictCheck(true);
        assertTrue(connectionParam.isStrictCheck());

        connectionParam.setRetryTimes(3);
        assertEquals(3, connectionParam.getRetryTimes());

        connectionParam.setAsyncWrite("stmt");
        assertEquals("stmt", connectionParam.getAsyncWrite());

        connectionParam.setPbsMode("line");
        assertEquals("line", connectionParam.getPbsMode());

        connectionParam.setWsKeepAlive(300);
        assertEquals(300, connectionParam.getWsKeepAlive());

        // Health check parameter Setter/Getter tests
        connectionParam.setHealthCheckInitInterval(10);
        assertEquals(10, connectionParam.getHealthCheckInitInterval());

        connectionParam.setHealthCheckMaxInterval(300);
        assertEquals(300, connectionParam.getHealthCheckMaxInterval());

        connectionParam.setHealthCheckConTimeout(1);
        assertEquals(1, connectionParam.getHealthCheckConTimeout());

        connectionParam.setHealthCheckCmdTimeout(5);
        assertEquals(5, connectionParam.getHealthCheckCmdTimeout());

        connectionParam.setHealthCheckRecoveryCount(3);
        assertEquals(3, connectionParam.getHealthCheckRecoveryCount());

        connectionParam.setHealthCheckRecoveryInterval(60);
        assertEquals(60, connectionParam.getHealthCheckRecoveryInterval());

        // Rebalance parameter Setter/Getter tests
        connectionParam.setRebalanceThreshold(20);
        assertEquals(20, connectionParam.getRebalanceThreshold());

        connectionParam.setRebalanceConBaseCount(30);
        assertEquals(30, connectionParam.getRebalanceConBaseCount());
    }

    // Health check parameter invalid value tests
    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckInitIntervalZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckInitIntervalNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckMaxIntervalLessThanInit() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "10");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "5");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckMaxIntervalZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckMaxIntervalNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "-10");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckConTimeoutZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CON_TIMEOUT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckConTimeoutNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CON_TIMEOUT, "-2");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckCmdTimeoutZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CMD_TIMEOUT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckCmdTimeoutNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CMD_TIMEOUT, "-3");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckRecoveryCountZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckRecoveryCountNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "-4");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckRecoveryIntervalNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_INTERVAL, "-10");
        ConnectionParam.getParam(properties);
    }

    // Rebalance parameter invalid value tests
    @Test(expected = SQLException.class)
    public void testInvalidRebalanceThresholdLessThan10() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "9");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRebalanceThresholdGreaterThan50() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "55");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRebalanceThresholdNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "-20");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRebalanceConBaseCountZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRebalanceConBaseCountNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, "-15");
        ConnectionParam.getParam(properties);
    }
}