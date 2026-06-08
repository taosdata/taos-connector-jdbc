package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.VersionUtil;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WSConnectionStmt2BindExecCacheTest {

    @Test
    public void supportsStmt2BindExec_returnsCachedCapabilityFromConstruction() throws Exception {
        Transport transport = Mockito.mock(Transport.class);
        ConnectionParam param = Mockito.mock(ConnectionParam.class);
        WSConnection connection = new WSConnection(
                "jdbc:TAOS-RS://localhost:6041/test",
                new Properties(),
                transport,
                param,
                com.taosdata.jdbc.TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION);

        Field field = com.taosdata.jdbc.AbstractConnection.class.getDeclaredField("supportStmt2BindExec");
        field.setAccessible(true);

        assertTrue(field.getBoolean(connection));
        assertTrue(connection.supportsStmt2BindExec());
        assertTrue(connection.supportsStmt2BindExec());
    }

    @Test
    public void supportsStmt2BindExec_preservesCachedFalseCapability() throws Exception {
        Transport transport = Mockito.mock(Transport.class);
        ConnectionParam param = Mockito.mock(ConnectionParam.class);
        WSConnection connection = new WSConnection(
                "jdbc:TAOS-RS://localhost:6041/test",
                new Properties(),
                transport,
                param,
                "0.0.0.0");

        Field field = com.taosdata.jdbc.AbstractConnection.class.getDeclaredField("supportStmt2BindExec");
        field.setAccessible(true);

        assertFalse(field.getBoolean(connection));
        assertFalse(connection.supportsStmt2BindExec());
        assertFalse(connection.supportsStmt2BindExec());
    }

    @Test
    public void supportsStmt2BindExec_matchesVersionUtilDecisionAtConstruction() throws Exception {
        Transport transport = Mockito.mock(Transport.class);
        ConnectionParam param = Mockito.mock(ConnectionParam.class);

        String supportedVersion = com.taosdata.jdbc.TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION;
        String unsupportedVersion = "0.0.0.0";

        WSConnection supported = new WSConnection(
                "jdbc:TAOS-RS://localhost:6041/test",
                new Properties(),
                transport,
                param,
                supportedVersion);
        WSConnection unsupported = new WSConnection(
                "jdbc:TAOS-RS://localhost:6041/test",
                new Properties(),
                transport,
                param,
                unsupportedVersion);

        assertTrue(VersionUtil.supportStmt2BindExec(supportedVersion));
        assertFalse(VersionUtil.supportStmt2BindExec(unsupportedVersion));
        assertTrue(supported.supportsStmt2BindExec());
        assertFalse(unsupported.supportsStmt2BindExec());
    }

    @Test
    public void abstractConnection_noLongerDeclaresSupportLineBindField() {
        try {
            com.taosdata.jdbc.AbstractConnection.class.getDeclaredField("supportLineBind");
            fail("supportLineBind should be removed after line-mode write-path deletion");
        } catch (NoSuchFieldException expected) {
            assertTrue(true);
        }
    }
}
