package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Source-level enforcement tests for the simplified WSRetryableStmt compatibility rules.
 */
public class WSRetryableStmtCompatibilityEnforcementTest {

    /**
     * Test that default constructor exists and is public.
     */
    @Test
    public void testDefaultConstructorIsPublic() throws Exception {
        java.lang.reflect.Constructor<?> defaultCtor = WSRetryableStmt.class.getConstructor(
                com.taosdata.jdbc.AbstractConnection.class,
                com.taosdata.jdbc.common.ConnectionParam.class,
                String.class,
                Transport.class,
                Long.class,
                com.taosdata.jdbc.ws.stmt2.entity.StmtInfo.class,
                java.util.concurrent.atomic.AtomicInteger.class
        );
        assertNotNull("Default constructor should exist", defaultCtor);
        assertTrue("Default constructor should be public", 
                java.lang.reflect.Modifier.isPublic(defaultCtor.getModifiers()));
    }

    /**
     * The simplified implementation should no longer expose the legacy boolean constructor.
     */
    @Test
    public void testLegacyBindExecConstructorRemoved() throws Exception {
        try {
            WSRetryableStmt.class.getDeclaredConstructor(
                    com.taosdata.jdbc.AbstractConnection.class,
                    com.taosdata.jdbc.common.ConnectionParam.class,
                    String.class,
                    Transport.class,
                    Long.class,
                    com.taosdata.jdbc.ws.stmt2.entity.StmtInfo.class,
                    java.util.concurrent.atomic.AtomicInteger.class,
                    boolean.class
            );
            fail("Legacy constructor with explicit useBindExec parameter should have been removed");
        } catch (NoSuchMethodException expected) {
            // Expected
        }
    }

    @Test
    public void testConstructorDerivesWriteEligibilityFromWSConnectionCapability() throws Exception {
        ConnectionParam param = mockParam();
        WSRetryableStmtRealBehaviorTest.FakeTransport transport =
                new WSRetryableStmtRealBehaviorTest.FakeTransport(param);
        WSConnection capableConnection = mock(WSConnection.class);
        when(capableConnection.supportsStmt2BindExec()).thenReturn(true);

        WSRetryableStmt capableStmt = newStmt(capableConnection, param, transport, "INSERT INTO t VALUES(?)");
        assertTrue("Capable WSConnection should enable bind-exec for writes", capableStmt.isUsingBindExec());

        AbstractConnection nonWsConnection = mock(AbstractConnection.class);
        WSRetryableStmt nonWsStmt = newStmt(nonWsConnection, param, transport, "INSERT INTO t VALUES(?)");
        assertFalse("Non-WSConnection should not enable bind-exec", nonWsStmt.isUsingBindExec());
    }

    @Test
    public void testQueryPathIsSeparatedFromWriteBindExecGate() throws Exception {
        ConnectionParam param = mockParam();
        WSRetryableStmtRealBehaviorTest.FakeTransport transport =
                new WSRetryableStmtRealBehaviorTest.FakeTransport(param);
        WSConnection capableConnection = mock(WSConnection.class);
        when(capableConnection.supportsStmt2BindExec()).thenReturn(true);

        WSRetryableStmt stmt = newStmt(capableConnection, param, transport, "SELECT * FROM t WHERE c1=?");
        assertTrue("Capable websocket connection should enable bind-exec for writes", stmt.isUsingBindExec());

        ByteBuf rawBlock = Unpooled.buffer(32);
        rawBlock.writeLongLE(0L);
        rawBlock.writeLongLE(12345L);
        rawBlock.writeIntLE(1);
        try {
            stmt.queryWithRetry(rawBlock);
            List<String> actions = transport.getRecordedActions();
            assertEquals("Should send exactly 3 actions", 3, actions.size());
            assertEquals("First action should be stmt2_bind", "stmt2_bind", actions.get(0));
            assertEquals("Second action should be stmt2_exec", "stmt2_exec", actions.get(1));
            assertEquals("Third action should be stmt2_result",
                    com.taosdata.jdbc.ws.entity.Action.STMT2_USE_RESULT.getAction(), actions.get(2));
            assertFalse("Query path should not send stmt2_bind_exec", actions.contains("stmt2_bind_exec"));
        } finally {
            rawBlock.release();
        }
    }

    /**
     * Test that WSConnection has the compatibility check method.
     */
    @Test
    public void testWSConnectionHasCompatibilityCheckMethod() throws Exception {
        java.lang.reflect.Method method = WSConnection.class.getMethod("supportsStmt2BindExec");
        assertNotNull("WSConnection should have supportsStmt2BindExec() method", method);
        assertEquals("supportsStmt2BindExec should return boolean",
                boolean.class, method.getReturnType());
        assertTrue("supportsStmt2BindExec should be public",
                java.lang.reflect.Modifier.isPublic(method.getModifiers()));
    }

    private static ConnectionParam mockParam() {
        ConnectionParam param = mock(ConnectionParam.class);
        when(param.isEnableAutoConnect()).thenReturn(false);
        when(param.getRetryTimes()).thenReturn(1);
        when(param.getRequestTimeout()).thenReturn(5000);
        when(param.getZoneId()).thenReturn(ZoneId.systemDefault());
        return param;
    }

    private static WSRetryableStmt newStmt(AbstractConnection connection, ConnectionParam param,
                                           Transport transport, String sql) {
        StmtInfo stmtInfo = new StmtInfo(sql);
        stmtInfo.setStmtId(12345L);
        return new WSRetryableStmt(
                connection,
                param,
                "test_db",
                transport,
                1L,
                stmtInfo,
                new AtomicInteger(0)
        );
    }
}
