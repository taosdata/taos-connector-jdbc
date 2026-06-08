package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.stmt2.entity.ResultResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Compatibility tests for the simplified WSRetryableStmt bind-exec model.
 *
 * <p>These tests stay at the reflection/source-inspection level and lock in the
 * current internal contract: write eligibility comes from websocket capability,
 * while actual bind-exec activation is still gated by operation type.
 */
public class WSRetryableStmtCompatibilityTest {

    /**
     * Test that the public constructor still exists and the eligibility field remains final.
     */
    @Test
    public void testDefaultConstructorExistsAndEligibilityFieldIsFinal() throws Exception {
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
        
        // Verify the code path by checking that useBindExec field is final
        Field useBindExecField = WSRetryableStmt.class.getDeclaredField("useBindExec");
        assertTrue("useBindExec should be final to prevent runtime modification",
                java.lang.reflect.Modifier.isFinal(useBindExecField.getModifiers()));
    }

    /**
     * The old per-call bind-exec constructor should no longer exist.
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

    /**
     * Test that isUsingBindExec() is NOT public, preventing external inspection
     * of the dormant feature state. It should be package-private for internal testing only.
     */
    @Test
    public void testIsUsingBindExecMethodIsNotPublic() throws Exception {
        java.lang.reflect.Method method = WSRetryableStmt.class.getDeclaredMethod("isUsingBindExec");
        assertNotNull("isUsingBindExec method should exist for internal testing", method);
        assertEquals("isUsingBindExec should return boolean", boolean.class, method.getReturnType());
        
        // Verify it's NOT public - should be package-private
        assertFalse("isUsingBindExec should NOT be public to hide dormant implementation",
                java.lang.reflect.Modifier.isPublic(method.getModifiers()));
    }

    /**
     * Test that the implementation still keeps both protocol paths and gates
     * bind-exec to write operations only.
     */
    @Test
    public void testWriteAndQueryProtocolPathsAreSeparatedByOperationType() throws Exception {
        Field operationWrite = WSRetryableStmt.class.getDeclaredField("OPERATION_TYPE_WRITE");
        Field operationQuery = WSRetryableStmt.class.getDeclaredField("OPERATION_TYPE_QUERY");
        assertNotNull("OPERATION_TYPE_WRITE should exist", operationWrite);
        assertNotNull("OPERATION_TYPE_QUERY should exist", operationQuery);

        ConnectionParam param = mockParam();
        WSConnection capableConnection = mock(WSConnection.class);
        when(capableConnection.supportsStmt2BindExec()).thenReturn(true);

        WSRetryableStmtRealBehaviorTest.FakeTransport writeTransport =
                new WSRetryableStmtRealBehaviorTest.FakeTransport(param);
        WSRetryableStmt writeStmt = newStmt(capableConnection, param, writeTransport,
                "INSERT INTO t VALUES(?)");
        ByteBuf writeBlock = newRawBlock();
        try {
            writeStmt.writeBlockWithRetrySync(writeBlock);
            assertEquals("Write path should use stmt2_bind_exec only",
                    Collections.singletonList(Action.STMT2_BIND_EXEC.getAction()),
                    writeTransport.getRecordedActions());
        } finally {
            releaseIfNeeded(writeBlock);
        }

        WSRetryableStmtRealBehaviorTest.FakeTransport queryTransport =
                new WSRetryableStmtRealBehaviorTest.FakeTransport(param);
        WSRetryableStmt queryStmt = newStmt(capableConnection, param, queryTransport,
                "SELECT * FROM t WHERE c1=?");
        ByteBuf queryBlock = newRawBlock();
        try {
            ResultResp result = queryStmt.queryWithRetry(queryBlock);
            assertNotNull("Query path should return a result response", result);
            List<String> actions = queryTransport.getRecordedActions();
            assertEquals("Query path should stay on bind + exec + result",
                    Arrays.asList(
                            Action.STMT2_BIND.getAction(),
                            Action.STMT2_EXEC.getAction(),
                            Action.STMT2_USE_RESULT.getAction()),
                    actions);
            assertFalse("Query path should not send stmt2_bind_exec",
                    actions.contains(Action.STMT2_BIND_EXEC.getAction()));
        } finally {
            releaseIfNeeded(queryBlock);
        }
    }

    /**
     * Verify that Action.STMT2_BIND_EXEC exists in the protocol enum for capable write paths.
     */
    @Test
    public void testStmt2BindExecProtocolExists() {
        Action bindExecAction = Action.STMT2_BIND_EXEC;
        assertNotNull("STMT2_BIND_EXEC action should exist in protocol", bindExecAction);
        assertEquals("Action name should be stmt2_bind_exec",
                "stmt2_bind_exec", bindExecAction.getAction());
        
        // Verify it returns the same response type as STMT2_EXEC
        assertEquals("STMT2_BIND_EXEC should use same response class as STMT2_EXEC",
                Action.STMT2_EXEC.getResponseClazz(),
                bindExecAction.getResponseClazz());
        
    }

    /**
     * Test that VersionUtil.supportStmt2BindExec exists for capability checking.
     * This is part of the plumbing but should not activate the feature yet.
     */
    @Test
    public void testVersionCheckMethodExists() throws Exception {
        java.lang.reflect.Method method = com.taosdata.jdbc.utils.VersionUtil.class
                .getMethod("supportStmt2BindExec", String.class);
        assertNotNull("supportStmt2BindExec method should exist", method);
        assertEquals("supportStmt2BindExec should return boolean",
                boolean.class, method.getReturnType());
        
        // Verify the method works correctly
        assertTrue("Version 3.4.1.13 should support bind-exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.13"));
        assertFalse("Version 3.4.1.12 should not support bind-exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.12"));
    }

    /**
     * Test that the legacy code path (useBindExec=false) would use STMT2_BIND action.
     * This verifies the constant and logic exist even though we can't execute it here.
     */
    @Test
    public void testLegacyPathConstantsExist() {
        // Verify the legacy protocol actions exist
        Action bindAction = Action.STMT2_BIND;
        Action execAction = Action.STMT2_EXEC;
        
        assertNotNull("STMT2_BIND action should exist", bindAction);
        assertNotNull("STMT2_EXEC action should exist", execAction);
        
        assertEquals("STMT2_BIND action name", "stmt2_bind", bindAction.getAction());
        assertEquals("STMT2_EXEC action name", "stmt2_exec", execAction.getAction());
        
        // Verify these are distinct from STMT2_BIND_EXEC
        assertNotEquals("STMT2_BIND should differ from STMT2_BIND_EXEC",
                bindAction, Action.STMT2_BIND_EXEC);
        assertNotEquals("STMT2_EXEC should differ from STMT2_BIND_EXEC",
                execAction, Action.STMT2_BIND_EXEC);
    }

    private static ConnectionParam mockParam() {
        ConnectionParam param = mock(ConnectionParam.class);
        when(param.isEnableAutoConnect()).thenReturn(false);
        when(param.getRetryTimes()).thenReturn(1);
        when(param.getRequestTimeout()).thenReturn(5000);
        when(param.getZoneId()).thenReturn(ZoneId.systemDefault());
        return param;
    }

    private static WSRetryableStmt newStmt(WSConnection connection, ConnectionParam param,
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

    private static ByteBuf newRawBlock() {
        ByteBuf rawBlock = Unpooled.buffer(32);
        rawBlock.writeLongLE(0L);
        rawBlock.writeLongLE(12345L);
        rawBlock.writeIntLE(1);
        return rawBlock;
    }

    private static void releaseIfNeeded(ByteBuf buffer) {
        if (buffer.refCnt() > 0) {
            buffer.release();
        }
    }
}
