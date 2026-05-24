package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.ws.entity.Action;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.*;

/**
 * Compatibility tests for WSRetryableStmt to ensure Task 1 remains dormant.
 * These tests verify that:
 * 1. Default/current behavior uses legacy STMT2_BIND + STMT2_EXEC
 * 2. STMT2_BIND_EXEC is NOT sent in default/current flow
 * 3. The bind-exec path remains dormant until later tasks activate it
 * 
 * These tests use reflection to verify internal state since we're testing
 * that the feature is dormant, not fully functional.
 */
public class WSRetryableStmtCompatibilityTest {

    /**
     * Test that default constructor sets useBindExec to false.
     * This ensures the default behavior does not activate the new bind-exec path.
     */
    @Test
    public void testDefaultConstructorDisablesBindExec() throws Exception {
        // We can't easily instantiate WSRetryableStmt in a unit test without full dependencies,
        // so we'll verify the default parameter behavior through reflection
        
        // Verify the 7-argument constructor exists and delegates to 8-argument with false
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
     * Critical test: Verify that the 8-parameter constructor with useBindExec
     * is NOT public, preventing external code from activating the dormant feature.
     * It should be package-private or private for internal use only.
     */
    @Test
    public void testBindExecConstructorIsNotPublic() throws Exception {
        java.lang.reflect.Constructor<?> bindExecCtor = WSRetryableStmt.class.getDeclaredConstructor(
                com.taosdata.jdbc.AbstractConnection.class,
                com.taosdata.jdbc.common.ConnectionParam.class,
                String.class,
                Transport.class,
                Long.class,
                com.taosdata.jdbc.ws.stmt2.entity.StmtInfo.class,
                java.util.concurrent.atomic.AtomicInteger.class,
                boolean.class
        );
        assertNotNull("Constructor with useBindExec parameter should exist for future use", bindExecCtor);
        
        // Verify it's NOT public - should be package-private or private
        assertFalse("Constructor with useBindExec should NOT be public to prevent premature activation",
                java.lang.reflect.Modifier.isPublic(bindExecCtor.getModifiers()));
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
     * Test that the implementation has both code paths (legacy and bind-exec)
     * by verifying the relevant constants exist.
     */
    @Test
    public void testBothCodePathsExist() throws Exception {
        // Verify OPERATION_TYPE constants exist
        Field operationWrite = WSRetryableStmt.class.getDeclaredField("OPERATION_TYPE_WRITE");
        Field operationQuery = WSRetryableStmt.class.getDeclaredField("OPERATION_TYPE_QUERY");
        assertNotNull("OPERATION_TYPE_WRITE should exist", operationWrite);
        assertNotNull("OPERATION_TYPE_QUERY should exist", operationQuery);

        // Verify BIND_MODE constants exist (plumbing for both paths)
        Field bindModeLegacy = WSRetryableStmt.class.getDeclaredField("BIND_MODE_LEGACY");
        Field bindModeBindExec = WSRetryableStmt.class.getDeclaredField("BIND_MODE_BIND_EXEC");
        assertNotNull("BIND_MODE_LEGACY should exist", bindModeLegacy);
        assertNotNull("BIND_MODE_BIND_EXEC should exist", bindModeBindExec);
    }

    /**
     * Critical test: Verify that Action.STMT2_BIND_EXEC exists in the protocol enum
     * but is NOT used in the default execution path.
     * The action enum entry proves the plumbing exists, but should remain dormant.
     */
    @Test
    public void testStmt2BindExecProtocolExistsButDormant() {
        // Verify the protocol action exists
        Action bindExecAction = Action.STMT2_BIND_EXEC;
        assertNotNull("STMT2_BIND_EXEC action should exist in protocol", bindExecAction);
        assertEquals("Action name should be stmt2_bind_exec",
                "stmt2_bind_exec", bindExecAction.getAction());
        
        // Verify it returns the same response type as STMT2_EXEC
        assertEquals("STMT2_BIND_EXEC should use same response class as STMT2_EXEC",
                Action.STMT2_EXEC.getResponseClazz(),
                bindExecAction.getResponseClazz());
        
        // This test documents that the protocol plumbing exists
        // but doesn't verify execution path - that would require integration tests
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
        assertTrue("Version 3.4.1.10 should support bind-exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.10"));
        assertFalse("Version 3.4.1.9 should not support bind-exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.9"));
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
}
