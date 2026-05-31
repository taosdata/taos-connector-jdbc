package com.taosdata.jdbc.ws;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Reflection-level tests for WSRetryableStmt bind-exec eligibility plumbing.
 *
 * <p>The current design derives write-side bind-exec eligibility from the owning
 * {@link WSConnection} and no longer keeps a separate per-call bind-mode constructor.
 */
public class WSRetryableStmtBindExecTest {

    /**
     * Test that WSRetryableStmt has internal bind mode capability through isUsingBindExec()
     * This method should be package-private, not public, to keep the feature dormant.
     */
    @Test
    public void testBindExecModeInternalCapability() {
        try {
            java.lang.reflect.Method method = WSRetryableStmt.class.getDeclaredMethod("isUsingBindExec");
            assertNotNull("isUsingBindExec method should exist for internal testing", method);
            assertEquals("isUsingBindExec should return boolean", boolean.class, method.getReturnType());
            assertFalse("isUsingBindExec should NOT be public (dormant feature)", 
                    java.lang.reflect.Modifier.isPublic(method.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("isUsingBindExec method should exist: " + e.getMessage());
        }
    }

    /**
     * Test that the legacy per-call bind-exec constructor has been removed.
     */
    @Test
    public void testLegacyPerCallBindExecConstructorRemoved() {
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
        } catch (NoSuchMethodException e) {
            // Expected
        }
    }

    /**
     * Test that WSRetryableStmt has a public default constructor (without useBindExec parameter).
     * This is the constructor that production code should use.
     */
    @Test
    public void testPublicDefaultConstructor() {
        try {
            java.lang.reflect.Constructor<?> constructor = WSRetryableStmt.class.getConstructor(
                    com.taosdata.jdbc.AbstractConnection.class,
                    com.taosdata.jdbc.common.ConnectionParam.class,
                    String.class,
                    Transport.class,
                    Long.class,
                    com.taosdata.jdbc.ws.stmt2.entity.StmtInfo.class,
                    java.util.concurrent.atomic.AtomicInteger.class
            );
            assertNotNull("Default constructor (without useBindExec) should exist", constructor);
            assertTrue("Default constructor should be public for production use", 
                    java.lang.reflect.Modifier.isPublic(constructor.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("Default constructor should exist: " + e.getMessage());
        }
    }

    /**
     * Test that useBindExec field exists and is correctly typed
     */
    @Test
    public void testUseBindExecFieldExists() {
        try {
            java.lang.reflect.Field field = WSRetryableStmt.class.getDeclaredField("useBindExec");
            assertNotNull("useBindExec field should exist", field);
            assertEquals("useBindExec should be boolean type", boolean.class, field.getType());
            assertTrue("useBindExec should be final to prevent runtime modification", 
                    java.lang.reflect.Modifier.isFinal(field.getModifiers()));
        } catch (NoSuchFieldException e) {
            fail("useBindExec field should exist: " + e.getMessage());
        }
    }

    /**
     * Test that the legacy per-call bind mode constants have been removed.
     */
    @Test
    public void testLegacyBindModeConstantsRemoved() {
        try {
            WSRetryableStmt.class.getDeclaredField("BIND_MODE_LEGACY");
            fail("BIND_MODE_LEGACY should have been removed");
        } catch (NoSuchFieldException e) {
            // Expected
        }

        try {
            WSRetryableStmt.class.getDeclaredField("BIND_MODE_BIND_EXEC");
            fail("BIND_MODE_BIND_EXEC should have been removed");
        } catch (NoSuchFieldException e) {
            // Expected
        }
    }
}
