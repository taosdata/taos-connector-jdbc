package com.taosdata.jdbc.ws;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for WSRetryableStmt bind execution mode routing.
 * These tests verify the internal plumbing exists for future bind-exec support
 * while ensuring it remains dormant (not publicly activatable) in Task 1.
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
     * Test that WSRetryableStmt has a package-private constructor accepting useBindExec parameter.
     * This preserves plumbing for future use while preventing public activation.
     */
    @Test
    public void testInternalConstructorWithBindExecParameter() {
        try {
            java.lang.reflect.Constructor<?> constructor = WSRetryableStmt.class.getDeclaredConstructor(
                    com.taosdata.jdbc.AbstractConnection.class,
                    com.taosdata.jdbc.common.ConnectionParam.class,
                    String.class,
                    Transport.class,
                    Long.class,
                    com.taosdata.jdbc.ws.stmt2.entity.StmtInfo.class,
                    java.util.concurrent.atomic.AtomicInteger.class,
                    boolean.class
            );
            assertNotNull("Constructor with useBindExec parameter should exist", constructor);
            assertFalse("Constructor with useBindExec should NOT be public (dormant feature)", 
                    java.lang.reflect.Modifier.isPublic(constructor.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("Constructor with useBindExec parameter should exist: " + e.getMessage());
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
     * Test that both BIND_MODE constants exist
     */
    @Test
    public void testBindModeConstantsExist() {
        try {
            java.lang.reflect.Field legacyMode = WSRetryableStmt.class.getDeclaredField("BIND_MODE_LEGACY");
            java.lang.reflect.Field bindExecMode = WSRetryableStmt.class.getDeclaredField("BIND_MODE_BIND_EXEC");
            
            assertNotNull("BIND_MODE_LEGACY should exist", legacyMode);
            assertNotNull("BIND_MODE_BIND_EXEC should exist", bindExecMode);
            
            // Verify they are static final int
            assertTrue("BIND_MODE_LEGACY should be static", 
                    java.lang.reflect.Modifier.isStatic(legacyMode.getModifiers()));
            assertTrue("BIND_MODE_LEGACY should be final", 
                    java.lang.reflect.Modifier.isFinal(legacyMode.getModifiers()));
            
            assertTrue("BIND_MODE_BIND_EXEC should be static", 
                    java.lang.reflect.Modifier.isStatic(bindExecMode.getModifiers()));
            assertTrue("BIND_MODE_BIND_EXEC should be final", 
                    java.lang.reflect.Modifier.isFinal(bindExecMode.getModifiers()));
        } catch (NoSuchFieldException e) {
            fail("Bind mode constants should exist: " + e.getMessage());
        }
    }
}
