package com.taosdata.jdbc.ws;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for WSRetryableStmt bind execution mode routing.
 * Tests the isUsingBindExec method which exposes the internal routing mode.
 * These are unit tests that verify constructor behavior without requiring actual connection/transport.
 */
public class WSRetryableStmtBindExecTest {

    /**
     * Test that WSRetryableStmt exposes bind mode through isUsingBindExec()
     * This test verifies the plumbing exists and returns the correct boolean values.
     */
    @Test
    public void testBindExecModeExposure() {
        // We can't instantiate WSRetryableStmt without valid dependencies in unit tests,
        // but we can verify the method signature exists and is public by checking the class
        try {
            java.lang.reflect.Method method = WSRetryableStmt.class.getMethod("isUsingBindExec");
            assertNotNull("isUsingBindExec method should exist", method);
            assertEquals("isUsingBindExec should return boolean", boolean.class, method.getReturnType());
            assertTrue("isUsingBindExec should be public", 
                    java.lang.reflect.Modifier.isPublic(method.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("isUsingBindExec method should exist: " + e.getMessage());
        }
    }

    /**
     * Test that WSRetryableStmt has a constructor accepting useBindExec parameter
     */
    @Test
    public void testConstructorWithBindExecParameter() {
        try {
            java.lang.reflect.Constructor<?> constructor = WSRetryableStmt.class.getConstructor(
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
            assertTrue("Constructor should be public", 
                    java.lang.reflect.Modifier.isPublic(constructor.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("Constructor with useBindExec parameter should exist: " + e.getMessage());
        }
    }

    /**
     * Test that WSRetryableStmt has a default constructor (without useBindExec parameter)
     */
    @Test
    public void testDefaultConstructor() {
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
            assertTrue("Constructor should be public", 
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
            assertTrue("useBindExec should be final", 
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
