package com.taosdata.jdbc.ws;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Compatibility enforcement tests for WSRetryableStmt.
 * 
 * These tests verify the core compatibility logic through code inspection:
 * 1. Default constructor always uses legacy STMT2_BIND + STMT2_EXEC
 * 2. The internal bind-exec path is properly gated by server compatibility
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
     * Test that internal constructor with bind-exec parameter is NOT public.
     */
    @Test
    public void testBindExecConstructorIsPackagePrivate() throws Exception {
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
        assertNotNull("Bind-exec constructor should exist for internal use", bindExecCtor);
        assertFalse("Bind-exec constructor should NOT be public", 
                java.lang.reflect.Modifier.isPublic(bindExecCtor.getModifiers()));
    }

    /**
     * Code review test: Verify the constructor implementation includes server compatibility check.
     */
    @Test
    public void testConstructorEnforcesServerCompatibility() throws Exception {
        String sourceFile = "src/main/java/com/taosdata/jdbc/ws/WSRetryableStmt.java";
        java.nio.file.Path sourcePath = java.nio.file.Paths.get(sourceFile);
        
        if (!java.nio.file.Files.exists(sourcePath)) {
            fail("Source file not found: " + sourceFile);
        }
        
        String sourceCode = new String(java.nio.file.Files.readAllBytes(sourcePath));
        
        // Verify the constructor contains the compatibility check
        assertTrue("Constructor should check for WSConnection instance",
                sourceCode.contains("connection instanceof WSConnection"));
        assertTrue("Constructor should call supportsStmt2BindExec()",
                sourceCode.contains("supportsStmt2BindExec()"));
        
        // Verify the compatibility check is in the right constructor
        assertTrue("Compatibility check should be in 8-parameter constructor",
                sourceCode.contains("boolean useBindExec)") &&
                sourceCode.contains("wsConn.supportsStmt2BindExec()"));
    }

    /**
     * Test that the default constructor delegates with useBindExec=false.
     */
    @Test
    public void testDefaultConstructorDelegatesToInternalWithFalse() throws Exception {
        String sourceFile = "src/main/java/com/taosdata/jdbc/ws/WSRetryableStmt.java";
        java.nio.file.Path sourcePath = java.nio.file.Paths.get(sourceFile);
        
        if (!java.nio.file.Files.exists(sourcePath)) {
            fail("Source file not found: " + sourceFile);
        }
        
        String sourceCode = new String(java.nio.file.Files.readAllBytes(sourcePath));
        
        // Verify the default constructor calls the internal constructor with false
        assertTrue("Default constructor should delegate with useBindExec=false",
                sourceCode.contains("this(connection, param, database, transport, instanceId, stmtInfo, batchInsertedRows, false)"));
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
}
