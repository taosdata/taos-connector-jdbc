package com.taosdata.jdbc.ws;

import org.junit.Test;

import static org.junit.Assert.*;

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

    /**
     * Code review test: verify the constructor derives write eligibility from websocket capability.
     */
    @Test
    public void testConstructorDerivesWriteEligibilityFromWSConnectionCapability() throws Exception {
        String sourceFile = "src/main/java/com/taosdata/jdbc/ws/WSRetryableStmt.java";
        java.nio.file.Path sourcePath = java.nio.file.Paths.get(sourceFile);
        
        if (!java.nio.file.Files.exists(sourcePath)) {
            fail("Source file not found: " + sourceFile);
        }
        
        String sourceCode = new String(java.nio.file.Files.readAllBytes(sourcePath));
        
        assertTrue("Constructor should derive write eligibility from WSConnection.supportsStmt2BindExec()",
                sourceCode.contains("this.useBindExec = ((WSConnection) connection).supportsStmt2BindExec();"));
    }

    /**
     * Query operations must stay on the legacy path even when write eligibility is enabled.
     */
    @Test
    public void testQueryPathIsSeparatedFromWriteBindExecGate() throws Exception {
        String sourceFile = "src/main/java/com/taosdata/jdbc/ws/WSRetryableStmt.java";
        java.nio.file.Path sourcePath = java.nio.file.Paths.get(sourceFile);
        
        if (!java.nio.file.Files.exists(sourcePath)) {
            fail("Source file not found: " + sourceFile);
        }
        
        String sourceCode = new String(java.nio.file.Files.readAllBytes(sourcePath));
        
        assertTrue("Bind-exec gate should apply only to write operations",
                sourceCode.contains("useBindExec && operationType == OPERATION_TYPE_WRITE"));
        assertTrue("queryWithRetry should execute with OPERATION_TYPE_QUERY",
                sourceCode.contains("executeWithRetry(rawBlock, OPERATION_TYPE_QUERY"));
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
