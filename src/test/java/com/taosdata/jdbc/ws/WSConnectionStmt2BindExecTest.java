package com.taosdata.jdbc.ws;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for stmt2_bind_exec capability check via VersionUtil.
 * These tests verify the version gating logic without requiring a full WSConnection instance.
 */
public class WSConnectionStmt2BindExecTest {

    /**
     * Test that version 3.1.4.10 supports stmt2_bind_exec
     */
    @Test
    public void testMinimumVersionSupportsStmt2BindExec() {
        boolean supports = com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.1.4.10");
        assertTrue("Version 3.1.4.10 should support stmt2_bind_exec", supports);
    }

    /**
     * Test that version above minimum supports stmt2_bind_exec
     */
    @Test
    public void testAboveMinimumVersionSupportsStmt2BindExec() {
        assertTrue("Version 3.1.4.11 should support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.1.4.11"));
        assertTrue("Version 3.2.0.0 should support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.2.0.0"));
        assertTrue("Version 4.0.0.0 should support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("4.0.0.0"));
    }

    /**
     * Test that version below minimum does not support stmt2_bind_exec
     */
    @Test
    public void testBelowMinimumVersionDoesNotSupportStmt2BindExec() {
        assertFalse("Version 3.1.4.9 should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.1.4.9"));
        assertFalse("Version 3.1.3.10 should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.1.3.10"));
        assertFalse("Version 3.0.0.0 should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.0.0.0"));
    }

    /**
     * Test that null version does not support stmt2_bind_exec
     */
    @Test
    public void testNullVersionDoesNotSupportStmt2BindExec() {
        assertFalse("Null version should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec(null));
    }

    /**
     * Test that unknown version does not support stmt2_bind_exec
     */
    @Test
    public void testUnknownVersionDoesNotSupportStmt2BindExec() {
        assertFalse("Unknown version should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("unknown"));
    }

    /**
     * Test version with alpha suffix above minimum
     */
    @Test
    public void testVersionWithAlphaSuffixAboveMinimum() {
        assertTrue("Version 3.1.4.11.alpha should support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.1.4.11.alpha"));
    }

    /**
     * Test version with alpha suffix at minimum (pre-release, not supported)
     */
    @Test
    public void testVersionWithAlphaSuffixAtMinimum() {
        assertFalse("Version 3.1.4.10.alpha should not support stmt2_bind_exec (pre-release)",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.1.4.10.alpha"));
    }

    /**
     * Test version with alpha suffix below minimum
     */
    @Test
    public void testVersionWithAlphaSuffixBelowMinimum() {
        assertFalse("Version 3.1.4.9.alpha should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.1.4.9.alpha"));
    }
}
