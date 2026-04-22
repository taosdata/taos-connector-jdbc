package com.taosdata.jdbc.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for VersionUtil.supportStmt2BindExec capability check.
 * Verifies version gating logic around minimum version 3.1.4.10.
 */
public class VersionUtilStmt2BindExecTest {

    /**
     * Test that version exactly at minimum (3.1.4.10) is supported
     */
    @Test
    public void testMinimumVersionSupported() {
        assertTrue("Version 3.1.4.10 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.4.10"));
    }

    /**
     * Test that version above minimum is supported
     */
    @Test
    public void testAboveMinimumVersionSupported() {
        assertTrue("Version 3.1.4.11 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.4.11"));
        assertTrue("Version 3.1.5.0 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.5.0"));
        assertTrue("Version 3.2.0.0 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.2.0.0"));
        assertTrue("Version 4.0.0.0 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("4.0.0.0"));
    }

    /**
     * Test that version below minimum is not supported
     */
    @Test
    public void testBelowMinimumVersionNotSupported() {
        assertFalse("Version 3.1.4.9 should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.4.9"));
        assertFalse("Version 3.1.4.0 should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.4.0"));
        assertFalse("Version 3.1.3.10 should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.3.10"));
        assertFalse("Version 3.0.0.0 should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.0.0.0"));
        assertFalse("Version 2.9.9.9 should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("2.9.9.9"));
    }

    /**
     * Test that null version is not supported
     */
    @Test
    public void testNullVersionNotSupported() {
        assertFalse("Null version should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec(null));
    }

    /**
     * Test that empty string version is not supported
     */
    @Test
    public void testEmptyVersionNotSupported() {
        assertFalse("Empty version should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec(""));
    }

    /**
     * Test that invalid version format returns false (not exception)
     */
    @Test
    public void testInvalidVersionFormat() {
        assertFalse("Invalid version format should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("invalid"));
        assertFalse("Malformed version should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.x.y.z"));
    }

    /**
     * Test version with alpha/beta suffix below minimum
     */
    @Test
    public void testVersionWithSuffixBelowMinimum() {
        assertFalse("Version 3.1.4.9.alpha should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.4.9.alpha"));
        assertFalse("Version 3.1.3.10.beta should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.3.10.beta"));
    }

    /**
     * Test version with alpha/beta suffix at or above minimum
     * Note: According to VersionUtil logic, "3.1.4.10.alpha" is considered LESS than "3.1.4.10"
     * because alpha suffix indicates a pre-release version
     */
    @Test
    public void testVersionWithSuffixAtOrAboveMinimum() {
        // Alpha versions at exact minimum are considered lower (pre-release)
        assertFalse("Version 3.1.4.10.alpha should not support stmt2_bind_exec (pre-release)",
                VersionUtil.supportStmt2BindExec("3.1.4.10.alpha"));
        
        // Versions above minimum with suffix should support
        assertTrue("Version 3.1.4.11.alpha should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.4.11.alpha"));
        assertTrue("Version 3.1.4.11.beta should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.4.11.beta"));
    }

    /**
     * Test edge case: version 3.1.4.10 with different separators
     */
    @Test
    public void testVersionWithDifferentSeparators() {
        // Version comparison should handle both . and - as separators
        assertTrue("Version 3.1.4-10 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.1.4-10"));
    }
}
