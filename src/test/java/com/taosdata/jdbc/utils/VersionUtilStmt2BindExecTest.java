package com.taosdata.jdbc.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for VersionUtil.supportStmt2BindExec capability check.
 * Verifies version gating logic around minimum version 3.4.1.13.
 */
public class VersionUtilStmt2BindExecTest {

    /**
     * Test that version exactly at minimum (3.4.1.13) is supported
     */
    @Test
    public void testMinimumVersionSupported() {
        assertTrue("Version 3.4.1.13 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.1.13"));
    }

    /**
     * Test that version above minimum is supported
     */
    @Test
    public void testAboveMinimumVersionSupported() {
        assertTrue("Version 3.4.1.14 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.1.14"));
        assertTrue("Version 3.4.2.0 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.2.0"));
        assertTrue("Version 3.5.0.0 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.5.0.0"));
        assertTrue("Version 4.0.0.0 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("4.0.0.0"));
    }

    /**
     * Test that version below minimum is not supported
     */
    @Test
    public void testBelowMinimumVersionNotSupported() {
        assertFalse("Version 3.4.1.12 should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.1.12"));
        assertFalse("Version 3.4.1.0 should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.1.0"));
        assertFalse("Version 3.4.0.13 should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.0.13"));
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
        assertFalse("Version 3.4.1.12.alpha should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.1.12.alpha"));
        assertFalse("Version 3.4.0.13.beta should not support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.0.13.beta"));
    }

    /**
     * Test version with alpha/beta suffix at or above minimum
     * Note: According to VersionUtil logic, "3.4.1.13.alpha" is considered LESS than "3.4.1.13"
     * because alpha suffix indicates a pre-release version
     */
    @Test
    public void testVersionWithSuffixAtOrAboveMinimum() {
        // Alpha versions at exact minimum are considered lower (pre-release)
        assertFalse("Version 3.4.1.13.alpha should not support stmt2_bind_exec (pre-release)",
                VersionUtil.supportStmt2BindExec("3.4.1.13.alpha"));
        
        // Versions above minimum with suffix should support
        assertTrue("Version 3.4.1.14.alpha should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.1.14.alpha"));
        assertTrue("Version 3.4.1.14.beta should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.1.14.beta"));
    }

    /**
     * Test edge case: version 3.4.1.13 with different separators
     */
    @Test
    public void testVersionWithDifferentSeparators() {
        // Version comparison should handle both . and - as separators
        assertTrue("Version 3.4.1-13 should support stmt2_bind_exec",
                VersionUtil.supportStmt2BindExec("3.4.1-13"));
    }
}
