/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for stmt2_bind_exec capability check via VersionUtil.
 * These tests verify the version gating logic without requiring a full WSConnection instance.
 */
public class WSConnectionStmt2BindExecTest {

    /**
     * Test that the configured minimum version supports stmt2_bind_exec.
     */
    @Test
    public void testMinimumVersionSupportsStmt2BindExec() {
        String minimumVersion = TSDBConstants.MIN_STMT2_BIND_EXEC_VERSION;
        boolean supports = com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec(minimumVersion);
        assertTrue("Version " + minimumVersion + " should support stmt2_bind_exec", supports);
    }

    /**
     * Test that version above minimum supports stmt2_bind_exec
     */
    @Test
    public void testAboveMinimumVersionSupportsStmt2BindExec() {
        assertTrue("Version 3.4.1.14 should support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.14"));
        assertTrue("Version 3.4.2.0 should support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.2.0"));
        assertTrue("Version 4.0.0.0 should support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("4.0.0.0"));
    }

    /**
     * Test that version below minimum does not support stmt2_bind_exec
     */
    @Test
    public void testBelowMinimumVersionDoesNotSupportStmt2BindExec() {
        assertFalse("Version 3.4.1.12 should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.12"));
        assertFalse("Version 3.4.0.10 should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.0.10"));
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
        assertTrue("Version 3.4.1.14.alpha should support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.14.alpha"));
    }

    /**
     * Test version with alpha suffix at minimum (pre-release, not supported)
     */
    @Test
    public void testVersionWithAlphaSuffixAtMinimum() {
        assertFalse("Version 3.4.1.13.alpha should not support stmt2_bind_exec (pre-release)",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.13.alpha"));
    }

    /**
     * Test version with alpha suffix below minimum
     */
    @Test
    public void testVersionWithAlphaSuffixBelowMinimum() {
        assertFalse("Version 3.4.1.12.alpha should not support stmt2_bind_exec",
                com.taosdata.jdbc.utils.VersionUtil.supportStmt2BindExec("3.4.1.12.alpha"));
    }
}
