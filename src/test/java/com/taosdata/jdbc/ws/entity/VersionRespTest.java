/*
 * Copyright (c) 2025 TAOS Data, Inc.
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

package com.taosdata.jdbc.ws.entity;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Minimal JUnit4 test for VersionResp (no mock, JDK8 compatible)
 */
public class VersionRespTest {

    // Basic test data
    private static final String TEST_VERSION = "3.0.0";

    @Test
    public void testVersionGetterAndSetter() {
        // 1. Initialize VersionResp (default constructor)
        VersionResp versionResp = new VersionResp();

        // 2. Test setter & getter with normal version string
        versionResp.setVersion(TEST_VERSION);
        assertEquals(TEST_VERSION, versionResp.getVersion());

        // 3. Test edge case: null version
        versionResp.setVersion(null);
        assertNull(versionResp.getVersion());

        // 4. Test edge case: empty version string
        versionResp.setVersion("");
        assertEquals("", versionResp.getVersion());
    }
}