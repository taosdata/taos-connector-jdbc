/*
 * Copyright (c) 2021 TAOS Data, Inc.
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

package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionUtilQueryNsTest {

    @Test
    public void supportQueryNs_acceptsMinimumVersion() {
        assertTrue(VersionUtil.supportQueryNs(TSDBConstants.MIN_QUERY_NS_VERSION));
    }

    @Test
    public void supportQueryNs_acceptsVersionsAboveMinimum() {
        assertTrue(VersionUtil.supportQueryNs("3.4.1.12"));
        assertTrue(VersionUtil.supportQueryNs("3.4.1.14.alpha.community"));
        assertTrue(VersionUtil.supportQueryNs("3.4.2.0"));
        assertTrue(VersionUtil.supportQueryNs("4.0.0.0"));
    }

    @Test
    public void supportQueryNs_rejectsVersionsBelowMinimum() {
        assertFalse(VersionUtil.supportQueryNs("3.4.1.10"));
        assertFalse(VersionUtil.supportQueryNs("3.4.0.99"));
        assertFalse(VersionUtil.supportQueryNs("3.3.6.0"));
    }

    @Test
    public void supportQueryNs_rejectsUnknownOrInvalidVersions() {
        assertFalse(VersionUtil.supportQueryNs(null));
        assertFalse(VersionUtil.supportQueryNs(""));
        assertFalse(VersionUtil.supportQueryNs(TSDBConstants.UNKNOWN_VERSION));
        assertFalse(VersionUtil.supportQueryNs("3.x.y.z"));
    }
}
