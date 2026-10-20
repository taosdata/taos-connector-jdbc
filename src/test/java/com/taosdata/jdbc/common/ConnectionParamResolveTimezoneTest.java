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

package com.taosdata.jdbc.common;

import org.junit.Assume;
import org.junit.Test;

import java.sql.SQLException;
import java.time.ZoneId;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ConnectionParam#resolveTimezone(String)}
 */
public class ConnectionParamResolveTimezoneTest {

    @Test
    public void testValidIanaName() throws SQLException {
        ZoneId tokyo = ZoneId.of("Asia/Tokyo");
        if (ZoneId.systemDefault().equals(tokyo)) {
            assertNull(ConnectionParam.resolveTimezone("Asia/Tokyo"));
        } else {
            assertEquals(tokyo, ConnectionParam.resolveTimezone("Asia/Tokyo"));
        }
    }

    @Test
    public void testSystemDefaultResolvesToNull() throws SQLException {
        String systemDefaultId = ZoneId.systemDefault().getId();
        // only IANA region names are accepted; skip when the JVM default is not one
        Assume.assumeTrue(systemDefaultId.contains("/"));
        assertNull(ConnectionParam.resolveTimezone(systemDefaultId));
    }

    @Test
    public void testNullOrEmptyMeansNoTimezone() throws SQLException {
        assertNull(ConnectionParam.resolveTimezone(null));
        assertNull(ConnectionParam.resolveTimezone(""));
    }

    @Test
    public void testOffsetFormsRejected() {
        assertInvalidTimezone("+08:00");
        assertInvalidTimezone("UTC+8");
        assertInvalidTimezone("GMT-8");
        // not an IANA region name
        assertInvalidTimezone("UTC");
    }

    @Test
    public void testInvalidIanaNameRejected() {
        assertInvalidTimezone("Invalid/Zone");
    }

    private static void assertInvalidTimezone(String tz) {
        try {
            ConnectionParam.resolveTimezone(tz);
            fail("expected SQLException for timezone: " + tz);
        } catch (SQLException expected) {
            // expected
        }
    }
}
