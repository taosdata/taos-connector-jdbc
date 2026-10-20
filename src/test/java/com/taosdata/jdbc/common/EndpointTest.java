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

package com.taosdata.jdbc.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class EndpointTest {

    @Test
    public void testConstructorAndGetters() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertEquals("127.0.0.1", endpoint.getHost());
        assertEquals(8080, endpoint.getPort());
        assertFalse(endpoint.isIpv6());
    }

    @Test
    public void testEquals_SameObject() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertEquals(endpoint, endpoint);
    }

    @Test
    public void testEquals_DifferentObjectSameValues() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("127.0.0.1", 8080, false);

        assertEquals(endpoint1, endpoint2);
        assertEquals(endpoint1.hashCode(), endpoint2.hashCode());
    }

    @Test
    public void testEquals_DifferentValues() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, true);

        assertNotEquals(endpoint1, endpoint2);
    }

    @Test
    public void testEquals_NullObject() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertNotNull(endpoint);
    }

    @Test
    public void testEquals_DifferentClass() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertNotEquals("Some String", endpoint);
    }

    @Test
    public void testHashCode() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("127.0.0.1", 8080, false);

        assertEquals(endpoint1.hashCode(), endpoint2.hashCode());
    }

    @Test
    public void testToString_Ipv4() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);

        assertEquals("127.0.0.1:8080", endpoint.toString());
    }

    @Test
    public void testToString_Ipv6() {
        Endpoint endpoint = new Endpoint("::1", 8080, true);

        assertEquals("[::1]:8080", endpoint.toString());
    }
}