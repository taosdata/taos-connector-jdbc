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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class ClusterTest {

    @Test
    public void testConstructorWithVarargs() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, false);

        Cluster cluster = new Cluster(endpoint1, endpoint2);

        Set<Endpoint> expectedEndpoints = new HashSet<>();
        Collections.addAll(expectedEndpoints, endpoint1, endpoint2);

        assertEquals(expectedEndpoints, cluster.getEndpoints());
    }

    @Test
    public void testConstructorWithSet() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, false);

        Set<Endpoint> endpoints = new HashSet<>();
        Collections.addAll(endpoints, endpoint1, endpoint2);

        Cluster cluster = new Cluster(endpoints);

        assertEquals(endpoints, cluster.getEndpoints());
    }

    @Test
    public void testGetEndpoints() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);
        Cluster cluster = new Cluster(endpoint);

        Set<Endpoint> endpoints = cluster.getEndpoints();

        assertNotNull(endpoints);
        assertEquals(1, endpoints.size());
        assertTrue(endpoints.contains(endpoint));
    }

    @Test
    public void testEqualsAndHashCode() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, false);

        Cluster cluster1 = new Cluster(endpoint1, endpoint2);
        Cluster cluster2 = new Cluster(endpoint2, endpoint1);

        assertEquals(cluster1, cluster2);
        assertEquals(cluster1.hashCode(), cluster2.hashCode());
    }

    @Test
    public void testNotEquals() {
        Endpoint endpoint1 = new Endpoint("127.0.0.1", 8080, false);
        Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081, false);

        Cluster cluster1 = new Cluster(endpoint1);
        Cluster cluster2 = new Cluster(endpoint2);

        assertNotEquals(cluster1, cluster2);
    }

    @Test
    public void testToString() {
        Endpoint endpoint = new Endpoint("127.0.0.1", 8080, false);
        Cluster cluster = new Cluster(endpoint);

        String expected = "Cluster{endpoints=[" + endpoint.toString() + "]}";
        assertEquals(expected, cluster.toString());
    }
}