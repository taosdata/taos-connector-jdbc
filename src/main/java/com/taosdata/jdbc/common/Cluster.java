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

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Cluster {
    // Immutable set: nodes cannot be modified after construction
    private final Set<Endpoint> endpoints;

    /**
     * Constructs a Cluster with variable endpoints.
     * @param endpoints the nodes that form the cluster
     */
    public Cluster(Endpoint... endpoints) {
        // Convert to immutable set to prevent external modification
        Set<Endpoint> temp = new HashSet<>();
        Collections.addAll(temp, endpoints);
        this.endpoints = Collections.unmodifiableSet(temp);
    }

    /**
     * Constructs a Cluster with a set of endpoints.
     * @param endpoints the nodes that form the cluster
     */
    public Cluster(Set<Endpoint> endpoints) {
        this.endpoints = Collections.unmodifiableSet(new HashSet<>(endpoints));
    }

    /**
     * Gets all nodes in the cluster (unmodifiable).
     * @return an immutable set of endpoints
     */
    public Set<Endpoint> getEndpoints() {
        return endpoints;
    }

    /**
     * Checks equality: two clusters are equal if they contain exactly the same endpoints (order-agnostic).
     * @param o the object to compare
     * @return true if endpoint sets are identical
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cluster cluster = (Cluster) o;
        return Objects.equals(endpoints, cluster.endpoints);
    }

    /**
     * Generates hash code based on the endpoint set.
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(endpoints);
    }

    @Override
    public String toString() {
        return "Cluster{endpoints=" + endpoints + "}";
    }
}