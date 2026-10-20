/*
 * Copyright (c) 2022 TAOS Data, Inc.
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

import com.taosdata.jdbc.ws.entity.Response;

import java.util.concurrent.CompletableFuture;

public class FutureResponse implements Comparable<FutureResponse> {
    private final String action;
    private final Long id;
    private final CompletableFuture<Response> future;
    private final long timestamp;

    public FutureResponse(String action, Long id, CompletableFuture<Response> future) {
        this.action = action;
        this.id = id;
        this.future = future;
        timestamp = System.nanoTime();
    }

    public String getAction() {
        return action;
    }

    public Long getId() {
        return id;
    }

    public CompletableFuture<Response> getFuture() {
        return future;
    }

    long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(FutureResponse fr) {
        long r = this.timestamp - fr.timestamp;
        if (r > 0) return 1;
        if (r < 0) return -1;
        return 0;
    }
}
