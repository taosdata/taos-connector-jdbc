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
 * Minimal JUnit4 test for Request (no mock, JDK8 compatible)
 */
public class RequestTest {

    // Basic test data
    private static final String TEST_ACTION = "tmq_fetch";
    private static final Long TEST_REQ_ID = 987654L;

    @Test
    public void testBasicFunctionality() {
        // 1. Prepare Payload (real instance, no mock)
        Payload payload = new Payload();
        payload.setReqId(TEST_REQ_ID);

        // 2. Test constructor & getter
        Request request = new Request(TEST_ACTION, payload);
        assertEquals(TEST_ACTION, request.getAction());
        assertEquals(payload, request.getArgs());
        assertEquals(TEST_REQ_ID, request.id()); // Test id() method

        // 3. Test setter
        String newAction = "tmq_ack";
        Long newReqId = 111222L;
        Payload newPayload = new Payload();
        newPayload.setReqId(newReqId);

        request.setAction(newAction);
        request.setArgs(newPayload);
        assertEquals(newAction, request.getAction());
        assertEquals(newReqId, request.id()); // Verify id() after setter

        // 4. Simple test for toString() (non-empty)
        String requestStr = request.toString();
        assertFalse("toString() should return non-empty string", requestStr.isEmpty());
    }
}