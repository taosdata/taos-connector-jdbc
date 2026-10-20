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

package com.taosdata.jdbc;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for VGroupIDResp class
 */
public class VGroupIDRespTest {

    private VGroupIDResp vGroupIDResp;

    /**
     * Initialize test instance before each test method
     */
    @Before
    public void setUp() {
        vGroupIDResp = new VGroupIDResp();
    }

    /**
     * Test default values of code and vgID
     */
    @Test
    public void testDefaultFieldValues() {
        assertEquals(0, vGroupIDResp.getCode());
        assertEquals(0, vGroupIDResp.getVgID());
    }

    /**
     * Test setCode() and getCode() methods with different values
     */
    @Test
    public void testSetAndGetCode() {
        // Test positive value
        int positiveCode = 200;
        vGroupIDResp.setCode(positiveCode);
        assertEquals(positiveCode, vGroupIDResp.getCode());

        // Test negative value
        int negativeCode = -50;
        vGroupIDResp.setCode(negativeCode);
        assertEquals(negativeCode, vGroupIDResp.getCode());

        // Test zero value
        int zeroCode = 0;
        vGroupIDResp.setCode(zeroCode);
        assertEquals(zeroCode, vGroupIDResp.getCode());
    }

    /**
     * Test setVgID() and getVgID() methods with different values
     */
    @Test
    public void testSetAndGetVgID() {
        // Test positive value
        int positiveVgID = 1000;
        vGroupIDResp.setVgID(positiveVgID);
        assertEquals(positiveVgID, vGroupIDResp.getVgID());

        // Test negative value
        int negativeVgID = -100;
        vGroupIDResp.setVgID(negativeVgID);
        assertEquals(negativeVgID, vGroupIDResp.getVgID());

        // Test zero value
        int zeroVgID = 0;
        vGroupIDResp.setVgID(zeroVgID);
        assertEquals(zeroVgID, vGroupIDResp.getVgID());
    }
}
