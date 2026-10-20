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

public class CodeTest {

    // Test constants
    private static final int SUCCESS_CODE = 0;
    private static final String SUCCESS_MESSAGE = "success";
    private static final int INVALID_CODE = 1;
    private static final int NEGATIVE_INVALID_CODE = -1;

    /**
     * Test SUCCESS enum constant properties (code and message)
     */
    @Test
    public void testSuccessEnumConstant() {
        Code success = Code.SUCCESS;

        // Verify code value
        assertEquals(SUCCESS_CODE, success.getCode());
        // Verify message value
        assertEquals(SUCCESS_MESSAGE, success.getMessage());
    }

    /**
     * Test of() method with valid code
     */
    @Test
    public void testOfWithValidCode() {
        Code result = Code.of(SUCCESS_CODE);
        // Should return SUCCESS enum constant
        assertEquals(Code.SUCCESS, result);
    }

    /**
     * Test of() method with invalid positive code
     */
    @Test
    public void testOfWithInvalidPositiveCode() {
        Code result = Code.of(INVALID_CODE);
        // Should return null for non-existent code
        assertNull(result);
    }

    /**
     * Test of() method with invalid negative code
     */
    @Test
    public void testOfWithInvalidNegativeCode() {
        Code result = Code.of(NEGATIVE_INVALID_CODE);
        // Should return null for non-existent code
        assertNull(result);
    }

    /**
     * Test enum basic functionality (values() and valueOf())
     * Ensure enum structure is intact
     */
    @Test
    public void testEnumBasicFunctionality() {
        // Verify values() returns only SUCCESS (since it's the only constant)
        Code[] allCodes = Code.values();
        assertEquals(1, allCodes.length);
        assertEquals(Code.SUCCESS, allCodes[0]);

        // Verify valueOf() works for SUCCESS
        Code success = Code.valueOf("SUCCESS");
        assertEquals(Code.SUCCESS, success);

        // Verify valueOf() throws IllegalArgumentException for invalid name (edge case)
        assertThrows(IllegalArgumentException.class, () -> Code.valueOf("FAILURE"));
    }
}