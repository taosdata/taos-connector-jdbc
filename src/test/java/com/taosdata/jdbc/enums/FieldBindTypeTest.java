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

package com.taosdata.jdbc.enums;

import org.junit.Test;

import static org.junit.Assert.*;

public class FieldBindTypeTest {

    @Test
    public void testGetValue() {
        assertEquals(1, FieldBindType.TAOS_FIELD_COL.getValue());
        assertEquals(2, FieldBindType.TAOS_FIELD_TAG.getValue());
        assertEquals(3, FieldBindType.TAOS_FIELD_QUERY.getValue());
        assertEquals(4, FieldBindType.TAOS_FIELD_TBNAME.getValue());
    }

    @Test
    public void testFromValue() {
        assertEquals(FieldBindType.TAOS_FIELD_COL, FieldBindType.fromValue(1));
        assertEquals(FieldBindType.TAOS_FIELD_TAG, FieldBindType.fromValue(2));
        assertEquals(FieldBindType.TAOS_FIELD_QUERY, FieldBindType.fromValue(3));
        assertEquals(FieldBindType.TAOS_FIELD_TBNAME, FieldBindType.fromValue(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromValue_InvalidValue() {
        FieldBindType.fromValue(99);
    }
}