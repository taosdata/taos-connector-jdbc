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

package com.taosdata.jdbc.ws.tmq.meta;

import org.junit.Test;

import static org.junit.Assert.*;

public class MetaTypeTest {

    @Test
    public void testFromStringWithValidUpperCaseValues() {
        assertEquals(MetaType.CREATE, MetaType.fromString("CREATE"));
        assertEquals(MetaType.DROP, MetaType.fromString("DROP"));
        assertEquals(MetaType.ALTER, MetaType.fromString("ALTER"));
        assertEquals(MetaType.DELETE, MetaType.fromString("DELETE"));
    }

    @Test
    public void testFromStringWithValidLowerCaseValues() {
        assertEquals(MetaType.CREATE, MetaType.fromString("create"));
        assertEquals(MetaType.DROP, MetaType.fromString("drop"));
        assertEquals(MetaType.ALTER, MetaType.fromString("alter"));
        assertEquals(MetaType.DELETE, MetaType.fromString("delete"));
    }

    @Test
    public void testFromStringWithValidMixedCaseValues() {
        assertEquals(MetaType.CREATE, MetaType.fromString("CrEaTe"));
        assertEquals(MetaType.DROP, MetaType.fromString("DrOp"));
        assertEquals(MetaType.ALTER, MetaType.fromString("AlTeR"));
        assertEquals(MetaType.DELETE, MetaType.fromString("DeLeTe"));
    }

    @Test
    public void testFromStringWithNullValue() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.fromString(null);
        });
        assertEquals("MetaType value cannot be null", exception.getMessage());
    }

    @Test
    public void testFromStringWithInvalidValue() {
        // Test non-existent value
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.fromString("UPDATE");
        });
        assertEquals("Invalid MetaType value: UPDATE", exception.getMessage());

        // Test empty string
        exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.fromString("");
        });
        assertEquals("Invalid MetaType value: ", exception.getMessage());

        // Test whitespace value
        exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.fromString(" CREATE ");
        });
        assertEquals("Invalid MetaType value:  CREATE ", exception.getMessage());
    }

    @Test
    public void testMatchesWithMatchingValue() {
        assertTrue(MetaType.CREATE.matches("CREATE"));
        assertTrue(MetaType.CREATE.matches("create"));
        assertTrue(MetaType.CREATE.matches("CrEaTe"));

        assertTrue(MetaType.DROP.matches("DROP"));
        assertTrue(MetaType.DROP.matches("drop"));

        assertTrue(MetaType.ALTER.matches("ALTER"));
        assertTrue(MetaType.ALTER.matches("alter"));

        assertTrue(MetaType.DELETE.matches("DELETE"));
        assertTrue(MetaType.DELETE.matches("delete"));
    }

    @Test
    public void testMatchesWithNonMatchingValue() {
        assertFalse(MetaType.CREATE.matches("DROP"));
        assertFalse(MetaType.DROP.matches("ALTER"));
        assertFalse(MetaType.ALTER.matches("DELETE"));
        assertFalse(MetaType.DELETE.matches("CREATE"));
    }

    @Test
    public void testMatchesWithNullValue() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.CREATE.matches(null);
        });
        assertEquals("MetaType value cannot be null", exception.getMessage());
    }

    @Test
    public void testEnumValuesCount() {
        // Ensure the enum has exactly 4 values (CREATE/DROP/ALTER/DELETE)
        assertEquals(4, MetaType.values().length);
    }
}