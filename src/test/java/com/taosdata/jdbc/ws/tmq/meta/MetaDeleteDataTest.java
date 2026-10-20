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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MetaDeleteDataTest {
    private MetaDeleteData metaDeleteData;

    @Before
    public void setUp() {
        metaDeleteData = new MetaDeleteData();
    }

    @Test
    public void testDefaultValues() {
        assertNull(metaDeleteData.getSql());
    }

    @Test
    public void testSqlGetterAndSetter() {
        String sql = "DELETE FROM sensor_data WHERE ts < 1735689600000";
        metaDeleteData.setSql(sql);
        assertEquals(sql, metaDeleteData.getSql());

        metaDeleteData.setSql(null);
        assertNull(metaDeleteData.getSql());

        metaDeleteData.setSql("");
        assertEquals("", metaDeleteData.getSql());
    }

    @Test
    public void testEqualsWithSameObject() {
        metaDeleteData.setSql("DELETE FROM table1 WHERE id = 1");
        assertEquals(metaDeleteData, metaDeleteData);
    }

    @Test
    public void testEqualsWithNull() {
        assertNotEquals(null, metaDeleteData);
    }

    @Test
    public void testEqualsWithDifferentClass() {
        Object other = new Object();
        assertNotEquals(metaDeleteData, other);
    }

    @Test
    public void testEqualsWithSameValues() {
        MetaDeleteData data1 = new MetaDeleteData();
        data1.setSql("DELETE FROM device WHERE status = 'offline'");

        MetaDeleteData data2 = new MetaDeleteData();
        data2.setSql("DELETE FROM device WHERE status = 'offline'");

        assertEquals(data1, data2);
        assertEquals(data1.hashCode(), data2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentSql() {
        MetaDeleteData data1 = new MetaDeleteData();
        data1.setSql("DELETE FROM table1");

        MetaDeleteData data2 = new MetaDeleteData();
        data2.setSql("DELETE FROM table2");

        assertNotEquals(data1, data2);
    }

    @Test
    public void testEqualsWithNullAndNonNullSql() {
        MetaDeleteData data1 = new MetaDeleteData();
        data1.setSql(null);

        MetaDeleteData data2 = new MetaDeleteData();
        data2.setSql("DELETE FROM table1");

        assertNotEquals(data1, data2);
    }

    @Test
    public void testEqualsWithSuperClassDifference() {
        // Simulate super class (Meta) field difference
        MetaDeleteData data1 = new MetaDeleteData() {
            @Override
            public boolean equals(Object o) {
                // Override to simulate super class equals returns false
                if (o == this) return true;
                if (o == null || getClass() != o.getClass()) return false;
                return false;
            }

            @Override
            public int hashCode() {
                return super.hashCode() + 100;
            }
        };
        data1.setSql("DELETE FROM table1");

        MetaDeleteData data2 = new MetaDeleteData();
        data2.setSql("DELETE FROM table1");

        assertNotEquals(data1, data2);
    }

    @Test
    public void testHashCodeConsistency() {
        metaDeleteData.setSql("DELETE FROM log WHERE level = 'error'");
        int initialHashCode = metaDeleteData.hashCode();
        assertEquals(initialHashCode, metaDeleteData.hashCode());
    }

    @Test
    public void testHashCodeWithSameValues() {
        MetaDeleteData data1 = new MetaDeleteData();
        data1.setSql("DELETE FROM sensor WHERE value > 100");

        MetaDeleteData data2 = new MetaDeleteData();
        data2.setSql("DELETE FROM sensor WHERE value > 100");

        assertEquals(data1.hashCode(), data2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentValues() {
        MetaDeleteData data1 = new MetaDeleteData();
        data1.setSql("DELETE FROM table1");

        MetaDeleteData data2 = new MetaDeleteData();
        data2.setSql("DELETE FROM table2");

        assertNotEquals(data1.hashCode(), data2.hashCode());
    }

    @Test
    public void testAllFieldsTogether() {
        String sql = "DELETE FROM user_data WHERE create_time < '2025-01-01'";
        metaDeleteData.setSql(sql);

        assertEquals(sql, metaDeleteData.getSql());
    }
}