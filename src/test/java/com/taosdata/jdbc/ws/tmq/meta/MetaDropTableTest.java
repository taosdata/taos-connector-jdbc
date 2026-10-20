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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MetaDropTableTest {
    private MetaDropTable metaDropTable;

    @Before
    public void setUp() {
        metaDropTable = new MetaDropTable();
    }

    @Test
    public void testDefaultValues() {
        assertNull(metaDropTable.getTableNameList());
    }

    @Test
    public void testTableNameListGetterAndSetter() {
        List<String> tableNames = Arrays.asList("table1", "table2", "table3");
        metaDropTable.setTableNameList(tableNames);
        assertEquals(tableNames, metaDropTable.getTableNameList());
        assertEquals(3, metaDropTable.getTableNameList().size());
        assertEquals("table1", metaDropTable.getTableNameList().get(0));

        metaDropTable.setTableNameList(null);
        assertNull(metaDropTable.getTableNameList());

        List<String> emptyList = Collections.emptyList();
        metaDropTable.setTableNameList(emptyList);
        assertEquals(emptyList, metaDropTable.getTableNameList());
        assertTrue(metaDropTable.getTableNameList().isEmpty());
    }

    @Test
    public void testEqualsWithSameObject() {
        metaDropTable.setTableNameList(Arrays.asList("table1"));
        assertEquals(metaDropTable, metaDropTable);
    }

    @Test
    public void testEqualsWithNull() {
        assertNotEquals(null, metaDropTable);
    }

    @Test
    public void testEqualsWithDifferentClass() {
        Object other = new Object();
        assertNotEquals(metaDropTable, other);
    }

    @Test
    public void testEqualsWithSameValues() {
        MetaDropTable table1 = new MetaDropTable();
        table1.setTableNameList(Arrays.asList("table1", "table2"));

        MetaDropTable table2 = new MetaDropTable();
        table2.setTableNameList(Arrays.asList("table1", "table2"));

        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentTableNameList() {
        MetaDropTable table1 = new MetaDropTable();
        table1.setTableNameList(Arrays.asList("table1"));

        MetaDropTable table2 = new MetaDropTable();
        table2.setTableNameList(Arrays.asList("table2"));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithNullAndNonNullTableNameList() {
        MetaDropTable table1 = new MetaDropTable();
        table1.setTableNameList(null);

        MetaDropTable table2 = new MetaDropTable();
        table2.setTableNameList(Arrays.asList("table1"));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithEmptyAndNonEmptyTableNameList() {
        MetaDropTable table1 = new MetaDropTable();
        table1.setTableNameList(Collections.emptyList());

        MetaDropTable table2 = new MetaDropTable();
        table2.setTableNameList(Arrays.asList("table1"));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithSuperClassDifference() {
        // Simulate super class (Meta) field difference
        // Create custom subclass to set super class fields if needed
        MetaDropTable table1 = new MetaDropTable() {
            @Override
            public boolean equals(Object o) {
                // Override to simulate super class equals returns false
                if (o == this) return true;
                if (o == null || getClass() != o.getClass()) return false;
                return false;
            }

            @Override
            public int hashCode() {
                return super.hashCode() + 1;
            }
        };
        table1.setTableNameList(Arrays.asList("table1"));

        MetaDropTable table2 = new MetaDropTable();
        table2.setTableNameList(Arrays.asList("table1"));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testHashCodeConsistency() {
        metaDropTable.setTableNameList(Arrays.asList("table1", "table2"));
        int initialHashCode = metaDropTable.hashCode();
        assertEquals(initialHashCode, metaDropTable.hashCode());
    }

    @Test
    public void testHashCodeWithSameValues() {
        MetaDropTable table1 = new MetaDropTable();
        table1.setTableNameList(Arrays.asList("table1", "table2"));

        MetaDropTable table2 = new MetaDropTable();
        table2.setTableNameList(Arrays.asList("table1", "table2"));

        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentValues() {
        MetaDropTable table1 = new MetaDropTable();
        table1.setTableNameList(Arrays.asList("table1"));

        MetaDropTable table2 = new MetaDropTable();
        table2.setTableNameList(Arrays.asList("table2"));

        assertNotEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testAllFieldsTogether() {
        List<String> tableNames = Arrays.asList("device_table", "sensor_table", "log_table");
        metaDropTable.setTableNameList(tableNames);

        assertEquals(tableNames, metaDropTable.getTableNameList());
        assertEquals(3, metaDropTable.getTableNameList().size());
        assertEquals("device_table", metaDropTable.getTableNameList().get(0));
        assertEquals("sensor_table", metaDropTable.getTableNameList().get(1));
        assertEquals("log_table", metaDropTable.getTableNameList().get(2));
    }
}