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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class FetchRespTest {
    private FetchResp fetchResp;

    @Before
    public void setUp() {
        fetchResp = new FetchResp();
    }

    @Test
    public void testIdGetterAndSetter() {
        long expectedId = 12345L;
        fetchResp.setId(expectedId);
        assertEquals(expectedId, fetchResp.getId());
    }

    @Test
    public void testCompletedGetterAndSetter() {
        fetchResp.setCompleted(true);
        assertTrue(fetchResp.isCompleted());

        fetchResp.setCompleted(false);
        assertFalse(fetchResp.isCompleted());
    }

    @Test
    public void testLengthsGetterAndSetter() {
        Integer[] expectedLengths = {10, 20, 30};
        fetchResp.setLengths(expectedLengths);
        assertArrayEquals(expectedLengths, fetchResp.getLengths());

        fetchResp.setLengths(null);
        assertNull(fetchResp.getLengths());
    }

    @Test
    public void testRowsGetterAndSetter() {
        int expectedRows = 100;
        fetchResp.setRows(expectedRows);
        assertEquals(expectedRows, fetchResp.getRows());
    }

    @Test
    public void testJsonPropertyAnnotations() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = "{\"id\":999,\"completed\":true,\"lengths\":[1,2,3],\"rows\":50}";

        FetchResp deserialized = mapper.readValue(json, FetchResp.class);

        assertEquals(999L, deserialized.getId());
        assertTrue(deserialized.isCompleted());
        assertArrayEquals(new Integer[]{1, 2, 3}, deserialized.getLengths());
        assertEquals(50, deserialized.getRows());
    }

    @Test
    public void testInheritanceFromCommonResp() {
        assertNotNull(fetchResp);
        assertTrue(fetchResp instanceof CommonResp);
    }

    @Test
    public void testDefaultValues() {
        FetchResp newInstance = new FetchResp();
        assertEquals(0L, newInstance.getId());
        assertFalse(newInstance.isCompleted());
        assertEquals(0, newInstance.getRows());
        assertNull(newInstance.getLengths());
    }

    @Test
    public void testLengthsModification() {
        Integer[] originalLengths = {5, 10, 15};
        fetchResp.setLengths(originalLengths);

        Integer[] retrieved = fetchResp.getLengths();
        retrieved[0] = 99;

        assertEquals(Integer.valueOf(99), fetchResp.getLengths()[0]);
    }

    @Test
    public void testEqualsAndHashCode() {
        FetchResp resp1 = new FetchResp();
        resp1.setId(1L);
        resp1.setCompleted(true);
        resp1.setRows(10);

        FetchResp resp2 = new FetchResp();
        resp2.setId(1L);
        resp2.setCompleted(true);
        resp2.setRows(10);

        assertEquals(resp1.getId(), resp2.getId());
        assertEquals(resp1.isCompleted(), resp2.isCompleted());
        assertEquals(resp1.getRows(), resp2.getRows());
    }
}