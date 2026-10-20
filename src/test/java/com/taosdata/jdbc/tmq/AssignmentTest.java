/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

package com.taosdata.jdbc.tmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class AssignmentTest {

    @Test
    public void testDefaultConstructor() {
        Assignment assignment = new Assignment();
        Assert.assertEquals(0, assignment.getVgId());
        Assert.assertEquals(0L, assignment.getCurrentOffset());
        Assert.assertEquals(0L, assignment.getBegin());
        Assert.assertEquals(0L, assignment.getEnd());
    }

    @Test
    public void testParameterizedConstructor() {
        int vgId = 1;
        long currentOffset = 100L;
        long begin = 0L;
        long end = 1000L;

        Assignment assignment = new Assignment(vgId, currentOffset, begin, end);
        Assert.assertEquals(vgId, assignment.getVgId());
        Assert.assertEquals(currentOffset, assignment.getCurrentOffset());
        Assert.assertEquals(begin, assignment.getBegin());
        Assert.assertEquals(end, assignment.getEnd());
    }

    @Test
    public void testSetVgId() {
        Assignment assignment = new Assignment();
        assignment.setVgId(5);
        Assert.assertEquals(5, assignment.getVgId());
    }

    @Test
    public void testSetCurrentOffset() {
        Assignment assignment = new Assignment();
        assignment.setCurrentOffset(200L);
        Assert.assertEquals(200L, assignment.getCurrentOffset());
    }

    @Test
    public void testSetBegin() {
        Assignment assignment = new Assignment();
        assignment.setBegin(50L);
        Assert.assertEquals(50L, assignment.getBegin());
    }

    @Test
    public void testSetEnd() {
        Assignment assignment = new Assignment();
        assignment.setEnd(500L);
        Assert.assertEquals(500L, assignment.getEnd());
    }

    @Test
    public void testSetAndGetAllFields() {
        Assignment assignment = new Assignment();
        assignment.setVgId(10);
        assignment.setCurrentOffset(300L);
        assignment.setBegin(100L);
        assignment.setEnd(1000L);

        Assert.assertEquals(10, assignment.getVgId());
        Assert.assertEquals(300L, assignment.getCurrentOffset());
        Assert.assertEquals(100L, assignment.getBegin());
        Assert.assertEquals(1000L, assignment.getEnd());
    }

    @Test
    public void testJsonSerialization() throws JsonProcessingException {
        Assignment assignment = new Assignment(1, 100L, 0L, 1000L);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(assignment);

        Assert.assertNotNull(json);
        Assert.assertTrue(json.contains("\"vgroup_id\":1"));
        Assert.assertTrue(json.contains("\"offset\":100"));
        Assert.assertTrue(json.contains("\"begin\":0"));
        Assert.assertTrue(json.contains("\"end\":1000"));
    }

    @Test
    public void testJsonDeserialization() throws Exception {
        String json = "{\"vgroup_id\":2,\"offset\":200,\"begin\":0,\"end\":2000}";
        ObjectMapper mapper = new ObjectMapper();
        Assignment assignment = mapper.readValue(json, Assignment.class);

        Assert.assertEquals(2, assignment.getVgId());
        Assert.assertEquals(200L, assignment.getCurrentOffset());
        Assert.assertEquals(0L, assignment.getBegin());
        Assert.assertEquals(2000L, assignment.getEnd());
    }

    @Test
    public void testNegativeValues() {
        Assignment assignment = new Assignment(-1, -100L, -1L, -1L);
        Assert.assertEquals(-1, assignment.getVgId());
        Assert.assertEquals(-100L, assignment.getCurrentOffset());
        Assert.assertEquals(-1L, assignment.getBegin());
        Assert.assertEquals(-1L, assignment.getEnd());
    }

    @Test
    public void testLargeValues() {
        Assignment assignment = new Assignment(100, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(100, assignment.getVgId());
        Assert.assertEquals(Long.MAX_VALUE, assignment.getCurrentOffset());
        Assert.assertEquals(Long.MIN_VALUE, assignment.getBegin());
        Assert.assertEquals(Long.MAX_VALUE, assignment.getEnd());
    }
}
