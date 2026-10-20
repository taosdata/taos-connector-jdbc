/*
 * Copyright (c) 2024 TAOS Data, Inc.
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

package com.taosdata.jdbc.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;

public class JsonUtilTest {
    private static final ObjectMapper objectMapper = JsonUtil.getObjectMapper();

    @Test
    public void testIgnoreUnknownProperties() throws Exception {
        String json = "{\"knownProperty\":\"value\", \"unknownProperty\":\"value\"}";
        TestKnownPropertyClass result = objectMapper.readValue(json, TestKnownPropertyClass.class);
        Assert.assertEquals("value", result.knownProperty);
    }

    @Test
    public void testSerializationWithoutNull() throws Exception {
        TestNullPropertyClass testClass = new TestNullPropertyClass();
        testClass.name = "John";
        testClass.age = null;
        String json = objectMapper.writeValueAsString(testClass);
        Assert.assertFalse(json.contains("age"));
    }

    @Test
    public void testDateFormat() throws Exception {
        TestClassWithDate testClass = new TestClassWithDate();
        testClass.date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2023-10-01 12:00:00");
        String json = objectMapper.writeValueAsString(testClass);
        Assert.assertTrue(json.contains("\"date\":\"2023-10-01 12:00:00\""));
    }

    @Test
    public void testJavaTimeModule() throws Exception {
        String json = "{\"localDateTime\":\"2023-10-01T12:00:00\"}";
        TestClassWithJavaTime result = objectMapper.readValue(json, TestClassWithJavaTime.class);
        Assert.assertEquals("2023-10-01T12:00", result.localDateTime.toString());
    }

    // 测试类
    static class TestKnownPropertyClass {
        @JsonProperty("knownProperty")
        public String knownProperty;
    }

    static class TestClassWithDate {
        @JsonProperty("date")
        public java.util.Date date;
    }

    static class TestClassWithArray {
        @JsonProperty("values")
        public String[] values;
    }

    static class TestClassWithJavaTime {
        @JsonProperty("localDateTime")
        public java.time.LocalDateTime localDateTime;
    }
    static class TestNullPropertyClass {
        @JsonProperty("name")
        public String name;
        @JsonProperty("age")
        public Integer age;
    }
}