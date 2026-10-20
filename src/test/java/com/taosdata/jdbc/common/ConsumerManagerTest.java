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

package com.taosdata.jdbc.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConsumerManagerTest {

    private StringWriter logOutput;

    @Before
    public void setUp() {
        logOutput = new StringWriter();
        ConsumerManager.setLogWriter(new PrintWriter(logOutput));
    }

    @After
    public void tearDown() {
        ConsumerManager.setLogWriter(null);
    }

    @Test
    public void testRegister() {
        ConsumerFactory mockFactory = mock(ConsumerFactory.class);
        ConsumerManager.register(mockFactory);

        assertTrue(logOutput.toString().contains("registerConsumerFactory"));
    }

    @Test(expected = NullPointerException.class)
    public void testRegister_NullFactory() {
        ConsumerManager.register(null);
    }
    @Test(expected = SQLException.class)
    public void testGetConsumer_NoSuitableConsumer() throws SQLException {
        ConsumerFactory mockFactory = mock(ConsumerFactory.class);
        when(mockFactory.acceptsType("testType")).thenReturn(false);

        ConsumerManager.register(mockFactory);
        ConsumerManager.getConsumer("testType");
    }

    @Test
    public void testGetLogWriter() {
        PrintWriter writer = ConsumerManager.getLogWriter();
        assertNotNull(writer);
    }

    @Test
    public void testSetLogWriter() {
        PrintWriter newWriter = new PrintWriter(new StringWriter());
        ConsumerManager.setLogWriter(newWriter);

        assertEquals(newWriter, ConsumerManager.getLogWriter());
    }

    @Test
    public void testPrintln() {
        ConsumerManager.println("Test message");
        assertTrue(logOutput.toString().contains("Test message"));
    }
}