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

package com.taosdata.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class WrapperImplTest {

    static class TestWrapperImpl extends WrapperImpl {
        // Test subclass
    }

    @Test
    public void testUnwrapSuccess() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        TestWrapperImpl unwrapped = wrapper.unwrap(TestWrapperImpl.class);
        Assert.assertNotNull(unwrapped);
        Assert.assertSame(wrapper, unwrapped);
    }

    @Test
    public void testUnwrapToSameClass() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        WrapperImpl unwrapped = wrapper.unwrap(WrapperImpl.class);
        Assert.assertNotNull(unwrapped);
        Assert.assertSame(wrapper, unwrapped);
    }

    @Test(expected = SQLException.class)
    public void testUnwrapFailure() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        wrapper.unwrap(String.class);
    }

    @Test
    public void testUnwrapToObject() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Object unwrapped = wrapper.unwrap(Object.class);
        Assert.assertNotNull(unwrapped);
        Assert.assertSame(wrapper, unwrapped);
    }

    @Test
    public void testIsWrapperForTrue() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertTrue(wrapper.isWrapperFor(TestWrapperImpl.class));
    }

    @Test
    public void testIsWrapperForParentClass() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertTrue(wrapper.isWrapperFor(WrapperImpl.class));
    }

    @Test
    public void testIsWrapperForObject() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertTrue(wrapper.isWrapperFor(Object.class));
    }

    @Test
    public void testIsWrapperForFalse() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertFalse(wrapper.isWrapperFor(String.class));
    }

    @Test
    public void testIsWrapperForDifferentInterface() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertFalse(wrapper.isWrapperFor(Runnable.class));
    }
}
