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

import org.junit.Assert;
import org.junit.Test;

public class OffsetAndMetadataTest {

    @Test
    public void testConstructorWithOffsetOnly() {
        long offset = 100L;
        OffsetAndMetadata oam = new OffsetAndMetadata(offset);
        Assert.assertEquals(offset, oam.offset());
        Assert.assertEquals("", oam.metadata());
    }

    @Test
    public void testConstructorWithOffsetAndMetadata() {
        long offset = 200L;
        String metadata = "test_metadata";
        OffsetAndMetadata oam = new OffsetAndMetadata(offset, metadata);
        Assert.assertEquals(offset, oam.offset());
        Assert.assertEquals("", oam.metadata());  // metadata is always set to empty string
    }

    @Test
    public void testOffsetZero() {
        OffsetAndMetadata oam = new OffsetAndMetadata(0L);
        Assert.assertEquals(0L, oam.offset());
    }

    @Test
    public void testOffsetPositive() {
        OffsetAndMetadata oam = new OffsetAndMetadata(1000L);
        Assert.assertEquals(1000L, oam.offset());
    }

    @Test
    public void testOffsetInvalidOffset() {
        OffsetAndMetadata oam = new OffsetAndMetadata(TMQConstants.INVALID_OFFSET);
        Assert.assertEquals(TMQConstants.INVALID_OFFSET, oam.offset());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeOffset() {
        new OffsetAndMetadata(-1L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeOffsetAndMetadata() {
        new OffsetAndMetadata(-100L, "metadata");
    }

    @Test
    public void testToString() {
        OffsetAndMetadata oam = new OffsetAndMetadata(123L);
        String str = oam.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains("123"));
        Assert.assertTrue(str.contains("OffsetAndMetadata"));
    }

    @Test
    public void testToStringWithInvalidOffset() {
        OffsetAndMetadata oam = new OffsetAndMetadata(TMQConstants.INVALID_OFFSET, "test");
        String str = oam.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains(String.valueOf(TMQConstants.INVALID_OFFSET)));
    }

    @Test
    public void testMetadataIsEmpty() {
        // Even with non-empty metadata parameter, it's always set to empty string
        OffsetAndMetadata oam = new OffsetAndMetadata(100L, "some_metadata");
        Assert.assertTrue(oam.metadata().isEmpty());
    }
}
