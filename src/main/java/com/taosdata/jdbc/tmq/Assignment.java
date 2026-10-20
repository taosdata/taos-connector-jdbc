/*
 * Copyright (c) 2023 TAOS Data, Inc.
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

import com.fasterxml.jackson.annotation.JsonProperty;

public class Assignment {
    @JsonProperty("vgroup_id")
    private int vgId;
    @JsonProperty("offset")
    private long currentOffset;
    private long begin;
    private long end;

    public Assignment() {
    }

    public Assignment(int vgId, long currentOffset, long begin, long end) {
        this.vgId = vgId;
        this.currentOffset = currentOffset;
        this.begin = begin;
        this.end = end;
    }

    public void setVgId(int vgId) {
        this.vgId = vgId;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public void setBegin(long begin) {
        this.begin = begin;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public int getVgId() {
        return vgId;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public long getBegin() {
        return begin;
    }

    public long getEnd() {
        return end;
    }
}
