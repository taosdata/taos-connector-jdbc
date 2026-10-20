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

public class OffsetAndMetadata {
    private final long offset;
    private final String metadata;


    public OffsetAndMetadata(long offset) {
        this(offset, "");
    }

    public OffsetAndMetadata(long offset, String metadata) {
        if (offset < 0 && offset != TMQConstants.INVALID_OFFSET)
            throw new IllegalArgumentException("Invalid negative offset: " + offset);

        this.offset = offset;

        this.metadata = "";
    }

    public long offset() {
        return offset;
    }

    public String metadata() {
        return metadata;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("OffsetAndMetadata{");
        sb.append("offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }
}
