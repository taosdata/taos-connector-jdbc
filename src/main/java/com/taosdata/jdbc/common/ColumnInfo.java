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

package com.taosdata.jdbc.common;

import java.util.List;

public class ColumnInfo implements Comparable<ColumnInfo> {
    private final List<Object> dataList;
    // taos data type
    private final int type;
    private final int index;
    private int serializeSize;

    public ColumnInfo(int columnIndex, List<Object> dataList, int type) {
        this.index = columnIndex;
        this.dataList = dataList;
        this.type = type;
    }

    public void add(Object data) {
        this.dataList.add(data);
    }

    public List<Object> getDataList() {
        return dataList;
    }

    public int getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    public int getSerializeSize() {
        return serializeSize;
    }

    public void setSerializeSize(int serializeSize) {
        this.serializeSize = serializeSize;
    }

    @Override
    public int compareTo(ColumnInfo c) {
        return this.index > c.index ? 1 : -1;
    }

}
