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

package com.taosdata.jdbc.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    private  List<ColumnInfo> dataList;
    private  ByteBuffer tableName;
    private  List<ColumnInfo> tagInfo;

    public TableInfo(List<ColumnInfo> dataList, ByteBuffer tableName, List<ColumnInfo> tagInfo) {
        this.dataList = dataList;
        this.tableName = tableName;
        this.tagInfo = tagInfo;
    }

    public static TableInfo getEmptyTableInfo() {
        return new TableInfo(new ArrayList<>(), ByteBuffer.wrap(new byte[]{}), new ArrayList<>());
    }
    public List<ColumnInfo> getDataList() {
        return dataList;
    }

    public ByteBuffer getTableName() {
        return tableName;
    }

    public List<ColumnInfo> getTagInfo() {
        return tagInfo;
    }

    public void setDataList(List<ColumnInfo> dataList) {
        this.dataList = dataList;
    }

    public void setTableName(ByteBuffer tableName) {
        this.tableName = tableName;
    }

    public void setTagInfo(List<ColumnInfo> tagInfo) {
        this.tagInfo = tagInfo;
    }


}
