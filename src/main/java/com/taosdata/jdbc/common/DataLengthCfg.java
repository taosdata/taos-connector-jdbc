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

public class DataLengthCfg {
    private static final Integer[] dataLenArr = {
            1,//TSDB_DATA_TYPE_NULL
            1,//TSDB_DATA_TYPE_BOOL
            1,//TSDB_DATA_TYPE_TINYINT
            2,//TSDB_DATA_TYPE_SMALLINT
            4,//TSDB_DATA_TYPE_INT
            8,//TSDB_DATA_TYPE_BIGINT
            4,//TSDB_DATA_TYPE_FLOAT
            8,//TSDB_DATA_TYPE_DOUBLE
            null,
            8, //TSDB_DATA_TYPE_TIMESTAMP
            null,
            1, //TSDB_DATA_TYPE_UTINYINT
            2, //TSDB_DATA_TYPE_USMALLINT
            4, //TSDB_DATA_TYPE_UINT
            8  //TSDB_DATA_TYPE_UBIGINT
};

    public static Integer getDataLength(int dataType){
        if (dataType < dataLenArr.length){
            return dataLenArr[dataType];
        }
        return null;
    }
}
