/*
 * Copyright (c) 2022 TAOS Data, Inc.
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

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;

import java.math.BigInteger;
import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.MAX_UNSIGNED_LONG;

public class UnsignedDataUtils {

    private static final BigInteger MAX_UNSIGNED_LONG_VALUE = new BigInteger(MAX_UNSIGNED_LONG);

    private UnsignedDataUtils() {
    }

    public static short parseUTinyInt(byte val) {
        return (short) (val & 0xff);
    }

    public static int parseUSmallInt(short val) {
        return val & 0xffff;
    }

    public static long parseUInteger(int val) {
        return val & 0xffffffffL;
    }

    public static BigInteger parseUBigInt(long val) {

        if (val > 0) {
            return BigInteger.valueOf(val);
        }

        return new BigInteger(Long.toUnsignedString(val));
    }

    public static long toUnsignedLongBits(BigInteger val) throws SQLException {
        if (val.signum() < 0 || val.compareTo(MAX_UNSIGNED_LONG_VALUE) > 0) {
            throw TSDBError.createSQLException(
                    TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "ubigint value is out of range");
        }
        return val.longValue();
    }

}
