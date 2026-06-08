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
