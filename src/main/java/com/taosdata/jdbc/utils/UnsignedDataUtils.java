package com.taosdata.jdbc.utils;

import java.math.BigDecimal;

public class UnsignedDataUtils {

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

    public static BigDecimal parseUBigInt(long val) {
        BigDecimal tmp = new BigDecimal(val >>> 1).multiply(new BigDecimal(2));

        return (val & 0x1) == 0x1 ? tmp.add(new BigDecimal(1)) : tmp;
    }

}
