package com.taosdata.jdbc.utils;

public class BlockUtil {
    private BlockUtil() {
    }
    public static boolean isNull(byte[] c, int n) {
        int position = n >>> 3;
        int index = n & 0x7;
        return (c[position] &0xFF & (1 << (7 - index))) == (1 << (7 - index));
    }
}
