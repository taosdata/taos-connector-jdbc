package com.taosdata.jdbc.ws.stmt2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;

final class FixedWidthRawSlab {
    private final int fixedWidth;
    private byte[] nulls;
    private byte[] values;
    private int rowCount;

    FixedWidthRawSlab(int fixedWidth, int initialRows) {
        if (fixedWidth <= 0) {
            throw new IllegalArgumentException("fixedWidth must be > 0");
        }
        if (initialRows <= 0) {
            throw new IllegalArgumentException("initialRows must be > 0");
        }
        this.fixedWidth = fixedWidth;
        this.nulls = new byte[initialRows];
        this.values = new byte[initialRows * fixedWidth];
    }

    void appendNull() {
        int rowIndex = reserveRow();
        nulls[rowIndex] = 1;
        zeroValue(rowIndex);
    }

    void appendFixed1(byte raw) {
        int rowIndex = reserveRow();
        int offset = rowIndex * fixedWidth;
        nulls[rowIndex] = 0;
        values[offset] = raw;
    }

    void appendFixed2(short raw) {
        int rowIndex = reserveRow();
        int offset = rowIndex * fixedWidth;
        nulls[rowIndex] = 0;
        values[offset] = (byte) raw;
        values[offset + 1] = (byte) (raw >>> 8);
    }

    void appendFixed4(int raw) {
        int rowIndex = reserveRow();
        int offset = rowIndex * fixedWidth;
        nulls[rowIndex] = 0;
        values[offset] = (byte) raw;
        values[offset + 1] = (byte) (raw >>> 8);
        values[offset + 2] = (byte) (raw >>> 16);
        values[offset + 3] = (byte) (raw >>> 24);
    }

    void appendFixed8(long raw) {
        int rowIndex = reserveRow();
        int offset = rowIndex * fixedWidth;
        nulls[rowIndex] = 0;
        values[offset] = (byte) raw;
        values[offset + 1] = (byte) (raw >>> 8);
        values[offset + 2] = (byte) (raw >>> 16);
        values[offset + 3] = (byte) (raw >>> 24);
        values[offset + 4] = (byte) (raw >>> 32);
        values[offset + 5] = (byte) (raw >>> 40);
        values[offset + 6] = (byte) (raw >>> 48);
        values[offset + 7] = (byte) (raw >>> 56);
    }

    ByteBuf nullSlice() {
        return Unpooled.wrappedBuffer(nulls, 0, rowCount);
    }

    ByteBuf valueSlice() {
        return Unpooled.wrappedBuffer(values, 0, rowCount * fixedWidth);
    }

    int rowCount() {
        return rowCount;
    }

    int rowCapacity() {
        return nulls.length;
    }

    int nullCapacityBytes() {
        return nulls.length;
    }

    int valueCapacityBytes() {
        return values.length;
    }

    void reset() {
        rowCount = 0;
    }

    private int reserveRow() {
        ensureRowCapacity(rowCount + 1);
        return rowCount++;
    }

    private void ensureRowCapacity(int neededRows) {
        if (neededRows <= nulls.length) {
            return;
        }
        int newCapacity = nulls.length;
        while (newCapacity < neededRows) {
            newCapacity <<= 1;
        }
        nulls = Arrays.copyOf(nulls, newCapacity);
        values = Arrays.copyOf(values, newCapacity * fixedWidth);
    }

    private void zeroValue(int rowIndex) {
        int offset = rowIndex * fixedWidth;
        Arrays.fill(values, offset, offset + fixedWidth, (byte) 0);
    }
}
