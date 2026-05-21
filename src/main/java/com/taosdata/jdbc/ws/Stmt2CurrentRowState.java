package com.taosdata.jdbc.ws;

final class Stmt2CurrentRowState {

    private final byte[] currentFixed1;
    private final short[] currentFixed2;
    private final int[] currentFixed4;
    private final long[] currentFixed8;
    private final byte[][] currentVar;
    private final int[] currentVarLength;
    private final String[] currentString;
    private final long[] currentNonNullBits;
    private final long[] currentTouchedBits;
    private final int[] touchedIndexes;
    private int touchedCount;
    private boolean rowDirty;

    Stmt2CurrentRowState(int fieldCount) {
        this.currentFixed1 = new byte[fieldCount];
        this.currentFixed2 = new short[fieldCount];
        this.currentFixed4 = new int[fieldCount];
        this.currentFixed8 = new long[fieldCount];
        this.currentVar = new byte[fieldCount][];
        this.currentVarLength = new int[fieldCount];
        this.currentString = new String[fieldCount];
        int bitWords = (fieldCount + Long.SIZE - 1) / Long.SIZE;
        this.currentNonNullBits = new long[bitWords];
        this.currentTouchedBits = new long[bitWords];
        this.touchedIndexes = new int[fieldCount];
    }

    void stageFixed1(int index, byte value) {
        markTouched(index);
        rowDirty = true;
        setNonNull(index, true);
        currentFixed1[index] = value;
        currentVar[index] = null;
        currentVarLength[index] = 0;
        currentString[index] = null;
    }

    void stageFixed2(int index, short value) {
        markTouched(index);
        rowDirty = true;
        setNonNull(index, true);
        currentFixed2[index] = value;
        currentVar[index] = null;
        currentVarLength[index] = 0;
        currentString[index] = null;
    }

    void stageFixed4(int index, int value) {
        markTouched(index);
        rowDirty = true;
        setNonNull(index, true);
        currentFixed4[index] = value;
        currentVar[index] = null;
        currentVarLength[index] = 0;
        currentString[index] = null;
    }

    void stageFixed8(int index, long value) {
        markTouched(index);
        rowDirty = true;
        setNonNull(index, true);
        currentFixed8[index] = value;
        currentVar[index] = null;
        currentVarLength[index] = 0;
        currentString[index] = null;
    }

    void stageVar(int index, byte[] value) {
        stageVar(index, value, value == null ? 0 : value.length);
    }

    void stageVar(int index, byte[] value, int length) {
        if (value == null) {
            stageNull(index);
            return;
        }
        markTouched(index);
        rowDirty = true;
        setNonNull(index, true);
        currentVar[index] = value;
        currentVarLength[index] = length;
        currentString[index] = null;
    }

    void stageString(int index, String value) {
        if (value == null) {
            stageNull(index);
            return;
        }
        markTouched(index);
        rowDirty = true;
        setNonNull(index, true);
        currentVar[index] = null;
        currentVarLength[index] = 0;
        currentString[index] = value;
    }

    void stageNull(int index) {
        markTouched(index);
        rowDirty = true;
        setNonNull(index, false);
        currentVar[index] = null;
        currentVarLength[index] = 0;
        currentString[index] = null;
    }

    boolean isNull(int index) {
        return !isNonNull(index);
    }

    byte fixed1Value(int index) {
        return currentFixed1[index];
    }

    short fixed2Value(int index) {
        return currentFixed2[index];
    }

    int fixed4Value(int index) {
        return currentFixed4[index];
    }

    long fixed8Value(int index) {
        return currentFixed8[index];
    }

    byte[] varValue(int index) {
        return currentVar[index];
    }

    int varLength(int index) {
        return currentVarLength[index];
    }

    String stringValue(int index) {
        return currentString[index];
    }

    boolean hasPendingValues() {
        return touchedCount > 0;
    }

    boolean isRowDirty() {
        return rowDirty;
    }

    void clear() {
        for (int i = 0; i < touchedCount; i++) {
            int index = touchedIndexes[i];
            clearTouched(index);
            setNonNull(index, false);
            currentVar[index] = null;
            currentVarLength[index] = 0;
            currentString[index] = null;
        }
        touchedCount = 0;
        rowDirty = false;
    }

    private void markTouched(int index) {
        if (!isTouched(index)) {
            setTouched(index, true);
            touchedIndexes[touchedCount++] = index;
        }
    }

    private boolean isNonNull(int index) {
        int word = index / Long.SIZE;
        long mask = 1L << (index % Long.SIZE);
        return (currentNonNullBits[word] & mask) != 0L;
    }

    private void setNonNull(int index, boolean nonNull) {
        int word = index / Long.SIZE;
        long mask = 1L << (index % Long.SIZE);
        if (nonNull) {
            currentNonNullBits[word] |= mask;
        } else {
            currentNonNullBits[word] &= ~mask;
        }
    }

    private boolean isTouched(int index) {
        int word = index / Long.SIZE;
        long mask = 1L << (index % Long.SIZE);
        return (currentTouchedBits[word] & mask) != 0L;
    }

    private void setTouched(int index, boolean touched) {
        int word = index / Long.SIZE;
        long mask = 1L << (index % Long.SIZE);
        if (touched) {
            currentTouchedBits[word] |= mask;
        } else {
            currentTouchedBits[word] &= ~mask;
        }
    }

    private void clearTouched(int index) {
        setTouched(index, false);
    }
}
