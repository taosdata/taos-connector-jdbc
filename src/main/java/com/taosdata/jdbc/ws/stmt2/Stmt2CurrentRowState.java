package com.taosdata.jdbc.ws.stmt2;

final class Stmt2CurrentRowState {

    private final boolean[] currentNull;
    private final long[] currentFixed;
    private final byte[][] currentVar;
    private String tableName;

    Stmt2CurrentRowState(int fieldCount) {
        this.currentNull = new boolean[fieldCount];
        this.currentFixed = new long[fieldCount];
        this.currentVar = new byte[fieldCount][];
        clear();
    }

    void stageFixed(int index, long value) {
        currentNull[index] = false;
        currentFixed[index] = value;
        currentVar[index] = null;
    }

    void stageVar(int index, byte[] value) {
        if (value == null) {
            stageNull(index);
            return;
        }
        currentNull[index] = false;
        currentFixed[index] = 0L;
        currentVar[index] = value;
    }

    void stageNull(int index) {
        currentNull[index] = true;
        currentFixed[index] = 0L;
        currentVar[index] = null;
    }

    void stageTbName(String value) {
        tableName = value;
    }

    boolean isNull(int index) {
        return currentNull[index];
    }

    long fixedValue(int index) {
        return currentFixed[index];
    }

    byte[] varValue(int index) {
        return currentVar[index];
    }

    String tableName() {
        return tableName;
    }

    boolean hasPendingValues() {
        if (tableName != null) {
            return true;
        }
        for (int i = 0; i < currentNull.length; i++) {
            if (!currentNull[i] || currentFixed[i] != 0L || currentVar[i] != null) {
                return true;
            }
        }
        return false;
    }

    void clear() {
        for (int i = 0; i < currentNull.length; i++) {
            currentNull[i] = true;
            currentFixed[i] = 0L;
            currentVar[i] = null;
        }
        tableName = null;
    }
}
