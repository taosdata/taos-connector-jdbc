package com.taosdata.jdbc.ws.stmt2;

public final class Stmt2CurrentRowState {

    private final boolean[] currentNull;
    private final boolean[] currentStaged;
    private final long[] currentFixed;
    private final byte[][] currentVar;
    private String tableName;

    public Stmt2CurrentRowState(int fieldCount) {
        this.currentNull = new boolean[fieldCount];
        this.currentStaged = new boolean[fieldCount];
        this.currentFixed = new long[fieldCount];
        this.currentVar = new byte[fieldCount][];
        clear();
    }

    public void stageFixed(int index, long value) {
        currentStaged[index] = true;
        currentNull[index] = false;
        currentFixed[index] = value;
        currentVar[index] = null;
    }

    public void stageVar(int index, byte[] value) {
        if (value == null) {
            stageNull(index);
            return;
        }
        currentStaged[index] = true;
        currentNull[index] = false;
        currentFixed[index] = 0L;
        currentVar[index] = value;
    }

    public void stageNull(int index) {
        currentStaged[index] = true;
        currentNull[index] = true;
        currentFixed[index] = 0L;
        currentVar[index] = null;
    }

    public void stageTbName(String value) {
        tableName = value;
    }

    public boolean isNull(int index) {
        return currentNull[index];
    }

    public long fixedValue(int index) {
        return currentFixed[index];
    }

    public byte[] varValue(int index) {
        return currentVar[index];
    }

    public String tableName() {
        return tableName;
    }

    public boolean hasPendingValues() {
        if (tableName != null) {
            return true;
        }
        for (int i = 0; i < currentNull.length; i++) {
            if (currentStaged[i]) {
                return true;
            }
        }
        return false;
    }

    public void clear() {
        for (int i = 0; i < currentNull.length; i++) {
            currentStaged[i] = false;
            currentNull[i] = true;
            currentFixed[i] = 0L;
            currentVar[i] = null;
        }
        tableName = null;
    }
}
