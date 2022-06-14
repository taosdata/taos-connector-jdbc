package com.taosdata.jdbc.tmq;

public class CallbackResult {
    private int code;
    private long consumerPointer;
    private long offset;

    public CallbackResult() {
    }

    public CallbackResult(int code, long consumerPointer, long offset) {
        this.code = code;
        this.consumerPointer = consumerPointer;
        this.offset = offset;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public long getConsumerPointer() {
        return consumerPointer;
    }

    public void setConsumerPointer(long consumerPointer) {
        this.consumerPointer = consumerPointer;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
