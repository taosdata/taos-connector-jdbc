package com.taosdata.jdbc.tmq;


public class CallbackResult {
    private int code;
    private TAOSConsumer consumer;
    private long offset;

    public CallbackResult() {
    }

    public CallbackResult(int code, TAOSConsumer consumer, long offset) {
        this.code = code;
        this.consumer = consumer;
        this.offset = offset;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public TAOSConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(TAOSConsumer consumer) {
        this.consumer = consumer;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
