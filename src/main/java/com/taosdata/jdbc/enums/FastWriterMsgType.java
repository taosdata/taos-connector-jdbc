package com.taosdata.jdbc.enums;

public enum FastWriterMsgType {
    EXCEEDS_BATCH_SIZE(1),
    LINGER_MS_EXPIRED(2),
    WRITE_COMPLETED(3);

    private final int msgType;
    // Constructor
    FastWriterMsgType(int msgType) {
        this.msgType = msgType;
    }
    // Getter method
    public int getMsgType() {
        return msgType;
    }
}
