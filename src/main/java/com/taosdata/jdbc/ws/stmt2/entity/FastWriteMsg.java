package com.taosdata.jdbc.ws.stmt2.entity;

import com.taosdata.jdbc.enums.FastWriterMsgType;

public class FastWriteMsg {
    private FastWriterMsgType messageType;

    public FastWriteMsg(FastWriterMsgType messageType) {
        this.messageType = messageType;
    }
    public FastWriterMsgType getMessageType() {
        return messageType;
    }
}
