package com.taosdata.jdbc.ws.entity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FetchBlockNewResp extends Response {
    private ByteBuffer buffer;
    private boolean isCompleted;
    private long time;
    private int code;
    private String message;
    private short version;

    public FetchBlockNewResp(ByteBuffer buffer) {
        this.setAction(Action.FETCH_BLOCK_NEW.getAction());
        this.buffer = buffer;
    }

    public void init() {
        buffer.getLong(); // action id
        version = buffer.getShort();
        time = buffer.getLong();
        this.setReqId(buffer.getLong());
        code = buffer.getInt();
        int messageLen = buffer.getInt();
        byte[] msgBytes = new byte[messageLen];
        buffer.get(msgBytes);

        message = new String(msgBytes, StandardCharsets.UTF_8);
        buffer.getLong(); // resultId
        isCompleted = buffer.get() == 1;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public boolean isCompleted() {
        return isCompleted;
    }


    public long getTime() {
        return time;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public short getVersion() {
        return version;
    }
}
