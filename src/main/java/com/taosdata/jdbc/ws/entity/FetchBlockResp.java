package com.taosdata.jdbc.ws.entity;

import java.nio.ByteBuffer;

public class FetchBlockResp extends Response {
    private ByteBuffer buffer;
    private boolean isCompleted;

    public FetchBlockResp(long id, ByteBuffer buffer, boolean isCompleted) {
        this.setAction(Action.FETCH_BLOCK.getAction());
        this.setReqId(id);
        this.buffer = buffer;
        this.isCompleted = isCompleted;
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
}
