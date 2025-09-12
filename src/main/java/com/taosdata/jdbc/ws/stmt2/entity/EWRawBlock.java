package com.taosdata.jdbc.ws.stmt2.entity;

import io.netty.buffer.ByteBuf;

public class EWRawBlock {


    private final ByteBuf byteBuf;
    private final int rowCount;
    private final Exception lastError;

    public EWRawBlock(ByteBuf byteBuf, int rowCount, Exception lastError) {
        this.byteBuf = byteBuf;
        this.rowCount = rowCount;
        this.lastError = lastError;
    }
    public ByteBuf getByteBuf() {
        return byteBuf;
    }
    public int getRowCount() {
        return rowCount;
    }
    public Exception getLastError() {
        return lastError;
    }
}
