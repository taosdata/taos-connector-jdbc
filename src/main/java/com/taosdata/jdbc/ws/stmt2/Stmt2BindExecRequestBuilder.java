package com.taosdata.jdbc.ws.stmt2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import com.taosdata.jdbc.utils.Utils;

/**
 * Builds the binary WebSocket envelope for {@code stmt2_bind_exec}.
 *
 * <p>The server expects a 26-byte little-endian header:
 * reqId(8) + stmtId(8) + actionId(8) + version(2), followed by the
 * columnar payload body.
 *
 * <p>The caller may leave reqId and stmtId as zero placeholders because
 * {@code WSRetryableStmt} patches those two fields before each send/retry.
 */
public final class Stmt2BindExecRequestBuilder {
    public static final long ACTION_ID = 11L;
    public static final short PROTOCOL_VERSION = 1;
    public static final int HEADER_SIZE = 26;

    private Stmt2BindExecRequestBuilder() {
    }

    public static ByteBuf build(byte[] payload) {
        return build(Unpooled.wrappedBuffer(payload));
    }

    /**
     * Builds the final bind-exec request without copying the payload again.
     *
     * <p>Ownership of {@code payload} is transferred to the returned composite
     * buffer. Callers should release only the returned request buffer.
     */
    public static ByteBuf build(ByteBuf payload) {
        CompositeByteBuf request = PooledByteBufAllocator.DEFAULT.compositeBuffer(2);
        boolean success = false;
        try {
            ByteBuf header = PooledByteBufAllocator.DEFAULT.directBuffer(HEADER_SIZE);
            header.writeLongLE(0L); // reqId placeholder
            header.writeLongLE(0L); // stmtId placeholder
            header.writeLongLE(ACTION_ID);
            header.writeShortLE(PROTOCOL_VERSION);
            request.addComponent(true, header);
            request.addComponent(true, payload);
            success = true;
            return request;
        } finally {
            if (!success) {
                Utils.releaseByteBuf(request);
            }
        }
    }
}
