/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.ws;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.List;

public class CustomWebSocketFrameEncoder extends MessageToMessageEncoder<WebSocketFrame> implements WebSocketFrameEncoder {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(CustomWebSocketFrameEncoder.class);


    public CustomWebSocketFrameEncoder() {
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, WebSocketFrame msg, List<Object> out) throws Exception {

        ByteBuf data = msg.content();
        byte opcode;
        if (msg instanceof TextWebSocketFrame) {
            opcode = 1;
        } else if (msg instanceof BinaryWebSocketFrame) {
            opcode = 2;
        } else if (msg instanceof PingWebSocketFrame) {
            opcode = 9;
        } else if (msg instanceof PongWebSocketFrame) {
            opcode = 10;
        } else if (msg instanceof CloseWebSocketFrame) {
            opcode = 8;
        } else {
            if (!(msg instanceof ContinuationWebSocketFrame)) {
                throw new UnsupportedOperationException("Cannot encode frame of type: " + msg.getClass().getName());
            }

            opcode = 0;
        }

        int length = data.readableBytes();
        if (logger.isTraceEnabled()) {
            logger.trace("Encoding WebSocket Frame opCode={} length={}", opcode, length);
        }

        int b0 = 0;
        if (msg.isFinalFragment()) {
            b0 |= 128;
        }

        b0 |= msg.rsv() % 8 << 4;
        b0 |= opcode % 128;
        if (opcode == 9 && length > 125) {
            throw new TooLongFrameException("invalid payload for PING (payload length must be <= 125, was " + length);
        } else {
            boolean release = true;
            ByteBuf buf = null;

            try {
                int maskLength = 4;
                if (length <= 125) {
                    int size = 2 + maskLength + length;
                    buf = ctx.alloc().buffer(size);
                    buf.writeByte(b0);
                    byte b = (byte) (128 | (byte) length);
                    buf.writeByte(b);
                } else if (length > 65535) {
                    int size = 10 + maskLength + length;
                    buf = ctx.alloc().buffer(size);
                    buf.writeByte(b0);
                    buf.writeByte(255);
                    buf.writeLong((long) length);
                } else {
                    int size = 4 + maskLength + length;
                    buf = ctx.alloc().buffer(size);
                    buf.writeByte(b0);
                    buf.writeByte(254);
                    buf.writeByte(length >>> 8 & 255);
                    buf.writeByte(length & 255);
                }

                buf.writeInt(0);
                buf.writeBytes(data);
                out.add(buf);

                release = false;
            } finally {
                if (release && buf != null) {
                    buf.release();
                }
            }
        }
    }
}
