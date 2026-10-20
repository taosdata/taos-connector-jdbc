/*
 * Copyright (c) 2024 TAOS Data, Inc.
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

package com.taosdata.jdbc.ws.entity;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;

public class FetchBlockNewResp extends Response {
    private ByteBuf buffer;

    public void setCompleted(boolean completed) {
        isCompleted = completed;
    }

    private boolean isCompleted;
    private long time;
    private int code;
    private String message;
    private short version;

    public FetchBlockNewResp(ByteBuf buffer) {
        this.setAction(Action.FETCH_BLOCK_NEW.getAction());
        this.buffer = buffer;
    }

    public void init() {
        buffer.readLongLE(); // action id
        version = buffer.readShortLE();
        time = buffer.readLongLE();
        this.setReqId(buffer.readLongLE());
        code = buffer.readIntLE();
        int messageLen = buffer.readIntLE();
        byte[] msgBytes = new byte[messageLen];
        buffer.readBytes(msgBytes);

        message = new String(msgBytes, StandardCharsets.UTF_8);
        buffer.readLongLE(); // resultId
        isCompleted = buffer.readByte() != 0;

        if (isCompleted || code != Code.SUCCESS.getCode()){
            ReferenceCountUtil.safeRelease(buffer);
        }
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuf buffer) {
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
