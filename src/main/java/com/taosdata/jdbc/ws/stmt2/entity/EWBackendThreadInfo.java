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

package com.taosdata.jdbc.ws.stmt2.entity;

import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ChunkSizingUtil;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class EWBackendThreadInfo {
    private final ArrayBlockingQueue<Map<Integer, Column>> writeQueue;
    private final ArrayBlockingQueue<EWRawBlock> serialQueue;
    private final AtomicBoolean serializeRunning;
    private volatile Stmt2ColumnFieldBuffer[] reusableColumnBuffers;
    private volatile Stmt2ChunkSizingUtil.BufferSpec[] nextBufferSpecs;
    private volatile int[] underuseStreaks;

    public EWBackendThreadInfo(int writeQueueSize, int serialQueueSize) {
        this.serializeRunning = new AtomicBoolean(false);
        this.writeQueue = new ArrayBlockingQueue<>(writeQueueSize);
        this.serialQueue = new ArrayBlockingQueue<>(serialQueueSize);
    }
    public ArrayBlockingQueue<Map<Integer, Column>> getWriteQueue() {
        return writeQueue;
    }
    public ArrayBlockingQueue<EWRawBlock> getSerialQueue() {
        return serialQueue;
    }
    public AtomicBoolean getSerializeRunning() {
        return serializeRunning;
    }

    public Stmt2ColumnFieldBuffer[] getReusableColumnBuffers() {
        return reusableColumnBuffers;
    }

    public void setReusableColumnBuffers(Stmt2ColumnFieldBuffer[] reusableColumnBuffers) {
        this.reusableColumnBuffers = reusableColumnBuffers;
    }

    public void releaseReusableColumnBuffers() {
        if (reusableColumnBuffers == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer buffer : reusableColumnBuffers) {
            if (buffer != null) {
                buffer.release();
            }
        }
        reusableColumnBuffers = null;
    }

    public Stmt2ChunkSizingUtil.BufferSpec[] getNextBufferSpecs() {
        return nextBufferSpecs;
    }

    public void setNextBufferSpecs(Stmt2ChunkSizingUtil.BufferSpec[] nextBufferSpecs) {
        this.nextBufferSpecs = nextBufferSpecs;
    }

    public int[] getUnderuseStreaks() {
        return underuseStreaks;
    }

    public void setUnderuseStreaks(int[] underuseStreaks) {
        this.underuseStreaks = underuseStreaks;
    }
}
