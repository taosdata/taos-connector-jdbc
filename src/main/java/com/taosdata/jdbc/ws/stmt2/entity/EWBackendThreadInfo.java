package com.taosdata.jdbc.ws.stmt2.entity;

import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.WSEWChunkSizingUtil;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class EWBackendThreadInfo {
    private final ArrayBlockingQueue<Map<Integer, Column>> writeQueue;
    private final ArrayBlockingQueue<EWRawBlock> serialQueue;
    private final AtomicBoolean serializeRunning;
    private volatile Stmt2ColumnFieldBuffer[] reusableColumnBuffers;
    private volatile WSEWChunkSizingUtil.BufferSpec[] nextBufferSpecs;
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

    public WSEWChunkSizingUtil.BufferSpec[] getNextBufferSpecs() {
        return nextBufferSpecs;
    }

    public void setNextBufferSpecs(WSEWChunkSizingUtil.BufferSpec[] nextBufferSpecs) {
        this.nextBufferSpecs = nextBufferSpecs;
    }

    public int[] getUnderuseStreaks() {
        return underuseStreaks;
    }

    public void setUnderuseStreaks(int[] underuseStreaks) {
        this.underuseStreaks = underuseStreaks;
    }
}
