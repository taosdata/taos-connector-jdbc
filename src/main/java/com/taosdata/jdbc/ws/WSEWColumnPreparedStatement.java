package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.stmt2.Stmt2BindExecRequestBuilder;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2VariableWidthReuseHelper;
import com.taosdata.jdbc.ws.stmt2.Stmt2ChunkSizingUtil;
import com.taosdata.jdbc.ws.stmt2.entity.EWBackendThreadInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWRawBlock;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;

public class WSEWColumnPreparedStatement extends AbstractWSEWPreparedStatement {
    private static final int EW_TARGET_ACTIVE_CHUNKS = 4;

    public WSEWColumnPreparedStatement(Transport transport,
                                       ConnectionParam param,
                                       String database,
                                       AbstractConnection connection,
                                       String sql,
                                       Long instanceId,
                                       Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp);
    }

    @Override
    protected RecursiveAction newSerializationTask(EWBackendThreadInfo backendThreadInfo,
                                                   int batchSize,
                                                   boolean progressive) {
        return new ColumnarWSEWSerializationTask(backendThreadInfo, batchSize, stmtInfo, progressive);
    }

    static final class ColumnarWSEWSerializationTask extends RecursiveAction {
        private final ArrayBlockingQueue<Map<Integer, Column>> writeQueue;
        private final ArrayBlockingQueue<EWRawBlock> serialQueue;
        private final AtomicBoolean running;
        private final EWBackendThreadInfo backendThreadInfo;
        private final int batchSize;
        private final StmtInfo stmtInfo;
        private final WSEWColumnBufferWriter bufferWriter;
        private final boolean progressive;
        final Stmt2ChunkSizingUtil.FieldBatchStats[] batchStats;

        ColumnarWSEWSerializationTask(EWBackendThreadInfo backendThreadInfo,
                                      int batchSize,
                                      StmtInfo stmtInfo,
                                      boolean progressive) {
            this.backendThreadInfo = backendThreadInfo;
            this.writeQueue = backendThreadInfo.getWriteQueue();
            this.serialQueue = backendThreadInfo.getSerialQueue();
            this.running = backendThreadInfo.getSerializeRunning();
            this.batchSize = batchSize;
            this.stmtInfo = stmtInfo;
            this.bufferWriter = new WSEWColumnBufferWriter(stmtInfo);
            this.progressive = progressive;
            this.batchStats = new Stmt2ChunkSizingUtil.FieldBatchStats[stmtInfo.getFields().size()];
            ensureReusableColumnBuffers();
        }

        @Override
        protected void compute() {
            try {
                while (writeQueue.size() >= batchSize && serialQueue.remainingCapacity() > 0) {
                    List<Map<Integer, Column>> rows = new ArrayList<>(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        try {
                            rows.add(writeQueue.take());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                    Stmt2ColumnFieldBuffer[] columnBuffers = null;
                    ByteBuf rawBlock = null;
                    try {
                        resetBatchStats();
                        columnBuffers = ensureReusableColumnBuffers();
                        columnBuffers = bufferWriter.buildFromQueuedRows(
                                rows,
                                columnBuffers,
                                batchStats);
                        backendThreadInfo.setReusableColumnBuffers(columnBuffers);
                        updateNextBufferSpecs(columnBuffers);
                        ByteBuf payload = Stmt2ColumnBindSerializer.serializeDetachedBuffer(columnBuffers);
                        try {
                            rawBlock = Stmt2BindExecRequestBuilder.build(payload);
                            payload = null;
                        } finally {
                            if (payload != null) {
                                Utils.releaseByteBuf(payload);
                            }
                        }
                        serialQueue.put(new EWRawBlock(rawBlock, rows.size(), null));
                        rawBlock = null;
                    } catch (SQLException e) {
                        try {
                            serialQueue.put(new EWRawBlock(null, rows.size(), e));
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        break;
                    } catch (IllegalArgumentException e) {
                        try {
                            serialQueue.put(new EWRawBlock(null, rows.size(),
                                    new SQLException("Buffer sizing calculation failed: " + e.getMessage(), e)));
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        break;
                    } catch (IllegalStateException e) {
                        try {
                            serialQueue.put(new EWRawBlock(null, rows.size(),
                                    new SQLException("Buffer initialization failed: " + e.getMessage(), e)));
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        break;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } finally {
                        bufferWriter.resetColumnBuffers(columnBuffers);
                        if (rawBlock != null) {
                            Utils.releaseByteBuf(rawBlock);
                        }
                    }

                    if (progressive) {
                        break;
                    }
                }
            } finally {
                running.set(false);
            }
        }

        private void resetBatchStats() {
            for (Stmt2ChunkSizingUtil.FieldBatchStats stat : batchStats) {
                if (stat != null) {
                    stat.reset();
                }
            }
        }

        private Stmt2ColumnFieldBuffer[] ensureReusableColumnBuffers() {
            int fieldCount = stmtInfo.getFields().size();
            ensureSizingState(fieldCount);
            Stmt2ChunkSizingUtil.BufferSpec[] nextSpecs = backendThreadInfo.getNextBufferSpecs();
            Stmt2ColumnFieldBuffer[] buffers = backendThreadInfo.getReusableColumnBuffers();
            if (!matchesBufferSpecs(buffers, nextSpecs)) {
                backendThreadInfo.releaseReusableColumnBuffers();
                buffers = bufferWriter.newReusableColumnBuffers(nextSpecs);
                backendThreadInfo.setReusableColumnBuffers(buffers);
            }
            return buffers;
        }

        private void ensureSizingState(int fieldCount) {
            if (backendThreadInfo.getNextBufferSpecs() == null
                    || backendThreadInfo.getNextBufferSpecs().length != fieldCount) {
                Stmt2ChunkSizingUtil.BufferSpec[] nextSpecs = new Stmt2ChunkSizingUtil.BufferSpec[fieldCount];
                Stmt2ChunkSizingUtil.BufferSpec[] previous = backendThreadInfo.getNextBufferSpecs();
                if (previous != null) {
                    System.arraycopy(previous, 0, nextSpecs, 0, Math.min(previous.length, fieldCount));
                }
                backendThreadInfo.setNextBufferSpecs(nextSpecs);
            }
            if (backendThreadInfo.getUnderuseStreaks() == null
                    || backendThreadInfo.getUnderuseStreaks().length != fieldCount) {
                int[] streaks = new int[fieldCount];
                int[] previous = backendThreadInfo.getUnderuseStreaks();
                if (previous != null) {
                    System.arraycopy(previous, 0, streaks, 0, Math.min(previous.length, fieldCount));
                }
                backendThreadInfo.setUnderuseStreaks(streaks);
            }
        }

        private boolean matchesBufferSpecs(Stmt2ColumnFieldBuffer[] buffers,
                                           Stmt2ChunkSizingUtil.BufferSpec[] nextSpecs) {
            if (buffers == null || buffers.length != stmtInfo.getFields().size()) {
                return false;
            }
            for (int i = 0; i < buffers.length; i++) {
                if (!buffers[i].getMeta().isVariableWidth()) {
                    continue;
                }
                if (!Stmt2VariableWidthReuseHelper.bufferSpecsEqual(
                        buffers[i].currentReusableSpec(),
                        Stmt2VariableWidthReuseHelper.resolveBufferSpec(nextSpecs, i))) {
                    return false;
                }
            }
            return true;
        }

        private void updateNextBufferSpecs(Stmt2ColumnFieldBuffer[] columnBuffers) {
            ensureSizingState(stmtInfo.getFields().size());
            Stmt2ChunkSizingUtil.BufferSpec[] nextSpecs = backendThreadInfo.getNextBufferSpecs();
            int[] underuseStreaks = backendThreadInfo.getUnderuseStreaks();
            for (int i = 0; i < columnBuffers.length; i++) {
                if (!columnBuffers[i].getMeta().isVariableWidth()) {
                    nextSpecs[i] = null;
                    underuseStreaks[i] = 0;
                    continue;
                }

                Stmt2ChunkSizingUtil.FieldBatchStats stats = batchStats[i];
                Stmt2ChunkSizingUtil.BufferSpec current = columnBuffers[i].currentReusableSpec();
                if (current == null) {
                    current = Stmt2VariableWidthReuseHelper.resolveBufferSpec(nextSpecs, i);
                }
                if (stats == null || current == null) {
                    nextSpecs[i] = current;
                    underuseStreaks[i] = 0;
                    continue;
                }

                stats.setActiveChunksUsed(columnBuffers[i].activeReusableChunkCount());
                Stmt2ChunkSizingUtil.BufferSpec wanted =
                        Stmt2ChunkSizingUtil.deriveWantedSpec(stats, batchSize);
                if (Stmt2VariableWidthReuseHelper.bufferSpecsEqual(current, wanted)) {
                    nextSpecs[i] = current;
                    underuseStreaks[i] = 0;
                } else if (!Stmt2ChunkSizingUtil.canReuse(
                        current, wanted, stats, batchSize, EW_TARGET_ACTIVE_CHUNKS)) {
                    nextSpecs[i] = wanted;
                    underuseStreaks[i] = 0;
                } else if (isSmallerSpec(wanted, current)) {
                    int streak = underuseStreaks[i] + 1;
                    if (Stmt2ChunkSizingUtil.shouldShrink(current, wanted, streak)) {
                        nextSpecs[i] = wanted;
                        underuseStreaks[i] = 0;
                    } else {
                        nextSpecs[i] = current;
                        underuseStreaks[i] = streak;
                    }
                } else {
                    nextSpecs[i] = current;
                    underuseStreaks[i] = 0;
                }
            }
        }

        private boolean isSmallerSpec(Stmt2ChunkSizingUtil.BufferSpec candidate,
                                      Stmt2ChunkSizingUtil.BufferSpec current) {
            long candidateBytes =
                    (long) candidate.getChunkBytes() * candidate.getReusableChunkCount();
            long currentBytes =
                    (long) current.getChunkBytes() * current.getReusableChunkCount();
            return candidateBytes < currentBytes;
        }
    }
}
