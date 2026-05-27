package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.BlobUtil;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.stmt2.Stmt2BindExecRequestBuilder;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import com.taosdata.jdbc.ws.stmt2.WSEWChunkSizingUtil;
import com.taosdata.jdbc.ws.stmt2.entity.EWBackendThreadInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWRawBlock;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BINARY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BLOB;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_BOOL;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DECIMAL128;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DECIMAL64;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_DOUBLE;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_FLOAT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_GEOMETRY;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_INT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_JSON;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_NCHAR;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_SMALLINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_TINYINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UBIGINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_USMALLINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_UTINYINT;
import static com.taosdata.jdbc.TSDBConstants.TSDB_DATA_TYPE_VARBINARY;

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

    static Stmt2ColumnFieldBuffer[] buildColumnBuffersFromQueuedRows(List<Map<Integer, Column>> rows,
                                                                     StmtInfo stmtInfo) throws SQLException {
        return buildColumnBuffersFromQueuedRows(rows, stmtInfo, null);
    }

    static Stmt2ColumnFieldBuffer[] buildColumnBuffersFromQueuedRows(List<Map<Integer, Column>> rows,
                                                                     StmtInfo stmtInfo,
                                                                     Stmt2ColumnFieldBuffer[] reusableBuffers) throws SQLException {
        Stmt2ColumnFieldBuffer[] buffers = reusableBuffers;
        boolean createdNew = false;
        if (buffers == null) {
            buffers = newReusableColumnBuffers(stmtInfo, null);
            createdNew = true;
        } else {
            resetColumnBuffers(buffers);
        }

        boolean success = false;
        try {
            fillColumnBuffersFromQueuedRows(rows, stmtInfo, buffers);
            success = true;
            return buffers;
        } finally {
            if (!success) {
                if (createdNew) {
                    releaseColumnBuffers(buffers);
                } else {
                    resetColumnBuffers(buffers);
                }
            }
        }
    }

    static Stmt2ColumnFieldBuffer[] buildColumnBuffersFromQueuedRows(
            List<Map<Integer, Column>> rows,
            StmtInfo stmtInfo,
            Stmt2ColumnFieldBuffer[] reusableBuffers,
            WSEWChunkSizingUtil.FieldBatchStats[] stats) throws SQLException {
        int fieldCount = stmtInfo.getFields().size();
        if (stats != null && stats.length < fieldCount) {
            throw new IllegalArgumentException(
                    "stats array length " + stats.length
                            + " is less than field count " + fieldCount
                            + "; caller must provide stats.length >= fieldCount");
        }
        Stmt2ColumnFieldBuffer[] buffers = reusableBuffers;
        boolean createdNew = false;
        if (buffers == null) {
            buffers = newReusableColumnBuffers(stmtInfo, null);
            createdNew = true;
        } else {
            resetColumnBuffers(buffers);
        }

        boolean success = false;
        try {
            fillColumnBuffersFromQueuedRows(rows, stmtInfo, buffers, stats);
            success = true;
            return buffers;
        } finally {
            if (!success) {
                if (createdNew) {
                    releaseColumnBuffers(buffers);
                } else {
                    resetColumnBuffers(buffers);
                }
            }
        }
    }

    private static void fillColumnBuffersFromQueuedRows(List<Map<Integer, Column>> rows,
                                                        StmtInfo stmtInfo,
                                                        Stmt2ColumnFieldBuffer[] buffers) throws SQLException {
        int tbNameFieldIdx = stmtInfo.getToBeBindTableNameIndex();
        for (Map<Integer, Column> row : rows) {
            for (int i = 0; i < buffers.length; i++) {
                Column column = row.get(i + 1);
                if (column == null) {
                    throw new SQLException("Missing bound column at index " + (i + 1));
                }
                if (i == tbNameFieldIdx) {
                    appendTbName(buffers[i], column);
                } else {
                    appendColumnValue(buffers[i], column, stmtInfo.getPrecision());
                }
            }
        }
    }

    private static void fillColumnBuffersFromQueuedRows(List<Map<Integer, Column>> rows,
                                                        StmtInfo stmtInfo,
                                                        Stmt2ColumnFieldBuffer[] buffers,
                                                        WSEWChunkSizingUtil.FieldBatchStats[] stats) throws SQLException {
        int tbNameFieldIdx = stmtInfo.getToBeBindTableNameIndex();
        for (Map<Integer, Column> row : rows) {
            for (int i = 0; i < buffers.length; i++) {
                Column column = row.get(i + 1);
                if (column == null) {
                    throw new SQLException("Missing bound column at index " + (i + 1));
                }
                if (i == tbNameFieldIdx) {
                    if (stats != null && stats[i] == null) {
                        stats[i] = new WSEWChunkSizingUtil.FieldBatchStats();
                    }
                    if (stats != null && stats[i] != null) {
                        observeWrite(stats[i], column, stmtInfo.getPrecision());
                    }
                    appendTbName(buffers[i], column);
                    continue;
                }
                if (stats != null && stats[i] == null && buffers[i].getMeta().isVariableWidth()) {
                    stats[i] = new WSEWChunkSizingUtil.FieldBatchStats();
                }
                if (stats != null && stats[i] != null) {
                    observeWrite(stats[i], column, stmtInfo.getPrecision());
                }
                appendColumnValue(buffers[i], column, stmtInfo.getPrecision());
            }
        }
    }

    private static void observeWrite(WSEWChunkSizingUtil.FieldBatchStats stats,
                                     Column column,
                                     int precision) {
        Object value = column.getData();
        int valueBytes;
        if (value instanceof String) {
            valueBytes = ByteBufUtil.utf8Bytes((String) value);
        } else if (value instanceof byte[]) {
            valueBytes = ((byte[]) value).length;
        } else if (value instanceof Blob) {
            try {
                valueBytes = (int) ((Blob) value).length();
            } catch (java.sql.SQLException e) {
                valueBytes = 0;
            }
        } else {
            valueBytes = 0;
        }
        stats.recordValueBytes(valueBytes, valueBytes, 1);
    }

    private static Stmt2ColumnFieldBuffer[] newReusableColumnBuffers(
            StmtInfo stmtInfo,
            WSEWChunkSizingUtil.BufferSpec[] bufferSpecs) {
        Stmt2ColumnFieldBuffer[] buffers = new Stmt2ColumnFieldBuffer[stmtInfo.getFields().size()];
        for (int i = 0; i < stmtInfo.getFields().size(); i++) {
            Field field = stmtInfo.getFields().get(i);
            Stmt2FieldMeta meta = Stmt2FieldMeta.fromField(field);
            if (meta.isVariableWidth()) {
                WSEWChunkSizingUtil.BufferSpec spec = resolveBufferSpec(bufferSpecs, i);
                buffers[i] = Stmt2ColumnFieldBuffer.forReusableValueBuffer(
                        meta,
                        null,
                        spec.getChunkBytes(),
                        spec.getChunkBytes() / 2,
                        spec.getReusableChunkCount());
                primeReusableBuffer(buffers[i], spec);
            } else {
                buffers[i] = new Stmt2ColumnFieldBuffer(meta);
            }
        }
        return buffers;
    }

    private static void resetColumnBuffers(Stmt2ColumnFieldBuffer[] buffers) {
        if (buffers == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer buffer : buffers) {
            if (buffer != null) {
                buffer.reset();
            }
        }
    }

    private static WSEWChunkSizingUtil.BufferSpec resolveBufferSpec(
            WSEWChunkSizingUtil.BufferSpec[] bufferSpecs,
            int index) {
        if (bufferSpecs != null
                && index < bufferSpecs.length
                && bufferSpecs[index] != null) {
            return bufferSpecs[index];
        }
        return WSEWChunkSizingUtil.bootstrapSpec();
    }

    private static void primeReusableBuffer(
            Stmt2ColumnFieldBuffer buffer,
            WSEWChunkSizingUtil.BufferSpec spec) {
        byte[] chunk = new byte[spec.getChunkBytes()];
        try {
            for (int i = 0; i < spec.getReusableChunkCount(); i++) {
                buffer.appendBytes(chunk);
            }
            buffer.reset();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to prime reusable WSEW buffer", e);
        }
    }

    private static void appendTbName(Stmt2ColumnFieldBuffer buffer, Column column) throws SQLException {
        Object value = column.getData();
        if (value instanceof String) {
            buffer.appendTbName((String) value);
            return;
        }
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            buffer.appendEncodedVar(bytes, bytes.length);
            return;
        }
        throw new SQLException("table name must be string or binary");
    }

    private static void appendColumnValue(Stmt2ColumnFieldBuffer buffer,
                                          Column column,
                                          int precision) throws SQLException {
        Object value = column.getData();
        if (value == null) {
            buffer.appendNull();
            return;
        }

        switch (column.getType()) {
            case TSDB_DATA_TYPE_BOOL:
                if (value instanceof Boolean) {
                    buffer.appendBool((Boolean) value);
                } else if (value instanceof Number) {
                    buffer.appendBool(((Number) value).intValue() != 0);
                } else {
                    throw unsupportedJavaType(column.getType(), value);
                }
                return;
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                if (value instanceof Boolean) {
                    buffer.appendTinyInt((byte) ((Boolean) value ? 1 : 0));
                } else if (value instanceof Number) {
                    buffer.appendTinyInt(((Number) value).byteValue());
                } else {
                    throw unsupportedJavaType(column.getType(), value);
                }
                return;
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                if (value instanceof Boolean) {
                    buffer.appendSmallInt((short) ((Boolean) value ? 1 : 0));
                } else if (value instanceof Number) {
                    buffer.appendSmallInt(((Number) value).shortValue());
                } else {
                    throw unsupportedJavaType(column.getType(), value);
                }
                return;
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                if (value instanceof Boolean) {
                    buffer.appendInt((Boolean) value ? 1 : 0);
                } else if (value instanceof Number) {
                    buffer.appendInt(((Number) value).intValue());
                } else {
                    throw unsupportedJavaType(column.getType(), value);
                }
                return;
            case TSDB_DATA_TYPE_BIGINT:
                if (value instanceof Boolean) {
                    buffer.appendBigInt((Boolean) value ? 1L : 0L);
                } else if (value instanceof Number) {
                    buffer.appendBigInt(((Number) value).longValue());
                } else {
                    throw unsupportedJavaType(column.getType(), value);
                }
                return;
            case TSDB_DATA_TYPE_UBIGINT:
                if (value instanceof BigInteger) {
                    buffer.appendUBigInt(((BigInteger) value).longValue());
                } else if (value instanceof Number) {
                    buffer.appendUBigInt(((Number) value).longValue());
                } else {
                    throw unsupportedJavaType(column.getType(), value);
                }
                return;
            case TSDB_DATA_TYPE_FLOAT:
                if (value instanceof Boolean) {
                    buffer.appendFloat((Boolean) value ? 1.0f : 0.0f);
                } else if (value instanceof Number) {
                    buffer.appendFloat(((Number) value).floatValue());
                } else {
                    throw unsupportedJavaType(column.getType(), value);
                }
                return;
            case TSDB_DATA_TYPE_DOUBLE:
                if (value instanceof Boolean) {
                    buffer.appendDouble((Boolean) value ? 1.0d : 0.0d);
                } else if (value instanceof Number) {
                    buffer.appendDouble(((Number) value).doubleValue());
                } else {
                    throw unsupportedJavaType(column.getType(), value);
                }
                return;
            case TSDB_DATA_TYPE_TIMESTAMP:
                if (value instanceof Instant) {
                    buffer.appendTimestamp(DateTimeUtils.toLong((Instant) value, precision));
                    return;
                }
                if (value instanceof Timestamp) {
                    buffer.appendTimestamp(DateTimeUtils.toLong(((Timestamp) value).toInstant(), precision));
                    return;
                }
                if (value instanceof Number) {
                    buffer.appendTimestamp(((Number) value).longValue());
                    return;
                }
                throw unsupportedJavaType(column.getType(), value);
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_BLOB:
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_DECIMAL64:
            case TSDB_DATA_TYPE_DECIMAL128:
                if (value instanceof String) {
                    buffer.appendString((String) value);
                    return;
                }
                if (value instanceof Blob) {
                    buffer.appendBytes(BlobUtil.getBytes((Blob) value));
                    return;
                }
                if (value instanceof byte[]) {
                    buffer.appendBytes((byte[]) value);
                    return;
                }
                throw unsupportedJavaType(column.getType(), value);
            default:
                throw new SQLException(
                        "Unsupported field type for WSEW columnar serialization: " + column.getType());
        }
    }

    private static SQLException unsupportedJavaType(int fieldType, Object value) {
        return new SQLException(
                "Unsupported java value type " + value.getClass().getName() + " for field type " + fieldType);
    }

    private static void releaseColumnBuffers(Stmt2ColumnFieldBuffer[] columnBuffers) {
        if (columnBuffers == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer buffer : columnBuffers) {
            if (buffer != null) {
                buffer.release();
            }
        }
    }

    static final class ColumnarWSEWSerializationTask extends RecursiveAction {
        private final ArrayBlockingQueue<Map<Integer, Column>> writeQueue;
        private final ArrayBlockingQueue<EWRawBlock> serialQueue;
        private final AtomicBoolean running;
        private final EWBackendThreadInfo backendThreadInfo;
        private final int batchSize;
        private final StmtInfo stmtInfo;
        private final boolean progressive;
        final WSEWChunkSizingUtil.FieldBatchStats[] batchStats;

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
            this.progressive = progressive;
            this.batchStats = new WSEWChunkSizingUtil.FieldBatchStats[stmtInfo.getFields().size()];
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
                        resetBatchStats(batchStats);
                        columnBuffers = ensureReusableColumnBuffers(backendThreadInfo, stmtInfo);
                        columnBuffers = buildColumnBuffersFromQueuedRows(
                                rows,
                                stmtInfo,
                                columnBuffers,
                                batchStats);
                        backendThreadInfo.setReusableColumnBuffers(columnBuffers);
                        updateNextBufferSpecs(backendThreadInfo, stmtInfo, columnBuffers, batchStats, batchSize);
                        byte[] payload = Stmt2ColumnBindSerializer.serialize(columnBuffers);
                        rawBlock = Stmt2BindExecRequestBuilder.build(payload);
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
                        resetColumnBuffers(columnBuffers);
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

        private static void resetBatchStats(WSEWChunkSizingUtil.FieldBatchStats[] batchStats) {
            for (WSEWChunkSizingUtil.FieldBatchStats stat : batchStats) {
                if (stat != null) {
                    stat.reset();
                }
            }
        }

        private static Stmt2ColumnFieldBuffer[] ensureReusableColumnBuffers(
                EWBackendThreadInfo backendThreadInfo,
                StmtInfo stmtInfo) {
            int fieldCount = stmtInfo.getFields().size();
            ensureSizingState(backendThreadInfo, fieldCount);
            WSEWChunkSizingUtil.BufferSpec[] nextSpecs = backendThreadInfo.getNextBufferSpecs();
            Stmt2ColumnFieldBuffer[] buffers = backendThreadInfo.getReusableColumnBuffers();
            if (!matchesBufferSpecs(stmtInfo, buffers, nextSpecs)) {
                backendThreadInfo.releaseReusableColumnBuffers();
                buffers = newReusableColumnBuffers(stmtInfo, nextSpecs);
                backendThreadInfo.setReusableColumnBuffers(buffers);
            }
            return buffers;
        }

        private static void ensureSizingState(EWBackendThreadInfo backendThreadInfo, int fieldCount) {
            if (backendThreadInfo.getNextBufferSpecs() == null
                    || backendThreadInfo.getNextBufferSpecs().length != fieldCount) {
                WSEWChunkSizingUtil.BufferSpec[] nextSpecs = new WSEWChunkSizingUtil.BufferSpec[fieldCount];
                WSEWChunkSizingUtil.BufferSpec[] previous = backendThreadInfo.getNextBufferSpecs();
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

        private static boolean matchesBufferSpecs(
                StmtInfo stmtInfo,
                Stmt2ColumnFieldBuffer[] buffers,
                WSEWChunkSizingUtil.BufferSpec[] nextSpecs) {
            if (buffers == null || buffers.length != stmtInfo.getFields().size()) {
                return false;
            }
            for (int i = 0; i < buffers.length; i++) {
                if (!buffers[i].getMeta().isVariableWidth()) {
                    continue;
                }
                if (!bufferSpecsEqual(buffers[i].currentReusableSpec(), resolveBufferSpec(nextSpecs, i))) {
                    return false;
                }
            }
            return true;
        }

        private static void updateNextBufferSpecs(
                EWBackendThreadInfo backendThreadInfo,
                StmtInfo stmtInfo,
                Stmt2ColumnFieldBuffer[] columnBuffers,
                WSEWChunkSizingUtil.FieldBatchStats[] batchStats,
                int batchSize) {
            ensureSizingState(backendThreadInfo, stmtInfo.getFields().size());
            WSEWChunkSizingUtil.BufferSpec[] nextSpecs = backendThreadInfo.getNextBufferSpecs();
            int[] underuseStreaks = backendThreadInfo.getUnderuseStreaks();
            for (int i = 0; i < columnBuffers.length; i++) {
                if (!columnBuffers[i].getMeta().isVariableWidth()) {
                    nextSpecs[i] = null;
                    underuseStreaks[i] = 0;
                    continue;
                }

                WSEWChunkSizingUtil.FieldBatchStats stats = batchStats[i];
                WSEWChunkSizingUtil.BufferSpec current = columnBuffers[i].currentReusableSpec();
                if (current == null) {
                    current = resolveBufferSpec(nextSpecs, i);
                }
                if (stats == null || current == null) {
                    nextSpecs[i] = current;
                    underuseStreaks[i] = 0;
                    continue;
                }

                stats.setActiveChunksUsed(columnBuffers[i].activeReusableChunkCount());
                WSEWChunkSizingUtil.BufferSpec wanted =
                        WSEWChunkSizingUtil.deriveWantedSpec(stats, batchSize);
                if (bufferSpecsEqual(current, wanted)) {
                    nextSpecs[i] = current;
                    underuseStreaks[i] = 0;
                } else if (!WSEWChunkSizingUtil.canReuse(
                        current, wanted, stats, batchSize, EW_TARGET_ACTIVE_CHUNKS)) {
                    nextSpecs[i] = wanted;
                    underuseStreaks[i] = 0;
                } else if (isSmallerSpec(wanted, current)) {
                    int streak = underuseStreaks[i] + 1;
                    if (WSEWChunkSizingUtil.shouldShrink(current, wanted, streak)) {
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

        private static boolean bufferSpecsEqual(
                WSEWChunkSizingUtil.BufferSpec left,
                WSEWChunkSizingUtil.BufferSpec right) {
            if (left == right) {
                return true;
            }
            if (left == null || right == null) {
                return false;
            }
            return left.getChunkBytes() == right.getChunkBytes()
                    && left.getReusableChunkCount() == right.getReusableChunkCount();
        }

        private static boolean isSmallerSpec(
                WSEWChunkSizingUtil.BufferSpec candidate,
                WSEWChunkSizingUtil.BufferSpec current) {
            long candidateBytes =
                    (long) candidate.getChunkBytes() * candidate.getReusableChunkCount();
            long currentBytes =
                    (long) current.getChunkBytes() * current.getReusableChunkCount();
            return candidateBytes < currentBytes;
        }
    }
}
