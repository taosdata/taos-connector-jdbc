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
import com.taosdata.jdbc.ws.stmt2.entity.EWBackendThreadInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWRawBlock;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;

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

    public WSEWColumnPreparedStatement(Transport transport,
                                       ConnectionParam param,
                                       String database,
                                       AbstractConnection connection,
                                       String sql,
                                       Long instanceId,
                                       Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp, true);
    }

    @Override
    protected RecursiveAction newSerializationTask(EWBackendThreadInfo backendThreadInfo,
                                                   int batchSize,
                                                   boolean progressive) {
        return new ColumnarWSEWSerializationTask(backendThreadInfo, batchSize, stmtInfo, progressive);
    }

    static Stmt2ColumnFieldBuffer[] buildColumnBuffersFromQueuedRows(List<Map<Integer, Column>> rows,
                                                                     StmtInfo stmtInfo) throws SQLException {
        Stmt2ColumnFieldBuffer[] buffers = new Stmt2ColumnFieldBuffer[stmtInfo.getFields().size()];
        boolean success = false;
        try {
            for (int i = 0; i < stmtInfo.getFields().size(); i++) {
                buffers[i] = new Stmt2ColumnFieldBuffer(Stmt2FieldMeta.fromField(stmtInfo.getFields().get(i)));
            }

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
            success = true;
            return buffers;
        } finally {
            if (!success) {
                releaseColumnBuffers(buffers);
            }
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
        private final int batchSize;
        private final StmtInfo stmtInfo;
        private final boolean progressive;

        ColumnarWSEWSerializationTask(EWBackendThreadInfo backendThreadInfo,
                                      int batchSize,
                                      StmtInfo stmtInfo,
                                      boolean progressive) {
            this.writeQueue = backendThreadInfo.getWriteQueue();
            this.serialQueue = backendThreadInfo.getSerialQueue();
            this.running = backendThreadInfo.getSerializeRunning();
            this.batchSize = batchSize;
            this.stmtInfo = stmtInfo;
            this.progressive = progressive;
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
                        columnBuffers = buildColumnBuffersFromQueuedRows(rows, stmtInfo);
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
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } finally {
                        releaseColumnBuffers(columnBuffers);
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
    }
}
