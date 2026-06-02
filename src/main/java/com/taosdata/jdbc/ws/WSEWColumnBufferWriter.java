package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.utils.BlobUtil;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import com.taosdata.jdbc.ws.stmt2.Stmt2VariableWidthReuseHelper;
import com.taosdata.jdbc.ws.stmt2.Stmt2ChunkSizingUtil;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBufUtil;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

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

final class WSEWColumnBufferWriter {
    private final StmtInfo stmtInfo;
    private final int fieldCount;
    private final int tbNameFieldIdx;
    private final int precision;

    WSEWColumnBufferWriter(StmtInfo stmtInfo) {
        this.stmtInfo = stmtInfo;
        this.fieldCount = stmtInfo.getFields().size();
        this.tbNameFieldIdx = stmtInfo.getToBeBindTableNameIndex();
        this.precision = stmtInfo.getPrecision();
    }

    Stmt2ColumnFieldBuffer[] buildFromQueuedRows(
            List<Map<Integer, Column>> rows,
            Stmt2ColumnFieldBuffer[] reusableBuffers,
            Stmt2ChunkSizingUtil.FieldBatchStats[] stats) throws SQLException {
        if (stats != null && stats.length < fieldCount) {
            throw new IllegalArgumentException(
                    "stats array length " + stats.length
                            + " is less than field count " + fieldCount
                            + "; caller must provide stats.length >= fieldCount");
        }
        Stmt2ColumnFieldBuffer[] buffers = requireReusableColumnBuffers(reusableBuffers);
        resetColumnBuffers(buffers);

        boolean success = false;
        try {
            fillColumnBuffersFromQueuedRows(rows, buffers, stats);
            success = true;
            return buffers;
        } finally {
            if (!success) {
                resetColumnBuffers(buffers);
            }
        }
    }

    Stmt2ColumnFieldBuffer[] newReusableColumnBuffers(Stmt2ChunkSizingUtil.BufferSpec[] bufferSpecs) {
        Stmt2ColumnFieldBuffer[] buffers = new Stmt2ColumnFieldBuffer[fieldCount];
        boolean success = false;
        try {
            for (int i = 0; i < fieldCount; i++) {
                Field field = stmtInfo.getFields().get(i);
                Stmt2FieldMeta meta = Stmt2FieldMeta.fromField(field);
                if (meta.isVariableWidth()) {
                    Stmt2ChunkSizingUtil.BufferSpec spec =
                            Stmt2VariableWidthReuseHelper.resolveBufferSpec(bufferSpecs, i);
                    buffers[i] = Stmt2VariableWidthReuseHelper.createReusableVariableWidthBuffer(meta, spec);
                } else {
                    buffers[i] = new Stmt2ColumnFieldBuffer(meta);
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

    void resetColumnBuffers(Stmt2ColumnFieldBuffer[] buffers) {
        if (buffers == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer buffer : buffers) {
            if (buffer != null) {
                buffer.reset();
            }
        }
    }

    void releaseColumnBuffers(Stmt2ColumnFieldBuffer[] columnBuffers) {
        if (columnBuffers == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer buffer : columnBuffers) {
            if (buffer != null) {
                buffer.release();
            }
        }
    }

    private Stmt2ColumnFieldBuffer[] requireReusableColumnBuffers(Stmt2ColumnFieldBuffer[] buffers) {
        if (buffers == null) {
            throw new IllegalStateException("Reusable column buffers must be initialized before serialization");
        }
        if (buffers.length != fieldCount) {
            throw new IllegalStateException(
                    "Reusable column buffer count mismatch, expected " + fieldCount + " but got " + buffers.length);
        }
        return buffers;
    }

    private void fillColumnBuffersFromQueuedRows(List<Map<Integer, Column>> rows,
                                                 Stmt2ColumnFieldBuffer[] buffers,
                                                 Stmt2ChunkSizingUtil.FieldBatchStats[] stats) throws SQLException {
        for (Map<Integer, Column> row : rows) {
            for (int i = 0; i < buffers.length; i++) {
                Column column = row.get(i + 1);
                if (column == null) {
                    throw new SQLException("Missing bound column at index " + (i + 1));
                }
                if (i == tbNameFieldIdx) {
                    if (stats != null && stats[i] == null) {
                        stats[i] = new Stmt2ChunkSizingUtil.FieldBatchStats();
                    }
                    if (stats != null && stats[i] != null) {
                        observeWrite(stats[i], column);
                    }
                    appendTbName(buffers[i], column);
                    continue;
                }
                if (stats != null && stats[i] == null && buffers[i].getMeta().isVariableWidth()) {
                    stats[i] = new Stmt2ChunkSizingUtil.FieldBatchStats();
                }
                if (stats != null && stats[i] != null) {
                    observeWrite(stats[i], column);
                }
                appendColumnValue(buffers[i], column);
            }
        }
    }

    private void observeWrite(Stmt2ChunkSizingUtil.FieldBatchStats stats, Column column) {
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

    private void appendTbName(Stmt2ColumnFieldBuffer buffer, Column column) throws SQLException {
        Object value = column.getData();
        if (value instanceof String) {
            buffer.appendTbName((String) value);
            return;
        }
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            buffer.appendTbNameBytes(bytes, bytes.length);
            return;
        }
        throw new SQLException("table name must be string or binary");
    }

    private void appendColumnValue(Stmt2ColumnFieldBuffer buffer, Column column) throws SQLException {
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

    private SQLException unsupportedJavaType(int fieldType, Object value) {
        return new SQLException(
                "Unsupported java value type " + value.getClass().getName() + " for field type " + fieldType);
    }
}
