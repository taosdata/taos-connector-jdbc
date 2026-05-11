package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.utils.BlobUtil;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.stmt2.Stmt2BindExecRequestBuilder;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;

public class WSColumnFastPreparedStatement extends WSColumnPreparedStatement implements PreparedStatement {

    private final Stmt2FieldMeta[] fieldMetas;
    private final byte[] fixedWidths;
    private final int tbNameFieldIdx;
    private Stmt2ColumnFieldBuffer[] columnBuffers;
    private Stmt2ColumnFieldBuffer.BufferSizeHints[] bufferSizeHints;
    private int expectedRowCount;

    public WSColumnFastPreparedStatement(Transport transport,
                                         ConnectionParam param,
                                         String database,
                                         AbstractConnection connection,
                                         String sql,
                                         Long instanceId,
                                         Stmt2PrepareResp prepareResp) {
        super(transport, param, database, connection, sql, instanceId, prepareResp);

        List<Field> fields = stmtInfo.getFields();
        int n = fields != null ? fields.size() : 0;
        this.fieldMetas = new Stmt2FieldMeta[n];
        this.fixedWidths = new byte[n];
        for (int i = 0; i < n; i++) {
            fieldMetas[i] = Stmt2FieldMeta.fromField(fields.get(i));
            fixedWidths[i] = (byte) fieldMetas[i].fixedWidth();
        }
        this.tbNameFieldIdx = resolveTbNameFieldIdx(fieldMetas);
        this.columnBuffers = allocateColumnBuffers();
    }

    private void stageFixed(int paramIdx, long value) throws SQLException {
        switch (fixedWidths[paramIdx]) {
            case 1:
                appendNonTbNameValue(paramIdx, new ColumnAppender() {
                    @Override
                    public void append(Stmt2ColumnFieldBuffer buffer) throws SQLException {
                        buffer.appendTinyInt((byte) value);
                    }
                });
                break;
            case 2:
                appendNonTbNameValue(paramIdx, new ColumnAppender() {
                    @Override
                    public void append(Stmt2ColumnFieldBuffer buffer) throws SQLException {
                        buffer.appendSmallInt((short) value);
                    }
                });
                break;
            case 4:
                appendNonTbNameValue(paramIdx, new ColumnAppender() {
                    @Override
                    public void append(Stmt2ColumnFieldBuffer buffer) throws SQLException {
                        buffer.appendFixed4Raw((int) value);
                    }
                });
                break;
            case 8:
                appendNonTbNameValue(paramIdx, new ColumnAppender() {
                    @Override
                    public void append(Stmt2ColumnFieldBuffer buffer) throws SQLException {
                        buffer.appendFixed8Raw(value);
                    }
                });
                break;
            default:
                throw new IllegalStateException("Not a fixed-width field at index " + paramIdx);
        }
    }

    private void stageVar(int paramIdx, byte[] bytes) throws SQLException {
        stageVar(paramIdx, bytes, bytes == null ? 0 : bytes.length);
    }

    private void stageVar(int paramIdx, byte[] bytes, int length) throws SQLException {
        if (paramIdx == tbNameFieldIdx && (bytes == null || length <= 0)) {
            throw tableNameRequiredException();
        }
        appendValue(paramIdx, new ColumnAppender() {
            @Override
            public void append(Stmt2ColumnFieldBuffer buffer) throws SQLException {
                buffer.appendEncodedVar(bytes, length);
            }
        });
    }

    private void stageString(int paramIdx, String value) throws SQLException {
        if (paramIdx == tbNameFieldIdx && (value == null || value.isEmpty())) {
            throw tableNameRequiredException();
        }
        appendValue(paramIdx, new ColumnAppender() {
            @Override
            public void append(Stmt2ColumnFieldBuffer buffer) throws SQLException {
                buffer.appendString(value);
            }
        });
    }

    private void stageNull(int paramIdx) throws SQLException {
        appendValue(paramIdx, new ColumnAppender() {
            @Override
            public void append(Stmt2ColumnFieldBuffer buffer) throws SQLException {
                buffer.appendNull();
            }
        });
    }

    private void stageTimestamp(int parameterIndex, long epochValue) throws SQLException {
        appendNonTbNameValue(parameterIndex - 1, new ColumnAppender() {
            @Override
            public void append(Stmt2ColumnFieldBuffer buffer) throws SQLException {
                buffer.appendTimestamp(epochValue);
            }
        });
    }

    private byte fieldType(int parameterIndex) {
        return fieldMetas[parameterIndex - 1].getFieldType();
    }

    private byte bindType(int parameterIndex) {
        return fieldMetas[parameterIndex - 1].getBindType();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        stageFixed(parameterIndex - 1, x ? 1L : 0L);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        stageFixed(parameterIndex - 1, x & 0xFFL);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        byte ft = fieldType(parameterIndex);
        if (ft == TSDB_DATA_TYPE_UTINYINT) {
            if (x < 0 || x > MAX_UNSIGNED_BYTE) {
                throw TSDBError.createSQLException(
                        TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "utinyint value is out of range");
            }
            stageFixed(parameterIndex - 1, x & 0xFFL);
        } else {
            stageFixed(parameterIndex - 1, x);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        byte ft = fieldType(parameterIndex);
        if (ft == TSDB_DATA_TYPE_USMALLINT) {
            if (x < 0 || x > MAX_UNSIGNED_SHORT) {
                throw TSDBError.createSQLException(
                        TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "usmallint value is out of range");
            }
            stageFixed(parameterIndex - 1, x & 0xFFFFL);
        } else {
            stageFixed(parameterIndex - 1, x);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        byte ft = fieldType(parameterIndex);
        if (ft == TSDB_DATA_TYPE_UINT) {
            if (x < 0 || x > MAX_UNSIGNED_INT) {
                throw TSDBError.createSQLException(
                        TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "uint value is out of range");
            }
            stageFixed(parameterIndex - 1, x & 0xFFFFFFFFL);
        } else {
            stageFixed(parameterIndex - 1, x);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        stageFixed(parameterIndex - 1, Float.floatToRawIntBits(x) & 0xFFFFFFFFL);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        stageFixed(parameterIndex - 1, Double.doubleToRawLongBits(x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x == null) {
            stageNull(parameterIndex - 1);
            return;
        }
        long ts = DateTimeUtils.toLong(DateTimeUtils.toInstant(x, this.zoneId), stmtInfo.getPrecision());
        stageTimestamp(parameterIndex, ts);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (x == null) {
            stageNull(parameterIndex - 1);
            return;
        }
        Instant instant = DateTimeUtils.toInstant(x, cal);
        long ts = DateTimeUtils.toLong(instant, stmtInfo.getPrecision());
        stageTimestamp(parameterIndex, ts);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x == null) {
            stageNull(parameterIndex - 1);
            return;
        }
        setTimestamp(parameterIndex, new Timestamp(x.getTime()));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null) {
            stageNull(parameterIndex - 1);
            return;
        }
        setTimestamp(parameterIndex, new Timestamp(x.getTime()));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        int idx = parameterIndex - 1;
        if (bindType(parameterIndex) == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue() && x == null) {
            throw TSDBError.createSQLException(
                    TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name can't be null");
        }
        stageString(idx, x);
    }

    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        int idx = parameterIndex - 1;
        if (bindType(parameterIndex) == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue() && x == null) {
            throw TSDBError.createSQLException(
                    TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name can't be null");
        }
        stageString(idx, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        int idx = parameterIndex - 1;
        if (bindType(parameterIndex) == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue() && x == null) {
            throw TSDBError.createSQLException(
                    TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name can't be null");
        }
        stageVar(idx, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (x == null) {
            stageNull(parameterIndex - 1);
            return;
        }
        stageVar(parameterIndex - 1, x.toPlainString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkBlobSupport();
        if (x == null) {
            stageNull(parameterIndex - 1);
            return;
        }
        stageVar(parameterIndex - 1, x.getBytes(1, (int) x.length()));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        checkBlobSupport();
        stageVar(parameterIndex - 1, BlobUtil.getFromInputStream(inputStream, length));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        checkBlobSupport();
        stageVar(parameterIndex - 1, BlobUtil.getFromInputStream(inputStream));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        stageNull(parameterIndex - 1);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        stageNull(parameterIndex - 1);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
            return;
        }
        switch (targetSqlType) {
            case Types.BOOLEAN:
                if (x instanceof Boolean) {
                    setBoolean(parameterIndex, (Boolean) x);
                } else if (x instanceof Number) {
                    setBoolean(parameterIndex, ((Number) x).intValue() != 0);
                } else {
                    throw new SQLException("Invalid type for boolean: " + x.getClass().getName());
                }
                break;
            case Types.TINYINT:
                if (x instanceof Number) {
                    setByte(parameterIndex, ((Number) x).byteValue());
                } else if (x instanceof Boolean) {
                    setByte(parameterIndex, (byte) ((Boolean) x ? 1 : 0));
                } else {
                    throw new SQLException("Invalid type for byte: " + x.getClass().getName());
                }
                break;
            case Types.SMALLINT:
                if (x instanceof Number) {
                    setShort(parameterIndex, ((Number) x).shortValue());
                } else if (x instanceof Boolean) {
                    setShort(parameterIndex, (short) ((Boolean) x ? 1 : 0));
                } else {
                    throw new SQLException("Invalid type for short: " + x.getClass().getName());
                }
                break;
            case Types.INTEGER:
                if (x instanceof Number) {
                    setInt(parameterIndex, ((Number) x).intValue());
                } else if (x instanceof Boolean) {
                    setInt(parameterIndex, (Boolean) x ? 1 : 0);
                } else {
                    throw new SQLException("Invalid type for int: " + x.getClass().getName());
                }
                break;
            case Types.BIGINT:
                if (x instanceof Number) {
                    setLong(parameterIndex, ((Number) x).longValue());
                } else if (x instanceof Boolean) {
                    setLong(parameterIndex, (Boolean) x ? 1 : 0);
                } else {
                    throw new SQLException("Invalid type for long: " + x.getClass().getName());
                }
                break;
            case Types.FLOAT:
                if (x instanceof Number) {
                    setFloat(parameterIndex, ((Number) x).floatValue());
                } else if (x instanceof Boolean) {
                    setFloat(parameterIndex, (Boolean) x ? 1 : 0);
                } else {
                    throw new SQLException("Invalid type for float: " + x.getClass().getName());
                }
                break;
            case Types.DOUBLE:
                if (x instanceof Number) {
                    setDouble(parameterIndex, ((Number) x).doubleValue());
                } else if (x instanceof Boolean) {
                    setDouble(parameterIndex, (Boolean) x ? 1 : 0);
                } else {
                    throw new SQLException("Invalid type for double: " + x.getClass().getName());
                }
                break;
            case Types.TIMESTAMP:
                if (x instanceof Timestamp) {
                    setTimestamp(parameterIndex, (Timestamp) x);
                } else if (x instanceof Date) {
                    setDate(parameterIndex, (Date) x);
                } else if (x instanceof Time) {
                    setTime(parameterIndex, (Time) x);
                } else if (x instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) x;
                    if (zoneId == null) {
                        setTimestamp(parameterIndex, Timestamp.valueOf(ldt));
                    } else {
                        ZonedDateTime zdt = ldt.atZone(zoneId);
                        long ts = DateTimeUtils.toLong(zdt.toInstant(), stmtInfo.getPrecision());
                        stageTimestamp(parameterIndex, ts);
                    }
                } else if (x instanceof Instant) {
                    stageTimestamp(parameterIndex,
                            DateTimeUtils.toLong((Instant) x, stmtInfo.getPrecision()));
                } else if (x instanceof ZonedDateTime) {
                    stageTimestamp(parameterIndex,
                            DateTimeUtils.toLong((ZonedDateTime) x, stmtInfo.getPrecision()));
                } else if (x instanceof OffsetDateTime) {
                    stageTimestamp(parameterIndex,
                            DateTimeUtils.toLong((OffsetDateTime) x, stmtInfo.getPrecision()));
                } else {
                    throw new SQLException("Invalid type for timestamp: " + x.getClass().getName());
                }
                break;
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.OTHER:
                if (x instanceof byte[]) {
                    setBytes(parameterIndex, (byte[]) x);
                } else if (x instanceof String) {
                    setString(parameterIndex, (String) x);
                } else {
                    throw new SQLException("Invalid type for binary: " + x.getClass().getName());
                }
                break;
            case Types.BLOB:
                if (x instanceof byte[]) {
                    setBytes(parameterIndex, (byte[]) x);
                } else if (x instanceof Blob) {
                    setBytes(parameterIndex, ((Blob) x).getBytes(1, (int) ((Blob) x).length()));
                } else {
                    throw new SQLException("Invalid type for blob: " + x.getClass().getName());
                }
                break;
            case Types.DECIMAL:
                if (x instanceof BigDecimal) {
                    setBigDecimal(parameterIndex, (BigDecimal) x);
                } else {
                    throw new SQLException("Invalid type for decimal: " + x.getClass().getName());
                }
                break;
            default:
                throw new SQLException("unsupported type: " + targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            stageNull(parameterIndex - 1);
            return;
        }
        if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        } else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof LocalDateTime) {
            if (zoneId == null) {
                setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) x));
            } else {
                ZonedDateTime zdt = ((LocalDateTime) x).atZone(zoneId);
                stageTimestamp(parameterIndex, DateTimeUtils.toLong(zdt.toInstant(), stmtInfo.getPrecision()));
            }
        } else if (x instanceof Instant) {
            stageTimestamp(parameterIndex, DateTimeUtils.toLong((Instant) x, stmtInfo.getPrecision()));
        } else if (x instanceof ZonedDateTime) {
            stageTimestamp(parameterIndex, DateTimeUtils.toLong((ZonedDateTime) x, stmtInfo.getPrecision()));
        } else if (x instanceof OffsetDateTime) {
            stageTimestamp(parameterIndex, DateTimeUtils.toLong((OffsetDateTime) x, stmtInfo.getPrecision()));
        } else if (x instanceof BigInteger) {
            BigInteger v = (BigInteger) x;
            if (v.compareTo(BigInteger.ZERO) < 0 || v.compareTo(new BigInteger(MAX_UNSIGNED_LONG)) > 0) {
                throw TSDBError.createSQLException(
                        TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "ubigint value is out of range");
            }
            stageFixed(parameterIndex - 1, v.longValue());
        } else if (x instanceof Blob) {
            stageVar(parameterIndex - 1, ((Blob) x).getBytes(1, (int) ((Blob) x).length()));
        } else if (x instanceof BigDecimal) {
            stageVar(parameterIndex - 1, ((BigDecimal) x).toPlainString().getBytes(StandardCharsets.UTF_8));
        } else {
            throw new SQLException("Unsupported data type: " + x.getClass().getName());
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        throw TSDBError.createSQLException(
                TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD,
                "clearParameters is not supported in stmt2 fast mode");
    }

    @Override
    public void addBatch() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        expectedRowCount++;
    }

    @Override
    public void clearBatch() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        resetFastState();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        checkBatchRowCountsOrReset();
        int affected = executeInsertImpl();
        int[] result = new int[affected];
        Arrays.fill(result, SUCCESS_NO_INFO);
        return result;
    }

    @Override
    public boolean execute() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        executeUpdate();
        return false;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        if (!stmtInfo.isInsert()) {
            throw TSDBError.createSQLException(
                    TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "The insert SQL must be prepared.");
        }
        if (stmtInfo.getFields() == null || stmtInfo.getFields().isEmpty()) {
            return this.executeUpdate(stmtInfo.getSql());
        }
        checkSingleExecuteRowCountsOrReset();
        return executeInsertImpl();
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            try {
                super.close();
            } finally {
                releaseColumnBuffers();
                columnBuffers = null;
                bufferSizeHints = null;
                expectedRowCount = 0;
            }
        }
    }

    private int executeInsertImpl() throws SQLException {
        ByteBuf rawBuf = null;
        try {
            rawBuf = Stmt2BindExecRequestBuilder.build(buildPayloadBuffer());
            this.affectedRows = 0;
            writeBlockWithRetrySync(rawBuf);
            this.affectedRows = batchInsertedRowsInner.getAndSet(0);
        } finally {
            if (rawBuf != null) {
                rawBuf.release();
            }
            resetFastState();
        }
        return this.affectedRows;
    }

    private Stmt2ColumnFieldBuffer[] allocateColumnBuffers() {
        Stmt2ColumnFieldBuffer[] buffers = new Stmt2ColumnFieldBuffer[fieldMetas.length];
        for (int i = 0; i < fieldMetas.length; i++) {
            buffers[i] = new Stmt2ColumnFieldBuffer(
                    fieldMetas[i],
                    bufferSizeHints == null ? null : bufferSizeHints[i]);
        }
        return buffers;
    }

    private void resetFastState() {
        bufferSizeHints = snapshotBufferSizeHints();
        releaseColumnBuffers();
        expectedRowCount = 0;
        columnBuffers = allocateColumnBuffers();
    }

    private void releaseColumnBuffers() {
        if (columnBuffers == null) {
            return;
        }
        for (Stmt2ColumnFieldBuffer columnBuffer : columnBuffers) {
            if (columnBuffer != null) {
                columnBuffer.release();
            }
        }
    }

    private Stmt2ColumnFieldBuffer.BufferSizeHints[] snapshotBufferSizeHints() {
        if (columnBuffers == null) {
            return null;
        }
        Stmt2ColumnFieldBuffer.BufferSizeHints[] hints =
                new Stmt2ColumnFieldBuffer.BufferSizeHints[columnBuffers.length];
        for (int i = 0; i < columnBuffers.length; i++) {
            hints[i] = columnBuffers[i].snapshotUsage();
        }
        return hints;
    }

    private int resolvedTableCount() throws SQLException {
        if (tbNameFieldIdx < 0) {
            return 1;
        }
        int tableCount = columnBuffers[tbNameFieldIdx].computeTableCount();
        if (expectedRowCount > 0 && tableCount <= 0) {
            throw tableNameRequiredException();
        }
        return tableCount;
    }

    private byte[] buildPayload() throws SQLException {
        ByteBuf payload = buildPayloadBuffer();
        try {
            return ByteBufUtil.getBytes(payload);
        } finally {
            Utils.releaseByteBuf(payload);
        }
    }

    private ByteBuf buildPayloadBuffer() throws SQLException {
        return Stmt2ColumnBindSerializer.serializeBuffer(columnBuffers, resolvedTableCount());
    }

    private void checkBatchRowCountsOrReset() throws SQLException {
        for (int i = 0; i < columnBuffers.length; i++) {
            int actual = columnBuffers[i].getRowCount();
            if (actual != expectedRowCount) {
                int expected = expectedRowCount;
                resetFastState();
                throw new SQLException(
                        "row count mismatch at column " + i + ": expected " + expected + ", got " + actual);
            }
        }
    }

    private void checkSingleExecuteRowCountsOrReset() throws SQLException {
        for (int i = 0; i < columnBuffers.length; i++) {
            int actual = columnBuffers[i].getRowCount();
            if (actual != 1) {
                resetFastState();
                throw new SQLException("row count mismatch at column " + i + ": expected 1, got " + actual);
            }
        }
    }

    private void appendValue(int index, ColumnAppender appender) throws SQLException {
        appender.append(columnBuffers[index]);
    }

    private void appendNonTbNameValue(int index, ColumnAppender appender) throws SQLException {
        if (index == tbNameFieldIdx) {
            throw tableNameRequiredException();
        }
        appendValue(index, appender);
    }

    private SQLException tableNameRequiredException() {
        return new SQLException("Table name not set for row; call setString on the tbname parameter");
    }

    private static int resolveTbNameFieldIdx(Stmt2FieldMeta[] metas) {
        int tbIdx = -1;
        for (int i = 0; i < metas.length; i++) {
            if (metas[i].getBindType() == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
                tbIdx = i;
            }
        }
        return tbIdx;
    }

    int getExpectedRowCount() {
        return expectedRowCount;
    }

    Stmt2ColumnFieldBuffer getColumnBuffer(int fieldIndex) {
        return columnBuffers[fieldIndex];
    }

    int getTbNameFieldIdx() {
        return tbNameFieldIdx;
    }

    private interface ColumnAppender {
        void append(Stmt2ColumnFieldBuffer buffer) throws SQLException;
    }
}
