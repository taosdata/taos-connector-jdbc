package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.BlobUtil;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnBindSerializer;
import com.taosdata.jdbc.ws.stmt2.Stmt2ColumnFieldBuffer;
import com.taosdata.jdbc.ws.stmt2.Stmt2FieldMeta;
import com.taosdata.jdbc.ws.stmt2.entity.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.taosdata.jdbc.TSDBConstants.*;

/**
 * Row-oriented prepared-statement producer that feeds the columnar stmt2 serializer.
 *
 * <p>Each JDBC setter call stages a value in the current-row area. Repeated calls
 * on the same parameter index overwrite the previous staged value (last-setter-wins).
 * {@link #addBatch()} snapshots the complete current row into per-field
 * {@link Stmt2ColumnFieldBuffer}s and increments the expected row count.
 * At execute time, a lightweight O(fieldCount) row-count consistency check is
 * performed before the payload is serialised and sent via {@code stmt2_bind_exec}.
 */
public class WSColumnPreparedStatement extends WSRetryableStmt implements PreparedStatement {

    // -----------------------------------------------------------------------
    // Cached metadata
    // -----------------------------------------------------------------------

    /** One descriptor per prepared field, in prepare order. */
    private final Stmt2FieldMeta[] fieldMetas;

    /** Index of the TAOS_FIELD_TBNAME field; -1 if not a supertable insert. */
    private final int tbNameFieldIdx;

    // -----------------------------------------------------------------------
    // Per-field column buffers (reset after each execute)
    // -----------------------------------------------------------------------

    private Stmt2ColumnFieldBuffer[] columnBuffers;

    // -----------------------------------------------------------------------
    // Current-row staging
    // -----------------------------------------------------------------------

    /** True = slot holds a null value (default for all slots). */
    private final boolean[] currentNull;

    /**
     * Staged fixed-width value for each field, encoded as long.
     * <ul>
     *   <li>BOOL, TINYINT, UTINYINT → bit 0 / byte value</li>
     *   <li>SMALLINT, USMALLINT → short value</li>
     *   <li>INT, UINT → int value (sign-extended to long)</li>
     *   <li>BIGINT, UBIGINT → long value</li>
     *   <li>FLOAT → {@link Float#floatToRawIntBits(float)} cast to long</li>
     *   <li>DOUBLE → {@link Double#doubleToRawLongBits(double)}</li>
     *   <li>TIMESTAMP → epoch value at column precision</li>
     * </ul>
     */
    private final long[] currentFixed;

    /**
     * Staged variable-width bytes for each field.
     * Null if the slot is null or the field is fixed-width.
     */
    private final byte[][] currentVar;

    /** Staged table name string for the TAOS_FIELD_TBNAME field. */
    private String currentTbName;

    // -----------------------------------------------------------------------
    // Batch tracking
    // -----------------------------------------------------------------------

    /** Number of rows that have been flushed to column buffers via addBatch(). */
    private int expectedRowCount;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    public WSColumnPreparedStatement(Transport transport,
                                     ConnectionParam param,
                                     String database,
                                     AbstractConnection connection,
                                     String sql,
                                     Long instanceId,
                                     Stmt2PrepareResp prepareResp) {
        super(connection, param, database, transport, instanceId,
                new StmtInfo(prepareResp, sql), new AtomicInteger(), true);

        List<Field> fields = stmtInfo.getFields();
        int n = (fields != null) ? fields.size() : 0;
        this.fieldMetas = new Stmt2FieldMeta[n];
        int tbIdx = -1;
        for (int i = 0; i < n; i++) {
            fieldMetas[i] = Stmt2FieldMeta.fromField(fields.get(i));
            if (fields.get(i).getBindType() == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
                tbIdx = i;
            }
        }
        this.tbNameFieldIdx = tbIdx;

        this.currentNull = new boolean[n];
        this.currentFixed = new long[n];
        this.currentVar = new byte[n][];

        Arrays.fill(currentNull, true); // all slots start as null

        this.columnBuffers = allocateColumnBuffers();
    }

    // -----------------------------------------------------------------------
    // Metadata helpers
    // -----------------------------------------------------------------------

    private Stmt2ColumnFieldBuffer[] allocateColumnBuffers() {
        Stmt2ColumnFieldBuffer[] bufs = new Stmt2ColumnFieldBuffer[fieldMetas.length];
        for (int i = 0; i < fieldMetas.length; i++) {
            bufs[i] = new Stmt2ColumnFieldBuffer(fieldMetas[i]);
        }
        return bufs;
    }

    // -----------------------------------------------------------------------
    // Staging helpers
    // -----------------------------------------------------------------------

    private void stageFixed(int paramIdx, long value) {
        currentNull[paramIdx] = false;
        currentFixed[paramIdx] = value;
    }

    private void stageVar(int paramIdx, byte[] bytes) {
        if (bytes == null) {
            currentNull[paramIdx] = true;
            currentVar[paramIdx] = null;
        } else {
            currentNull[paramIdx] = false;
            currentVar[paramIdx] = bytes;
        }
    }

    private void stageNull(int paramIdx) {
        currentNull[paramIdx] = true;
        currentVar[paramIdx] = null;
        currentFixed[paramIdx] = 0L;
    }

    /** Resolves the TDengine field type for the given 1-based parameter index. */
    private byte fieldType(int parameterIndex) {
        return fieldMetas[parameterIndex - 1].getFieldType();
    }

    /** Returns the bind type for the given 1-based parameter index. */
    private byte bindType(int parameterIndex) {
        return fieldMetas[parameterIndex - 1].getBindType();
    }

    // -----------------------------------------------------------------------
    // Row flush
    // -----------------------------------------------------------------------

    /**
     * Flushes the current staged row into the per-field column buffers.
     * Resets all staging state afterwards.
     */
    private void flushCurrentRow() throws SQLException {
        for (int i = 0; i < fieldMetas.length; i++) {
            Stmt2FieldMeta meta = fieldMetas[i];
            Stmt2ColumnFieldBuffer buf = columnBuffers[i];

            if (meta.getBindType() == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
                if (currentTbName == null) {
                    throw new SQLException(
                            "Table name not set for row; call setString on the tbname parameter");
                }
                buf.appendTbName(currentTbName);
                continue;
            }

            if (currentNull[i]) {
                buf.appendNull();
            } else if (meta.isVariableWidth()) {
                buf.appendBytes(currentVar[i]);
            } else {
                appendFixedValue(buf, meta, currentFixed[i]);
            }
        }
        // Reset staging state for next row
        Arrays.fill(currentNull, true);
        Arrays.fill(currentVar, null);
        Arrays.fill(currentFixed, 0L);
        currentTbName = null;
    }

    private static void appendFixedValue(Stmt2ColumnFieldBuffer buf,
                                          Stmt2FieldMeta meta,
                                          long value) throws SQLException {
        switch (meta.getFieldType() & 0xFF) {
            case TSDB_DATA_TYPE_BOOL:
                buf.appendBool(value != 0);
                break;
            case TSDB_DATA_TYPE_TINYINT:
                buf.appendTinyInt((byte) value);
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                buf.appendUTinyInt((byte) value);
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                buf.appendSmallInt((short) value);
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                buf.appendUSmallInt((short) value);
                break;
            case TSDB_DATA_TYPE_INT:
                buf.appendInt((int) value);
                break;
            case TSDB_DATA_TYPE_UINT:
                buf.appendUInt((int) value);
                break;
            case TSDB_DATA_TYPE_BIGINT:
                buf.appendBigInt(value);
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                buf.appendUBigInt(value);
                break;
            case TSDB_DATA_TYPE_FLOAT:
                buf.appendFloat(Float.intBitsToFloat((int) value));
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                buf.appendDouble(Double.longBitsToDouble(value));
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                buf.appendTimestamp(value);
                break;
            default:
                throw new SQLException(
                        "Unexpected fixed-width field type: " + meta.getFieldType());
        }
    }

    // -----------------------------------------------------------------------
    // Setters – fixed-width types
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Setters – timestamp
    // -----------------------------------------------------------------------

    private void stageTimestamp(int parameterIndex, long epochValue) {
        stageFixed(parameterIndex - 1, epochValue);
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

    // -----------------------------------------------------------------------
    // Setters – variable-width types
    // -----------------------------------------------------------------------

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        int idx = parameterIndex - 1;
        if (bindType(parameterIndex) == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
            if (x == null) {
                throw TSDBError.createSQLException(
                        TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name can't be null");
            }
            currentTbName = x;
            return;
        }
        stageVar(idx, x == null ? null : encodeString(x, fieldType(parameterIndex)));
    }

    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        int idx = parameterIndex - 1;
        if (bindType(parameterIndex) == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
            if (x == null) {
                throw TSDBError.createSQLException(
                        TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name can't be null");
            }
            currentTbName = x;
            return;
        }
        stageVar(idx, x == null ? null : x.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        int idx = parameterIndex - 1;
        if (bindType(parameterIndex) == (byte) FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
            if (x == null) {
                throw TSDBError.createSQLException(
                        TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name can't be null");
            }
            currentTbName = new String(x, StandardCharsets.UTF_8);
            return;
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

    // -----------------------------------------------------------------------
    // Setters – null
    // -----------------------------------------------------------------------

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        stageNull(parameterIndex - 1);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        stageNull(parameterIndex - 1);
    }

    // -----------------------------------------------------------------------
    // Setters – object dispatch
    // -----------------------------------------------------------------------

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
                    long ts = DateTimeUtils.toLong((Instant) x, stmtInfo.getPrecision());
                    stageTimestamp(parameterIndex, ts);
                } else if (x instanceof ZonedDateTime) {
                    long ts = DateTimeUtils.toLong((ZonedDateTime) x, stmtInfo.getPrecision());
                    stageTimestamp(parameterIndex, ts);
                } else if (x instanceof OffsetDateTime) {
                    long ts = DateTimeUtils.toLong((OffsetDateTime) x, stmtInfo.getPrecision());
                    stageTimestamp(parameterIndex, ts);
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
                long ts = DateTimeUtils.toLong(zdt.toInstant(), stmtInfo.getPrecision());
                stageTimestamp(parameterIndex, ts);
            }
        } else if (x instanceof Instant) {
            long ts = DateTimeUtils.toLong((Instant) x, stmtInfo.getPrecision());
            stageTimestamp(parameterIndex, ts);
        } else if (x instanceof ZonedDateTime) {
            long ts = DateTimeUtils.toLong((ZonedDateTime) x, stmtInfo.getPrecision());
            stageTimestamp(parameterIndex, ts);
        } else if (x instanceof OffsetDateTime) {
            long ts = DateTimeUtils.toLong((OffsetDateTime) x, stmtInfo.getPrecision());
            stageTimestamp(parameterIndex, ts);
        } else if (x instanceof BigInteger) {
            BigInteger v = (BigInteger) x;
            if (v.compareTo(BigInteger.ZERO) < 0
                    || v.compareTo(new BigInteger(MAX_UNSIGNED_LONG)) > 0) {
                throw TSDBError.createSQLException(
                        TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "ubigint value is out of range");
            }
            stageFixed(parameterIndex - 1, v.longValue());
        } else if (x instanceof Blob) {
            byte[] bytes = ((Blob) x).getBytes(1, (int) ((Blob) x).length());
            stageVar(parameterIndex - 1, bytes);
        } else if (x instanceof BigDecimal) {
            stageVar(parameterIndex - 1,
                    ((BigDecimal) x).toPlainString().getBytes(StandardCharsets.UTF_8));
        } else {
            throw new SQLException("Unsupported data type: " + x.getClass().getName());
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    // -----------------------------------------------------------------------
    // Unsupported stream / reader setters
    // -----------------------------------------------------------------------

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    // -----------------------------------------------------------------------
    // Parameter lifecycle
    // -----------------------------------------------------------------------

    @Override
    public void clearParameters() {
        Arrays.fill(currentNull, true);
        Arrays.fill(currentVar, null);
        Arrays.fill(currentFixed, 0L);
        currentTbName = null;
    }

    // -----------------------------------------------------------------------
    // Batch / execute
    // -----------------------------------------------------------------------

    @Override
    public void addBatch() throws SQLException {
        flushCurrentRow();
        expectedRowCount++;
    }

    /**
     * Drops rows already accumulated in column buffers via {@link #addBatch()},
     * but preserves any current-row staged parameter values that have not yet
     * been committed to the batch.
     */
    @Override
    public void clearBatch() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        expectedRowCount = 0;
        columnBuffers = allocateColumnBuffers();
    }

    @Override
    public int[] executeBatch() throws SQLException {
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
        throw TSDBError.createSQLException(
                TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                "WSColumnPreparedStatement only supports insert; use TSWSPreparedStatement for queries");
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
        addBatch();
        return executeInsertImpl();
    }

    // -----------------------------------------------------------------------
    // Core execute implementation
    // -----------------------------------------------------------------------

    private int executeInsertImpl() throws SQLException {
        checkRowCounts();

        byte[] payload = Stmt2ColumnBindSerializer.serialize(columnBuffers);

        // Build ByteBuf: 16-byte header placeholder (reqId + stmtId) + columnar payload.
        // modifyStmtIdAndReqId() in WSRetryableStmt will patch the first 16 bytes.
        ByteBuf rawBuf = PooledByteBufAllocator.DEFAULT.directBuffer(16 + payload.length);
        rawBuf.writeLongLE(0L); // reqId placeholder
        rawBuf.writeLongLE(0L); // stmtId placeholder
        rawBuf.writeBytes(payload);

        try {
            this.affectedRows = 0;
            writeBlockWithRetrySync(rawBuf);
            this.affectedRows = batchInsertedRowsInner.getAndSet(0);
        } finally {
            rawBuf.release();
            resetBuffers();
        }

        return this.affectedRows;
    }

    /**
     * Lightweight O(fieldCount) row-count consistency check.
     * Compares each field accumulator's row count against expectedRowCount.
     */
    private void checkRowCounts() throws SQLException {
        int expected = expectedRowCount;
        for (int i = 0; i < columnBuffers.length; i++) {
            int actual = columnBuffers[i].getRowCount();
            if (actual != expected) {
                throw new SQLException(
                        "Row count mismatch at parameter index " + (i + 1)
                                + ": expected " + expected + " rows, got " + actual
                                + " rows");
            }
        }
    }

    /** Resets column buffers and batch state after execute. */
    private void resetBuffers() {
        expectedRowCount = 0;
        columnBuffers = allocateColumnBuffers();
        Arrays.fill(currentNull, true);
        Arrays.fill(currentVar, null);
        Arrays.fill(currentFixed, 0L);
        currentTbName = null;
    }

    // -----------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (this.getResultSet() == null) {
            return null;
        }
        return getResultSet().getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        return new WSParameterMetaData(stmtInfo.isInsert(), stmtInfo.getFields(),
                stmtInfo.getColTypeList());
    }

    // -----------------------------------------------------------------------
    // Close
    // -----------------------------------------------------------------------

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            if (transport.isConnected() && stmtInfo.getStmtId() != 0) {
                long reqId = ReqId.getReqID();
                com.taosdata.jdbc.ws.entity.Request close =
                        RequestFactory.generateClose(stmtInfo.getStmtId(), reqId);
                transport.send(close, this.getQueryTimeoutInMs());
            }
            super.close();
        }
    }

    // -----------------------------------------------------------------------
    // Unsupported PreparedStatement helpers
    // -----------------------------------------------------------------------

    @Override
    public void addBatch(String sql) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    // -----------------------------------------------------------------------
    // Package-private accessors for testing
    // -----------------------------------------------------------------------

    /**
     * Returns the number of rows flushed into column buffers so far.
     * Exposed for unit tests.
     */
    int getExpectedRowCount() {
        return expectedRowCount;
    }

    /**
     * Returns the column buffer at the given zero-based field index.
     * Exposed for unit tests.
     */
    Stmt2ColumnFieldBuffer getColumnBuffer(int fieldIndex) {
        return columnBuffers[fieldIndex];
    }

    /**
     * Returns the tbname field index (-1 if not a supertable).
     * Exposed for unit tests.
     */
    int getTbNameFieldIdx() {
        return tbNameFieldIdx;
    }

    // -----------------------------------------------------------------------
    // Internal utilities
    // -----------------------------------------------------------------------

    private static byte[] encodeString(String s, byte fieldType) {
        switch (fieldType & 0xFF) {
            case TSDB_DATA_TYPE_NCHAR:
                return s.getBytes(StandardCharsets.UTF_8);
            default:
                return s.getBytes(StandardCharsets.UTF_8);
        }
    }
}
