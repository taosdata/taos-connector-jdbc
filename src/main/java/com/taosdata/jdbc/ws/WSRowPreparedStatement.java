package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.AutoExpandingBuffer;
import com.taosdata.jdbc.common.TableInfo;
import com.taosdata.jdbc.enums.FeildBindType;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;

public class WSRowPreparedStatement extends WSStatement implements PreparedStatement{
    protected final ConnectionParam param;
    private long reqId;
    private long stmtId;
    private final String rawSql;
    protected int queryTimeout = 0;
    protected int precision = TimestampPrecision.MS;
    protected int toBeBindTableNameIndex = -1;
    protected int toBeBindColCount = 0;
    protected int toBeBindTagCount = 0;
    protected List<Field> fields;
    protected ArrayList<Byte> colTypeList = new ArrayList<>();
    protected boolean isInsert = false;
    private TableInfo tableInfo;
    private AutoExpandingBuffer tableNameLensBuf;
    private AutoExpandingBuffer tableNamesBuf;
    private AutoExpandingBuffer tagLensBuf;
    private AutoExpandingBuffer tagsBuf;
    private AutoExpandingBuffer colLensBuf;
    private AutoExpandingBuffer colsBuf;
    private int curTableTagTotalLen = 0;
    private int curTableColTotalLen = 0;
    private int totalTableCount = 0;

    private final int MAX_COMPONENT_COUNT = 1000;
    private final int SMALL_BUFFER_INIT_SIZE = 1024; // 1KB
    private final int TABLE_NAME_BUFFER_INIT_SIZE = 1024 * 10; // 10KB
    private final int TAGS_BUFFER_INIT_SIZE = 1024 * 100; // 100KB
    private final int COLS_BUFFER_INIT_SIZE = 1024 * 1024; // 1MB

    private void initBuffers() {
        buffersStopWrite();
        freeBuffers();
        tableNameLensBuf = new AutoExpandingBuffer(SMALL_BUFFER_INIT_SIZE, MAX_COMPONENT_COUNT);
        tableNamesBuf = new AutoExpandingBuffer(TABLE_NAME_BUFFER_INIT_SIZE, MAX_COMPONENT_COUNT);

        tagLensBuf = new AutoExpandingBuffer(SMALL_BUFFER_INIT_SIZE, MAX_COMPONENT_COUNT);
        tagsBuf = new AutoExpandingBuffer(TAGS_BUFFER_INIT_SIZE, MAX_COMPONENT_COUNT);

        colLensBuf = new AutoExpandingBuffer(SMALL_BUFFER_INIT_SIZE, MAX_COMPONENT_COUNT);
        colsBuf = new AutoExpandingBuffer(COLS_BUFFER_INIT_SIZE, MAX_COMPONENT_COUNT);
    }
    public WSRowPreparedStatement(Transport transport,
                                  ConnectionParam param,
                                  String database,
                                  AbstractConnection connection,
                                  String sql,
                                  Long instanceId) {
        super(transport, database, connection, instanceId, param.getZoneId());
        this.rawSql = sql;
        this.param = param;
        initBuffers();
    }

    public WSRowPreparedStatement(Transport transport,
                                  ConnectionParam param,
                                  String database,
                                  AbstractConnection connection,
                                  String sql,
                                  Long instanceId,
                                  Stmt2PrepareResp prepareResp) {
        super(transport, database, connection, instanceId, param.getZoneId());
        this.rawSql = sql;
        this.param = param;

        reqId = prepareResp.getReqId();
        stmtId = prepareResp.getStmtId();
        isInsert = prepareResp.isInsert();
        if (isInsert){
            fields = prepareResp.getFields();
            if (!fields.isEmpty()){
                precision = fields.get(0).getPrecision();
            }
            for (int i = 0; i < fields.size(); i++){
                Field field = fields.get(i);
                if (field.getBindType() == FeildBindType.TAOS_FIELD_TBNAME.getValue()){
                    toBeBindTableNameIndex = i;
                }
                if (field.getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
                    toBeBindTagCount++;
                }
                if (field.getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
                    toBeBindColCount++;
                }
                colTypeList.add(field.getFieldType());
            }
        } else if (prepareResp.getFieldsCount() > 0){
            toBeBindColCount = prepareResp.getFieldsCount();
            for (int i = 0; i < toBeBindColCount; i++){
                colTypeList.add((byte)-1);
            }
        }

        this.tableInfo = TableInfo.getEmptyTableInfo();
        initBuffers();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (seconds < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);

        this.queryTimeout = seconds;
        transport.setTimeout(seconds * 1000L);
    }

    @Override
    public boolean execute() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        if (isInsert){
            executeUpdate();
        } else {
            executeQuery();
        }

        return !isInsert;
    }
    @Override
    public ResultSet executeQuery() throws SQLException {
        if (this.isInsert){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "The query SQL must be prepared.");
        }

        addBatch();
        executeBatchImpl();
        Request request = RequestFactory.generateUseResult(stmtId, reqId);
        ResultResp resp = (ResultResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }

        this.resultSet = new BlockResultSet(this, this.transport, resp, this.database, this.zoneId);
        this.affectedRows = -1;
        return this.resultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (!this.isInsert){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "The insert SQL must be prepared.");
        }

        if (fields.isEmpty()){
            return this.executeUpdate(this.rawSql);
        }

        addBatch();
        return executeBatchImpl();
    }
    public void setNullByTSDBType(int parameterIndex, int type) throws SQLException {
        switch (type) {
            case TSDB_DATA_TYPE_BOOL:
                setBooleanInner(parameterIndex, false, true);
                break;
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                setByteInner(parameterIndex, (byte) 0, true, (byte)type);
                break;
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                setShortInner(parameterIndex, (short) 0, true, (byte)type);
                break;
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                setIntInner(parameterIndex, 0, true, (byte)type);
                break;
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                setLongInner(parameterIndex, 0L, true, (byte)type);
                break;
            case TSDB_DATA_TYPE_FLOAT:
                setFloatInner(parameterIndex, 0.0f, true);
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                setDoubleInner(parameterIndex, 0.0d, true);
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                setTimestampInner(parameterIndex, 0, true);
                break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
                seStringInner(parameterIndex, null, true, (byte)type);
                break;
//            // bind decimal is not supported now
//            case TSDB_DATA_TYPE_DECIMAL64:
//            case TSDB_DATA_TYPE_DECIMAL128:
//                break;

            default:
                throw new SQLException("unsupported type: " + type);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        byte bindType;
        switch (sqlType) {
            case Types.BOOLEAN:
                setBooleanInner(parameterIndex, false, true);
                break;
            case Types.TINYINT:
                setByteInner(parameterIndex, (byte) 0, true, (byte) TSDB_DATA_TYPE_TINYINT);
                break;
            case Types.SMALLINT:
                setShortInner(parameterIndex, (short) 0, true, (byte) TSDB_DATA_TYPE_SMALLINT);
                break;
            case Types.INTEGER:
                setIntInner(parameterIndex, 0, true, (byte) TSDB_DATA_TYPE_INT);
                break;
            case Types.BIGINT:
                setLongInner(parameterIndex, 0L, true, (byte) TSDB_DATA_TYPE_BIGINT);
                break;
            case Types.FLOAT:
                setFloatInner(parameterIndex, 0.0f, true);
                break;
            case Types.DOUBLE:
                setDoubleInner(parameterIndex, 0.0d, true);
                break;
            case Types.TIMESTAMP:
                setTimestampInner(parameterIndex, 0, true);
                break;
            case Types.BINARY:
            case Types.VARCHAR:
            case Types.VARBINARY:
                bindType = fields.get(parameterIndex - 1).getBindType();
                if (bindType > 0){
                    seStringInner(parameterIndex, null, true, bindType);
                } else {
                    seStringInner(parameterIndex, null, true, (byte)TSDB_DATA_TYPE_VARCHAR);
                }
                break;
            case Types.NCHAR:
                bindType = fields.get(parameterIndex - 1).getBindType();
                if (bindType > 0){
                    seStringInner(parameterIndex, null, true, bindType);
                } else {
                    seStringInner(parameterIndex, null, true, (byte)TSDB_DATA_TYPE_NCHAR);
                }
                break;
            // json
            case Types.OTHER:
                bindType = fields.get(parameterIndex - 1).getBindType();
                if (bindType > 0){
                    seStringInner(parameterIndex, null, true, bindType);
                } else {
                    seStringInner(parameterIndex, null, true, (byte)TSDB_DATA_TYPE_JSON);
                }
                break;
            default:
                throw new SQLException("unsupported type: " + sqlType);
        }
    }

    private void setBooleanInner(int parameterIndex, boolean x, boolean isNull) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeBool(x, isNull);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeBool(x, isNull);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setBooleanInner(parameterIndex, x, false);
    }

    // ---------------- 以下为统一风格的基础类型处理 ----------------
    private void setByteInner(int parameterIndex, byte x, boolean isNull, byte type) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeByte(x, isNull, type);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeByte(x, isNull, type);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setByteInner(parameterIndex, x, false, (byte)TSDB_DATA_TYPE_TINYINT);
    }

    private void setShortInner(int parameterIndex, short x, boolean isNull, byte type) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeShort(x, isNull, type);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeShort(x, isNull, type);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        if (colTypeList.get(parameterIndex - 1) == TSDB_DATA_TYPE_UTINYINT){
            if (x < 0 || x > MAX_UNSIGNED_BYTE){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "utinyint value is out of range");
            }
            setByteInner(parameterIndex, (byte) x, false, (byte)TSDB_DATA_TYPE_UTINYINT);
            return;
        }
        setShortInner(parameterIndex, x, false, (byte)TSDB_DATA_TYPE_SMALLINT);
    }

    private void setIntInner(int parameterIndex, int x, boolean isNull, byte type) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeInt(x, isNull, type);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeInt(x, isNull, type);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        if (colTypeList.get(parameterIndex - 1) == TSDB_DATA_TYPE_USMALLINT){
            if (x < 0 || x > MAX_UNSIGNED_SHORT){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "usmallint value is out of range");
            }
            setShortInner(parameterIndex, (short) x, false, (byte)TSDB_DATA_TYPE_USMALLINT);
            return;
        }
        setIntInner(parameterIndex, x, false, (byte)TSDB_DATA_TYPE_INT);
    }

    private void setLongInner(int parameterIndex, long x, boolean isNull, byte type) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeLong(x, isNull, type);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeLong(x, isNull, type);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        if (colTypeList.get(parameterIndex - 1) == TSDB_DATA_TYPE_UINT){
            if (x < 0 || x > MAX_UNSIGNED_INT){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "uint value is out of range");
            }
            setIntInner(parameterIndex, (int) x, false, (byte)TSDB_DATA_TYPE_UINT);
            return;
        }
        setLongInner(parameterIndex, x, false, (byte)TSDB_DATA_TYPE_BIGINT);
    }

    private void setFloatInner(int parameterIndex, float x, boolean isNull) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeFloat(x, isNull);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeFloat(x, isNull);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setFloatInner(parameterIndex, x, false);
    }

    private void setDoubleInner(int parameterIndex, double x, boolean isNull) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeDouble(x, isNull);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeDouble(x, isNull);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setDoubleInner(parameterIndex, x, false);
    }
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    private void seStringInner(int parameterIndex, String x, boolean isNull, byte type) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeString(x, isNull, type);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeString(x, isNull, type);
        } else if (bindType == FeildBindType.TAOS_FIELD_TBNAME.getValue()) {
            if (x == null){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name can't be null");
            }
            int length = tableNamesBuf.writeString(x);
            tableNamesBuf.writeBytes(new byte[]{0});
            tableNameLensBuf.writeShort((short) (length + 1));
        }
    }

    private void setBytesInner(int parameterIndex, byte[] x, boolean isNull, byte type) throws SQLException {
        byte bindType = fields.get(parameterIndex - 1).getBindType();
        if (bindType == FeildBindType.TAOS_FIELD_COL.getValue()) {
            curTableColTotalLen += colsBuf.serializeBytes(x, isNull, type);
        } else if (bindType == FeildBindType.TAOS_FIELD_TAG.getValue()) {
            curTableTagTotalLen += tagsBuf.serializeBytes(x, isNull, type);
        } else if (bindType == FeildBindType.TAOS_FIELD_TBNAME.getValue()) {
            if (x == null){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name can't be null");
            }
            tableNamesBuf.writeBytes(x);
            tableNamesBuf.writeBytes(new byte[]{0});
            tableNameLensBuf.writeShort((short) (x.length + 1));
        }
    }

    private byte getTSDBType(int parameterIndex, byte defaultType){
        byte type = colTypeList.get(parameterIndex - 1);
        if (type >= 0){
            return type;
        }
        return defaultType;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        seStringInner(parameterIndex, x, x == null, getTSDBType(parameterIndex, (byte)TSDB_DATA_TYPE_VARCHAR));
    }

    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        seStringInner(parameterIndex, x, x == null, getTSDBType(parameterIndex, (byte)TSDB_DATA_TYPE_NCHAR));
    }
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBytesInner(parameterIndex, x, x == null, getTSDBType(parameterIndex, (byte)TSDB_DATA_TYPE_VARBINARY));
    }
    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
            return;
        }
        Timestamp timestamp = new Timestamp(x.getTime());
        setTimestamp(parameterIndex, timestamp);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
            return;
        }
        Timestamp timestamp = new Timestamp(x.getTime());
        setTimestamp(parameterIndex, timestamp);
    }

    private void setTimestampInner(int parameterIndex, long x, boolean isNull) throws SQLException {
        if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
            int totalLen = colsBuf.serializeTimeStamp(x, isNull);
            curTableColTotalLen += totalLen;
        } else if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
            int totalLen = tagsBuf.serializeTimeStamp(x, isNull);
            curTableTagTotalLen += totalLen;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x == null){
            setNull(parameterIndex, Types.TIMESTAMP);
            return;
        }

        long ts = DateTimeUtils.toLong(DateTimeUtils.toInstant(x, this.zoneId), precision);
        setTimestampInner(parameterIndex, ts, false);
    }

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
    public void clearParameters() {
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (x == null){
            setNull(parameterIndex, targetSqlType);
            return;
        }
        switch (targetSqlType) {
            case Types.BOOLEAN:
                if (x instanceof Boolean){
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
                if (x instanceof Date) {
                    setDate(parameterIndex, (Date) x);
                } else if (x instanceof Time) {
                    setTime(parameterIndex, (Time) x);
                } else if (x instanceof LocalDateTime) {
                    if (zoneId == null) {
                        setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) x));
                    } else {
                        ZonedDateTime zonedDateTime = ((LocalDateTime) x).atZone(zoneId);
                        Instant instant = zonedDateTime.toInstant();
                        long ts = DateTimeUtils.toLong(instant, precision);
                        setTimestampInner(parameterIndex, ts, false);
                    }
                } else if (x instanceof Instant) {
                    long ts = DateTimeUtils.toLong((Instant) x, precision);
                    setTimestampInner(parameterIndex, ts, false);
                } else if (x instanceof ZonedDateTime) {
                    long ts = DateTimeUtils.toLong((ZonedDateTime) x, precision);
                    setTimestampInner(parameterIndex, ts, false);
                } else if (x instanceof OffsetDateTime) {
                    long ts = DateTimeUtils.toLong((OffsetDateTime) x, precision);
                    setTimestampInner(parameterIndex, ts, false);
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
            default:
                throw new SQLException("unsupported type: " + targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null){
            byte bindType = fields.get(parameterIndex - 1).getBindType();
            if (bindType >= 0){
                setNullByTSDBType(parameterIndex, bindType);
            } else {
                // query
                setNull(parameterIndex, Types.TINYINT);
            }

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
        } else if (x instanceof String) {
            setNString(parameterIndex, (String) x);
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof LocalDateTime) {
            setObject(parameterIndex, x, Types.TIMESTAMP);
        } else if (x instanceof Instant) {
            setObject(parameterIndex, x, Types.TIMESTAMP);
        } else if (x instanceof ZonedDateTime) {
            setObject(parameterIndex, x, Types.TIMESTAMP);
        } else if (x instanceof OffsetDateTime) {
            setObject(parameterIndex, x, Types.TIMESTAMP);
        } else if (x instanceof BigInteger) {
            BigInteger v = (BigInteger) x;
            if (v.compareTo(BigInteger.ZERO) < 0 || v.compareTo(new BigInteger(MAX_UNSIGNED_LONG)) > 0){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "ubigint value is out of range");
            }
            setLongInner(parameterIndex, ((BigInteger) x).longValue(), false, (byte)TSDB_DATA_TYPE_UBIGINT);
        } else {
            throw new SQLException("Unsupported data type: " + x.getClass().getName());
        }
    }

    @Override
    // Only support batch insert
    public void addBatch() throws SQLException {
        tagLensBuf.writeInt(curTableTagTotalLen);
        colLensBuf.writeInt(curTableColTotalLen);
        totalTableCount++;
        clearCache();
    }

    private void clearCache(){
        curTableTagTotalLen = 0;
        curTableColTotalLen = 0;
    }

    private boolean isTableInfoEmpty(){
        return tableInfo.getTableName().capacity() == 0
                && tableInfo.getTagInfo().isEmpty()
                && tableInfo.getDataList().isEmpty();
    }
    @Override
    public int[] executeBatch() throws SQLException {
        int affected = executeBatchImpl();
        int[] ints = new int[affected];
        for (int i = 0, len = ints.length; i < len; i++) {
            ints[i] = SUCCESS_NO_INFO;
        }

        return ints;
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            if (transport.isConnected() && stmtId != 0) {
                Request close = RequestFactory.generateClose(stmtId, reqId);
                transport.send(close);
            }
            super.close();
        }
        freeBuffers();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (this.getResultSet() == null)
            return null;
        return getResultSet().getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        return new WSParameterMetaData(isInsert, fields, colTypeList);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
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

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
       Instant instant = DateTimeUtils.toInstant(x, cal);
       setTimestamp(parameterIndex, Timestamp.from(instant));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
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
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
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
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    private void freeBuffer(AutoExpandingBuffer buf){
        if (buf != null){
            buf.release();
        }
    }
    private void freeBuffers(){
        freeBuffer(tableNameLensBuf);
        freeBuffer(tableNamesBuf);
        freeBuffer(tagLensBuf);
        freeBuffer(tagsBuf);
        freeBuffer(colLensBuf);
        freeBuffer(colsBuf);
    }
    private void buffersStopWrite(){
        if (tableNameLensBuf != null){
            tableNameLensBuf.stopWrite();
        }
        if (tableNamesBuf != null){
            tableNamesBuf.stopWrite();
        }
        if (tagLensBuf != null){
            tagLensBuf.stopWrite();
        }
        if (tagsBuf != null){
            tagsBuf.stopWrite();
        }
        if (colLensBuf != null){
            colLensBuf.stopWrite();
        }
        if (colsBuf != null){
            colsBuf.stopWrite();
        }
    }
    private int executeBatchImpl() throws SQLException {
        buffersStopWrite();

        int totalTableNameSize = tableNamesBuf.getBuffer().capacity();
        int totalTagSize = tagsBuf.getBuffer().capacity();
        int totalColSize = colsBuf.getBuffer().capacity();
        int totalSize = totalTableNameSize + totalTagSize + totalColSize;

        int toBebindTableNameCount = toBeBindTableNameIndex >= 0 ? 1 : 0;

        totalSize += totalTableCount * (
                toBebindTableNameCount * Short.BYTES
                        + (toBeBindTagCount > 0 ? 1 : 0) * Integer.BYTES
                        + (toBeBindColCount > 0 ? 1 : 0) * Integer.BYTES);

        ByteBuf headBuf = PooledByteBufAllocator.DEFAULT.directBuffer(58);

        //************ header *****************
        // ReqId
        headBuf.writeLongLE(reqId);
        // stmtId
        headBuf.writeLongLE(stmtId);
        // actionId
        headBuf.writeLongLE(9L);
        // version
        headBuf.writeShortLE(1);
        // col_idx
        headBuf.writeIntLE(-1);

        //************ data *****************
        // TotalLength
        headBuf.writeIntLE(totalSize + 28);
        // tableCount
        headBuf.writeIntLE(totalTableCount);
        // TagCount
        headBuf.writeIntLE(toBeBindTagCount);
        // ColCount
        headBuf.writeIntLE(toBeBindColCount);

        // tableNameOffset
        if (toBebindTableNameCount > 0){
            headBuf.writeIntLE(0x1C);
        } else {
            headBuf.writeIntLE(0);
        }

        // tagOffset
        if (toBeBindTagCount > 0){
            if (toBebindTableNameCount > 0){
                headBuf.writeIntLE(28 + totalTableNameSize + Short.BYTES * totalTableCount);
            } else {
                headBuf.writeIntLE(28);
            }
        } else {
            headBuf.writeIntLE(0);
        }

        // colOffset
        if (toBeBindColCount > 0){
            int skipSize = 0;
            if (toBebindTableNameCount > 0){
                skipSize += totalTableNameSize + Short.BYTES * totalTableCount;
            }

            if (toBeBindTagCount > 0){
                skipSize += totalTagSize + Integer.BYTES * totalTableCount;
            }
            headBuf.writeIntLE(28 + skipSize);
        } else {
            headBuf.writeIntLE(0);
        }


        CompositeByteBuf rawBlock = PooledByteBufAllocator.DEFAULT.compositeBuffer();
        rawBlock.addComponent(true, headBuf);

        if (toBebindTableNameCount > 0){
            rawBlock.addComponent(true, tableNameLensBuf.getBuffer());
            rawBlock.addComponent(true, tableNamesBuf.getBuffer());
        }

        if (toBeBindTagCount > 0){
            rawBlock.addComponent(true, tagLensBuf.getBuffer());
            rawBlock.addComponent(true, tagsBuf.getBuffer());
        }

        if (toBeBindColCount > 0){
            rawBlock.addComponent(true, colLensBuf.getBuffer());
            rawBlock.addComponent(true, colsBuf.getBuffer());
        }

//        StringBuilder sb = new StringBuilder();
//        for (int i = 30; i < rawBlock.capacity(); i++) {
//            int bb = rawBlock.getByte(i) & 0xff;
//            sb.append(bb);
//            sb.append(",");
//        }
//        System.out.println(sb);
//        System.exit(0);

        try {
            this.affectedRows = 0;
            // bind
            Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.STMT2_BIND.getAction(),
                    reqId, rawBlock);
            if (Code.SUCCESS.getCode() != bindResp.getCode()) {
                throw TSDBError.createSQLException(bindResp.getCode(), "(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
            }

            // execute
            Request request = RequestFactory.generateExec(stmtId, reqId);
            Stmt2ExecResp resp = (Stmt2ExecResp) transport.send(request);
            if (Code.SUCCESS.getCode() != resp.getCode()) {
                throw TSDBError.createSQLException(resp.getCode(), "(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
            }
            this.affectedRows = resp.getAffected();
        } finally {
            initBuffers();
            totalTableCount = 0;
        }

        return this.affectedRows;
    }
}
