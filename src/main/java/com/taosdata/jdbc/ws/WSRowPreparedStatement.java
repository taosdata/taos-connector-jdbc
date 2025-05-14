package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.TaosPrepareStatement;
import com.taosdata.jdbc.common.AutoExpandingBuffer;
import com.taosdata.jdbc.common.Column;
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
import io.netty.util.ReferenceCountUtil;

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
import java.util.*;

import static com.taosdata.jdbc.TSDBConstants.*;

public class WSRowPreparedStatement extends WSStatement implements TaosPrepareStatement {
    private static final List<Object> nullTag = Collections.singletonList(null);

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
    protected ArrayList<Integer> colTypeList = new ArrayList<>();
    protected boolean isInsert = false;
    protected final Map<Integer, Column> colOrderedMap = new HashMap<>();
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

    private void initBuffers() {
        buffersStopWrite();
        freeBuffers();
        tableNameLensBuf = new AutoExpandingBuffer(1024, 1000);
        tableNamesBuf = new AutoExpandingBuffer(10240, 1000);

        tagLensBuf = new AutoExpandingBuffer(1024, 1000);
        tagsBuf = new AutoExpandingBuffer(100 * 1024, 1000);

        colLensBuf = new AutoExpandingBuffer(1024, 1000);
        colsBuf = new AutoExpandingBuffer(1024 * 1024, 1000);
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
                colTypeList.add((int) field.getFieldType());
            }
        } else if (prepareResp.getFieldsCount() > 0){
            toBeBindColCount = prepareResp.getFieldsCount();
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
//
//        if (!tag.isEmpty() || !colListQueue.isEmpty()){
//            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "The query SQL only support bind columns.");
//        }
//
//        // only support jdbc standard bind api
//        if (colOrderedMap.isEmpty()){
//            return executeQuery(this.rawSql);
//        }
//
//        onlyBindCol();
//        if (!isTableInfoEmpty()){
//            tableInfoList.put(tableInfo.getTableName(), tableInfo);
//        }
//
//        this.executeBatchImpl();
//
//        Request request = RequestFactory.generateUseResult(stmtId, reqId);
//        ResultResp resp = (ResultResp) transport.send(request);
//        if (Code.SUCCESS.getCode() != resp.getCode()) {
//            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
//        }
//
//        this.resultSet = new BlockResultSet(this, this.transport, resp, this.database, this.zoneId);
//        this.affectedRows = -1;
//        return this.resultSet;

        return null;
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

    @Override
    public void setTagNull(int index, int type) throws SQLException {
    }

    @Override
    public void setTagBoolean(int index, boolean value) {
    }

    @Override
    public void setTagByte(int index, byte value) {
    }

    @Override
    public void setTagShort(int index, short value) {
    }

    @Override
    public void setTagInt(int index, int value) {
    }

    @Override
    public void setTagLong(int index, long value) {
    }
    @Override
    public void setTagBigInteger(int index, BigInteger value) throws SQLException {
    }
    @Override
    public void setTagFloat(int index, float value) {
    }

    @Override
    public void setTagDouble(int index, double value) {
    }

    @Override
    public void setTagTimestamp(int index, long value) {
    }

    @Override
    public void setTagTimestamp(int index, Timestamp value) {
    }

    @Override
    public void setTagString(int index, String value) {
    }

    @Override
    public void setTagVarbinary(int index, byte[] value) {
    }
    @Override
    public void setTagGeometry(int index, byte[] value) {
    }

    @Override
    public void setTagNString(int index, String value) {
    }

    @Override
    public void setTagJson(int index, String value) {
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.BOOLEAN:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_BOOL, parameterIndex));
                break;
            case Types.TINYINT:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_TINYINT, parameterIndex));
                break;
            case Types.SMALLINT:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_SMALLINT, parameterIndex));
                break;
            case Types.INTEGER:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_INT, parameterIndex));
                break;
            case Types.BIGINT:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_BIGINT, parameterIndex));
                break;
            case Types.FLOAT:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_FLOAT, parameterIndex));
                break;
            case Types.DOUBLE:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_DOUBLE, parameterIndex));
                break;
            case Types.TIMESTAMP:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
                break;
            case Types.BINARY:
            case Types.VARCHAR:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_BINARY, parameterIndex));
                break;
            case Types.VARBINARY:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_VARBINARY, parameterIndex));
                break;
            case Types.NCHAR:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_NCHAR, parameterIndex));
                break;
            // json
            case Types.OTHER:
                colOrderedMap.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_JSON, parameterIndex));
                break;
            default:
                throw new SQLException("unsupported type: " + sqlType);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {

    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
            int totalLen = colsBuf.serializeInt(x, false);
            curTableColTotalLen += totalLen;
        } else if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
            int totalLen = tagsBuf.serializeInt(x, false);
            curTableTagTotalLen += totalLen;
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
            int totalLen = colsBuf.serializeLong(x, false);
            curTableColTotalLen += totalLen;
        } else if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
            int totalLen = tagsBuf.serializeLong(x, false);
            curTableTagTotalLen += totalLen;
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
            int totalLen = colsBuf.serializeFloat(x, false);
            curTableColTotalLen += totalLen;
        } else if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
            int totalLen = tagsBuf.serializeFloat(x, false);
            curTableTagTotalLen += totalLen;
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
            int totalLen = colsBuf.serializeDouble(x, false);
            curTableColTotalLen += totalLen;

        } else if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
            int totalLen = tagsBuf.serializeDouble(x, false);
            curTableTagTotalLen += totalLen;
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
            int totalLen = colsBuf.serializeBytes(x.getBytes(StandardCharsets.UTF_8), false, colTypeList.get(parameterIndex - 1));
            curTableColTotalLen += totalLen;
        } else if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
            int totalLen = tagsBuf.serializeBytes(x.getBytes(StandardCharsets.UTF_8), false, colTypeList.get(parameterIndex - 1));
            curTableTagTotalLen += totalLen;
        } else if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_TBNAME.getValue()){
            byte[] tableName = x.getBytes(StandardCharsets.UTF_8);
            tableNamesBuf.writeBytes(tableName);
            tableNamesBuf.writeBytes(new byte[]{0});
            tableNameLensBuf.writeShort((short)(tableName.length + 1));
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BINARY, parameterIndex));
    }

    public void setVarbinary(int parameterIndex, byte[] x) throws SQLException {
        // UTF-8
        if (x == null) {
            setNull(parameterIndex, Types.VARBINARY);
            return;
        }
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_VARBINARY, parameterIndex));
    }

    public void setGeometry(int parameterIndex, byte[] x) throws SQLException {
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_GEOMETRY, parameterIndex));
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

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        long ts = DateTimeUtils.toLong(DateTimeUtils.toInstant(x, this.zoneId), precision);
        if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
            int totalLen = colsBuf.serializeTimeStamp(ts, false);
            curTableColTotalLen += totalLen;
        } else if (fields.get(parameterIndex - 1).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
            int totalLen = tagsBuf.serializeTimeStamp(ts, false);
            curTableTagTotalLen += totalLen;
        }
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
        switch (targetSqlType) {
            case Types.BOOLEAN:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BOOL, parameterIndex));
                break;
            case Types.TINYINT:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_TINYINT, parameterIndex));
                break;
            case Types.SMALLINT:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_SMALLINT, parameterIndex));
                break;
            case Types.INTEGER:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_INT, parameterIndex));
                break;
            case Types.BIGINT:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BIGINT, parameterIndex));
                break;
            case Types.FLOAT:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_FLOAT, parameterIndex));
                break;
            case Types.DOUBLE:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_DOUBLE, parameterIndex));
                break;
            case Types.TIMESTAMP:
                Instant instant = DateTimeUtils.toInstant((Timestamp) x, zoneId);
                colOrderedMap.put(parameterIndex, new Column(instant, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
                break;
            case Types.BINARY:
            case Types.VARCHAR:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BINARY, parameterIndex));
                break;
            case Types.VARBINARY:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_VARBINARY, parameterIndex));
                break;
            case Types.NCHAR:
                colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_NCHAR, parameterIndex));
                break;
            // json
            case Types.OTHER:
                if (x instanceof Number){
                    colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_UBIGINT, parameterIndex));
                } else {
                    colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_JSON, parameterIndex));
                }
                break;
            default:
                throw new SQLException("unsupported type: " + targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
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
            if (zoneId == null) {
                setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) x));
            } else {
                ZonedDateTime zonedDateTime = ((LocalDateTime) x).atZone(zoneId);
                Instant instant = zonedDateTime.toInstant();
                colOrderedMap.put(parameterIndex, new Column( instant, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
            }
        } else if (x instanceof Instant) {
            colOrderedMap.put(parameterIndex, new Column( x, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
        } else if (x instanceof ZonedDateTime) {
            ZonedDateTime zonedDateTime = (ZonedDateTime) x;
            Instant instant = zonedDateTime.toInstant();
            colOrderedMap.put(parameterIndex, new Column(instant, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
        } else if (x instanceof OffsetDateTime) {
            OffsetDateTime offsetDateTime = (OffsetDateTime) x;
            Instant instant = offsetDateTime.toInstant();
            colOrderedMap.put(parameterIndex, new Column(instant, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
        } else if (x instanceof BigInteger) {
            colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_UBIGINT, parameterIndex));
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
//        List<Object> list = new ArrayList<>();
//        while (!tag.isEmpty()){
//            ColumnInfo columnInfo = tag.poll();
//            if (columnInfo.getDataList().size() != 1){
//                throw new SQLException("tag size is not equal 1");
//            }
//
//            list.add(columnInfo.getDataList().get(0));
//        }
//        if (!colOrderedMap.isEmpty()) {
//            colOrderedMap.keySet().stream().sorted().forEach(i -> {
//                Column col = this.colOrderedMap.get(i);
//                list.add(col.getData());
//            });
//        }
//        return new TSDBParameterMetaData(list.toArray(new Object[0]));

        return null;
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
        colOrderedMap.put(parameterIndex, new Column(instant, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
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
    public void setNString(int parameterIndex, String value) throws SQLException {
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



    @Override
    public void setInt(int columnIndex, List<Integer> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setFloat(int columnIndex, List<Float> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTimestamp(int columnIndex, List<Long> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setLong(int columnIndex, List<Long> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    @Override
    public void setBigInteger(int columnIndex, List<BigInteger> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    @Override
    public void setDouble(int columnIndex, List<Double> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    public void setBoolean(int columnIndex, List<Boolean> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setByte(int columnIndex, List<Byte> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setShort(int columnIndex, List<Short> list) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setString(int columnIndex, List<String> list, int size) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    @Override
    public void setVarbinary(int columnIndex, List<byte[]> list, int size) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    @Override
    public void setGeometry(int columnIndex, List<byte[]> list, int size) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    // note: expand the required space for each NChar character
    @Override
    public void setNString(int columnIndex, List<String> list, int size) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void columnDataAddBatch() throws SQLException {
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
//
//        try {
//            Files.write(Paths.get("abe.txt"), sb.toString().getBytes(StandardCharsets.UTF_8));
//        } catch (Exception e){
//
//        }
////        System.out.println(sb);
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
    @Override
    public void columnDataExecuteBatch() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTableName(String name) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
}
