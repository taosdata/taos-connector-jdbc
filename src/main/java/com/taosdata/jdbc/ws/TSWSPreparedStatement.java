package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.common.ColumnInfo;
import com.taosdata.jdbc.common.SerializeBlock;
import com.taosdata.jdbc.common.TableInfo;
import com.taosdata.jdbc.enums.FeildBindType;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.taosdata.jdbc.TSDBConstants.*;

public class TSWSPreparedStatement extends WSStatement implements TaosPrepareStatement {
    private static final List<Object> nullTag = Collections.singletonList(null);

    private final ConnectionParam param;
    private long reqId;
    private long stmtId;
    private final String rawSql;

    private int queryTimeout = 0;
    private int precision = TimestampPrecision.MS;
    private int toBeBindTableNameIndex = -1;
    private int toBeBindColCount = 0;
    private int toBeBindTagCount = 0;
    private List<Field> fields;
    private ArrayList<Integer> tagTypeList = new ArrayList<>();
    private ArrayList<Integer> colTypeList = new ArrayList<>();
    private boolean isInsert = false;
    private final Map<Integer, Column> colOrderedMap = new TreeMap<>();
    private final PriorityQueue<ColumnInfo> tag = new PriorityQueue<>();
    private final PriorityQueue<ColumnInfo> colListQueue = new PriorityQueue<>();
    private List<TableInfo> tableInfoList = new ArrayList<>();
    private TableInfo tableInfo;


    public TSWSPreparedStatement(Transport transport, ConnectionParam param, String database, AbstractConnection connection, String sql, Long instanceId, ZoneId zoneId) throws SQLException {
        super(transport, database, connection, instanceId, zoneId);
        this.rawSql = sql;
        this.param = param;
        if (!sql.contains("?"))
            return;

        reqId = ReqId.getReqID();
        Request request = RequestFactory.generateInit(reqId, true, true);
        Stmt2Resp resp = (Stmt2Resp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }
        stmtId = resp.getStmtId();
        Request prepare = RequestFactory.generatePrepare(stmtId, reqId, sql);
        Stmt2PrepareResp prepareResp = (Stmt2PrepareResp) transport.send(prepare);
        if (Code.SUCCESS.getCode() != prepareResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(prepareResp.getCode()) + "):" + prepareResp.getMessage());
        }

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
                    tagTypeList.add((int) field.getFieldType());
                }
                if (field.getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
                    toBeBindColCount++;
                    colTypeList.add((int) field.getFieldType());
                }
            }
        } else if (prepareResp.getFieldsCount() > 0){
            toBeBindColCount = prepareResp.getFieldsCount();
        }

        this.tableInfo = TableInfo.getEmptyTableInfo();
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
    public boolean execute(String sql, Long reqId) throws SQLException {
        return super.execute(sql, reqId);
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

        if (!tag.isEmpty() || !colListQueue.isEmpty()){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "The query SQL only support bind columns.");
        }

        // only support jdbc standard bind api
        if (colOrderedMap.isEmpty()){
            return executeQuery(this.rawSql);
        }

        onlyBindCol();
        if (!isTableInfoEmpty()){
            tableInfoList.add(tableInfo);
        }

        this.executeBatchImpl();

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

        if (colOrderedMap.size() == fields.size()){
            // bind all
            bindAllColWithStdApi();
        } else{
            // mixed standard api and extended api, only support one table
            onlyBindTag();
            onlyBindCol();
        }

        if (isTableInfoEmpty()){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEUPDATE, "no data to be bind");
        }

        tableInfoList.add(tableInfo);
        return executeBatchImpl();
    }

    public void setTagSqlTypeNull(int index, int type) throws SQLException {
        switch (type) {
            case Types.BOOLEAN:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_BOOL));
                break;
            case Types.TINYINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_TINYINT));
                break;
            case Types.SMALLINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_SMALLINT));
                break;
            case Types.INTEGER:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_INT));
                break;
            case Types.BIGINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_BIGINT));
                break;
            case Types.FLOAT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_FLOAT));
                break;
            case Types.DOUBLE:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_DOUBLE));
                break;
            case Types.TIMESTAMP:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_TIMESTAMP));
                break;
            case Types.BINARY:
            case Types.VARCHAR:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_BINARY));
                break;
            case Types.VARBINARY:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_VARBINARY));
                break;
            case Types.NCHAR:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_NCHAR));
                break;
            // json
            case Types.OTHER:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_JSON));
                break;
            default:
                throw new SQLException("unsupported type: " + type);
        }
    }

    @Override
    public void setTagNull(int index, int type) throws SQLException {
        switch (type) {
            case TSDB_DATA_TYPE_BOOL:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_BOOL));
                break;
            case TSDB_DATA_TYPE_TINYINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_TINYINT));
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_UTINYINT));
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_SMALLINT));
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_USMALLINT));
                break;
            case TSDB_DATA_TYPE_INT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_INT));
                break;
            case TSDB_DATA_TYPE_UINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_UINT));
                break;
            case TSDB_DATA_TYPE_BIGINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_BIGINT));
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_UBIGINT));
                break;
            case TSDB_DATA_TYPE_FLOAT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_FLOAT));
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_DOUBLE));
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_TIMESTAMP));
                break;
            case TSDB_DATA_TYPE_BINARY:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_BINARY));
                break;
            case TSDB_DATA_TYPE_VARBINARY:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_VARBINARY));
                break;
            case TSDB_DATA_TYPE_GEOMETRY:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_GEOMETRY));
                break;
            case TSDB_DATA_TYPE_NCHAR:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_NCHAR));
                break;
            // json
            case TSDB_DATA_TYPE_JSON:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_JSON));
                break;
            default:
                throw new SQLException("unsupported type: " + type);
        }
    }

    @Override
    public void setTagBoolean(int index, boolean value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_BOOL));
    }

    @Override
    public void setTagByte(int index, byte value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_TINYINT));
    }

    @Override
    public void setTagShort(int index, short value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_SMALLINT));
    }

    @Override
    public void setTagInt(int index, int value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_INT));
    }

    @Override
    public void setTagLong(int index, long value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_BIGINT));
    }
    @Override
    public void setTagBigInteger(int index, BigInteger value) throws SQLException {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_BIGINT));
    }
    @Override
    public void setTagFloat(int index, float value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_FLOAT));
    }

    @Override
    public void setTagDouble(int index, double value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_DOUBLE));
    }

    @Override
    public void setTagTimestamp(int index, long value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(new Timestamp(value)), TSDB_DATA_TYPE_TIMESTAMP));
    }

    @Override
    public void setTagTimestamp(int index, Timestamp value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(DateTimeUtils.toInstant(value, this.zoneId)), TSDB_DATA_TYPE_TIMESTAMP));
    }

    @Override
    public void setTagString(int index, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        tag.add(new ColumnInfo(index, Collections.singletonList(bytes), TSDB_DATA_TYPE_BINARY));
    }

    @Override
    public void setTagVarbinary(int index, byte[] value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_VARBINARY));
    }
    @Override
    public void setTagGeometry(int index, byte[] value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_GEOMETRY));
    }

    @Override
    public void setTagNString(int index, String value) {
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_NCHAR));
    }

    @Override
    public void setTagJson(int index, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        tag.add(new ColumnInfo(index, Collections.singletonList(bytes), TSDB_DATA_TYPE_JSON));
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
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BOOL, parameterIndex));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_TINYINT, parameterIndex));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_SMALLINT, parameterIndex));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_INT, parameterIndex));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BIGINT, parameterIndex));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_FLOAT, parameterIndex));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        colOrderedMap.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_DOUBLE, parameterIndex));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        // UTF-8
        if (x == null) {
            setNull(parameterIndex, Types.VARCHAR);
            return;
        }
        byte[] bytes = x.getBytes(StandardCharsets.UTF_8);
        setBytes(parameterIndex, bytes);
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
        colOrderedMap.put(parameterIndex, new Column(DateTimeUtils.toInstant(x, this.zoneId), TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
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
        colOrderedMap.clear();
        tag.clear();
        colListQueue.clear();

        tableInfo = TableInfo.getEmptyTableInfo();
        tableInfoList.clear();
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

    private void bindAllToTableInfo(){
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).getBindType() == FeildBindType.TAOS_FIELD_TBNAME.getValue()) {
                if (colOrderedMap.get(index + 1).data instanceof byte[]){
                    tableInfo.setTableName(new String((byte[])colOrderedMap.get(index + 1).data, StandardCharsets.UTF_8));
                }
                if (colOrderedMap.get(index + 1).data instanceof String){
                    tableInfo.setTableName((String) colOrderedMap.get(index + 1).data);
                }
            } else if (fields.get(index).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()) {
                LinkedList<Object> list = new LinkedList<>();
                list.add(colOrderedMap.get(index + 1).data);
                tableInfo.getTagInfo().add(new ColumnInfo(index + 1, list, fields.get(index).getFieldType()));
            } else if (fields.get(index).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()) {
                LinkedList<Object> list = new LinkedList<>();
                list.add(colOrderedMap.get(index + 1).data);
                tableInfo.getDataList().add(new ColumnInfo(index + 1, list, fields.get(index).getFieldType()));
            }
        }
    }


    private void bindColToTableInfo(){
        for (ColumnInfo columnInfo: tableInfo.getDataList()){
            columnInfo.add(colOrderedMap.get(columnInfo.getIndex()).data);
        }
    }

    private void bindAllColWithStdApi() {
        if (isTableInfoEmpty()) {
            // first time, bind all
            bindAllToTableInfo();
        } else {
            if (toBeBindTableNameIndex >= 0) {
                Object tbname = colOrderedMap.get(toBeBindTableNameIndex + 1).data;
                if ((tbname instanceof String && tableInfo.getTableName().equals(tbname))
                        || (tbname instanceof byte[] && tableInfo.getTableName().equals(new String((byte[]) tbname, StandardCharsets.UTF_8)))) {
                    // same table, only bind col
                    bindColToTableInfo();
                } else {
                    // different table, flush tableInfo and create a new one
                    tableInfoList.add(tableInfo);
                    tableInfo = TableInfo.getEmptyTableInfo();
                    bindAllToTableInfo();
                }
            } else {
                // must same table
                bindColToTableInfo();
            }
        }
    }

    private void onlyBindCol() {
        if (tableInfo.getDataList().isEmpty()){
            for (Map.Entry<Integer, Column> entry : colOrderedMap.entrySet()) {
                Column col = entry.getValue();
                List<Object> list = new ArrayList<>();
                list.add(col.data);

                int type = col.type;
                if (isInsert){
                    type = colTypeList.get(col.index - 1);
                }
                tableInfo.getDataList().add(new ColumnInfo(entry.getKey(), list, type));
            }
        } else {
            for (Map.Entry<Integer, Column> entry : colOrderedMap.entrySet()) {
                Column col = entry.getValue();
                tableInfo.getDataList().get(col.index - 1).add(col.data);
            }
        }
    }
    private void onlyBindTag() throws SQLException {
        if (!tableInfo.getTagInfo().isEmpty()){
            return;
        }

        if (isInsert && tag.size() != toBeBindTagCount){
            throw new SQLException("tag size is not equal to toBeBindTagCount");
        }
        while (!tag.isEmpty()) {
            ColumnInfo columnInfo = tag.poll();
            if (isInsert && columnInfo.getType() != tagTypeList.get(columnInfo.getIndex())){
                tableInfo.getTagInfo().add(new ColumnInfo(columnInfo.getIndex(), columnInfo.getDataList(), tagTypeList.get(columnInfo.getIndex())));
            } else {
                tableInfo.getTagInfo().add(columnInfo);
            }
        }
    }

    @Override
    // Only support batch insert
    public void addBatch() throws SQLException {
        if (colOrderedMap.size() == fields.size()){
            // jdbc standard bind api
            bindAllColWithStdApi();
            return;
        }

        // mixed standard api and extended api, only support one table
        onlyBindTag();
        onlyBindCol();
    }

    private boolean isTableInfoEmpty(){
        return StringUtils.isEmpty(tableInfo.getTableName())
                && tableInfo.getTagInfo().isEmpty()
                && tableInfo.getDataList().isEmpty();
    }
    @Override
    public int[] executeBatch() throws SQLException {

        if (!isTableInfoEmpty()){
            tableInfoList.add(tableInfo);
        }

        int affected = executeBatchImpl();
        int[] ints = new int[affected];
        for (int i = 0, len = ints.length; i < len; i++)
            ints[i] = SUCCESS_NO_INFO;
        return ints;
    }

    @Override
    public void close() throws SQLException {
        super.close();
        Request close = RequestFactory.generateClose(stmtId, reqId);
        transport.sendWithoutResponse(close);
        Stmt2Resp resp = (Stmt2Resp) transport.send(close);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }
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
        List<Object> list = new ArrayList<>();
        while (!tag.isEmpty()){
            ColumnInfo columnInfo = tag.poll();
            if (columnInfo.getDataList().size() != 1){
                throw new SQLException("tag size is not equal 1");
            }

            list.add(columnInfo.getDataList().get(0));
        }
        if (!colOrderedMap.isEmpty()) {
            colOrderedMap.keySet().stream().sorted().forEach(i -> {
                Column col = this.colOrderedMap.get(i);
                list.add(col.data);
            });
        }
        return new TSDBParameterMetaData(list.toArray(new Object[0]));
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
        colOrderedMap.put(parameterIndex, new Column(value, TSDB_DATA_TYPE_NCHAR, parameterIndex));
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

    static class Column {
        private final Object data;
        // taos data type
        private final int type;
        private final int index;

        public Column(Object data, int type, int index) {
            this.data = data;
            this.type = type;
            this.index = index;
        }
    }

    @Override
    public void setInt(int columnIndex, List<Integer> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_INT, Integer.BYTES);
    }

    @Override
    public void setFloat(int columnIndex, List<Float> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_FLOAT, Float.BYTES);
    }

    @Override
    public void setTimestamp(int columnIndex, List<Long> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, Long.BYTES);
    }

    @Override
    public void setLong(int columnIndex, List<Long> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BIGINT, Long.BYTES);
    }
    @Override
    public void setBigInteger(int columnIndex, List<BigInteger> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDB_DATA_TYPE_UBIGINT, Long.BYTES);
    }
    @Override
    public void setDouble(int columnIndex, List<Double> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_DOUBLE, Double.BYTES);
    }

    public void setBoolean(int columnIndex, List<Boolean> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BOOL, Byte.BYTES);
    }

    @Override
    public void setByte(int columnIndex, List<Byte> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_TINYINT, Byte.BYTES);
    }

    @Override
    public void setShort(int columnIndex, List<Short> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_SMALLINT, Short.BYTES);
    }

    @Override
    public void setString(int columnIndex, List<String> list, int size) throws SQLException {
        List<byte[]> collect = list.stream().map(x -> {
            if (x == null) {
                return null;
            }
            return x.getBytes(StandardCharsets.UTF_8);
        }).collect(Collectors.toList());
        setValueImpl(columnIndex, collect, TSDBConstants.TSDB_DATA_TYPE_BINARY, size);
    }
    @Override
    public void setVarbinary(int columnIndex, List<byte[]> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDB_DATA_TYPE_VARBINARY, size);
    }
    @Override
    public void setGeometry(int columnIndex, List<byte[]> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDB_DATA_TYPE_GEOMETRY, size);
    }
    // note: expand the required space for each NChar character
    @Override
    public void setNString(int columnIndex, List<String> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_NCHAR, size * Integer.BYTES);
    }

    public <T> void setValueImpl(int columnIndex, List<T> list, int type, int bytes) throws SQLException {
        List<Object> listObject = new ArrayList<>(list);
        if (isInsert){
            type = colTypeList.get(columnIndex);
        }
        ColumnInfo p = new ColumnInfo(columnIndex, listObject, type);
        colListQueue.add(p);
    }

    @Override
    public void columnDataAddBatch() throws SQLException {
        if (!colOrderedMap.isEmpty()){
            throw new SQLException("column data is not empty");
        }

        while (!tag.isEmpty()){
            ColumnInfo columnInfo = tag.poll();
            if (isInsert && columnInfo.getType() != tagTypeList.get(columnInfo.getIndex())){
                tableInfo.getTagInfo().add(new ColumnInfo(columnInfo.getIndex(), columnInfo.getDataList(), tagTypeList.get(columnInfo.getIndex())));
            } else {
                tableInfo.getTagInfo().add(columnInfo);
            }

        }
        while (!colListQueue.isEmpty()) {
            ColumnInfo columnInfo = colListQueue.poll();
            if (isInsert && columnInfo.getType() != colTypeList.get(columnInfo.getIndex())){
                tableInfo.getDataList().add(new ColumnInfo(columnInfo.getIndex(), columnInfo.getDataList(), colTypeList.get(columnInfo.getIndex())));
            } else {
                tableInfo.getDataList().add(columnInfo);
            }
        }
        tableInfoList.add(tableInfo);
        tableInfo = TableInfo.getEmptyTableInfo();
    }


    private int executeBatchImpl() throws SQLException {
        if (tableInfoList.isEmpty()) {
            throw new SQLException("batch data is empty");
        }

        byte[] rawBlock;
        try {
            rawBlock = SerializeBlock.getStmt2BindBlock(reqId, stmtId, tableInfoList, toBeBindTableNameIndex, toBeBindTagCount, toBeBindColCount, precision);
        } catch (IOException e) {
            throw new SQLException("data serialize error!", e);
        } finally {
            this.clearParameters();
        }

        // bind
        Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.STMT2_BIND.getAction(),
                reqId, rawBlock);
        if (Code.SUCCESS.getCode() != bindResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
        }

        // execute
        Request request = RequestFactory.generateExec(stmtId, reqId);
        Stmt2ExecResp resp = (Stmt2ExecResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }

        this.affectedRows = resp.getAffected();
        return this.affectedRows;
    }
    @Override
    public void columnDataExecuteBatch() throws SQLException {
        executeBatchImpl();
    }

    public void columnDataCloseBatch() throws SQLException {
        this.close();
    }

    @Override
    public void setTableName(String name) throws SQLException {
        this.tableInfo.setTableName(name);
    }

    private void ensureTagCapacity(int index) {
        if (this.tableInfo.getTagInfo().size() < index + 1) {
            int delta = index + 1 - this.tableInfo.getTagInfo().size();
            this.tableInfo.getTagInfo().addAll(Collections.nCopies(delta, null));
        }
    }

}
