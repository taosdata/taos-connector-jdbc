package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.common.ColumnInfo;
import com.taosdata.jdbc.common.SerializeBlock;
import com.taosdata.jdbc.common.TableInfo;
import com.taosdata.jdbc.enums.FeildBindType;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDateTime;
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
    private int toBeBindTableNameCount = 0;
    private int toBeBindColCount = 0;
    private int toBeBindTagCount = 0;
    private List<Field> fields;
    private boolean isInsert = false;


    private final Map<Integer, Column> column = new HashMap<>();

    private final PriorityQueue<ColumnInfo> tag = new PriorityQueue<>();
    private final PriorityQueue<ColumnInfo> queue = new PriorityQueue<>();

    private List<TableInfo> tableInfoList = new ArrayList<>();
    private TableInfo tableInfo;


    public TSWSPreparedStatement(Transport transport, ConnectionParam param, String database, AbstractConnection connection, String sql, Long instanceId) throws SQLException {
        super(transport, database, connection, instanceId);
        this.rawSql = sql;
        this.param = param;
        if (!sql.contains("?"))
            return;

        reqId = ReqId.getReqID();
        Request request = RequestFactory.generateInit(reqId, true, false);
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

//        if (prepareResp.getFieldsCount() != prepareResp.getFields().size()){
//            throw new SQLException("prepare error: fields count not match");
//        }

        isInsert = prepareResp.isInsert();
        if (isInsert){
            fields = prepareResp.getFields();
            for (Field field : fields){
                if (field.getBindType() == FeildBindType.TAOS_FIELD_TBNAME.getValue()){
                    toBeBindTableNameCount++;
                }
                if (field.getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
                    toBeBindTagCount ++;
                }
                if (field.getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
                    toBeBindColCount ++;
                }
            }
        } else if (!isInsert && prepareResp.getFieldsCount() > 0){
            for (Field field : fields){
                if (field.getBindType() == FeildBindType.TAOS_FIELD_QUERY.getValue()){
                    toBeBindColCount ++;
                }
            }
        } else {
            return;
        }

        this.tableInfo = TableInfo.getEmptyTableInfo();

        // now we know the number of fields, we can prepare the cache data
        if (prepareResp.isInsert()){
//            for (int i = 0; i < toBeBindColCount; i++){
//                if (prepareResp.getFields().get(i).getType() == TSDB_DATA_TYPE_UNKNOWN){
//                    column.put(i, new Column(null, TSDB_DATA_TYPE_UNKNOWN, i));
//                }
//                column.put(i, new Column(null, TSDB_DATA_TYPE_UNKNOWN, i));
//            }

        } else {

        }
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
    public ResultSet executeQuery() throws SQLException {
//        List<Object> list = new ArrayList<>();
//        if (!tag.isEmpty()) {
//            tag.keySet().stream().sorted().forEach(i -> {
//                Column col = this.tag.get(i);
//                list.add(col.data);
//            });
//        }
//        if (!column.isEmpty()) {
//            column.keySet().stream().sorted().forEach(i -> {
//                Column col = this.column.get(i);
//                list.add(col.data);
//            });
//        }
//        Object[] parameters = list.toArray(new Object[0]);
//        this.clearParameters();
//
//        final String sql = Utils.getNativeSql(this.rawSql, parameters);
//        return executeQuery(sql);


        return null;
    }

    @Override
    public int executeUpdate() throws SQLException {
//        if (column.isEmpty())
//            throw new SQLException("no parameter to execute");
//        if (!data.isEmpty())
//            throw TSDBError.undeterminedExecutionError();
//
//        //set tag
//        if (!tag.isEmpty()) {
//            List<ColumnInfo> collect = tag.keySet().stream().sorted().map(i -> {
//                Column col = this.tag.get(i);
//                return new ColumnInfo(i, col.data, col.type);
//            }).collect(Collectors.toList());
//            byte[] tagBlock;
//            try {
//                tagBlock = SerializeBlock.getRawBlock(collect, precision);
//            } catch (IOException e) {
//                throw new SQLException("data serialize error!", e);
//            }
//            Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.SET_TAGS.getAction(),
//                    reqId, stmtId, BindType.TAG.get(), tagBlock);
//            if (Code.SUCCESS.getCode() != bindResp.getCode()) {
//                throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
//            }
//        }
//        // bind
//        List<ColumnInfo> collect = column.keySet().stream().sorted().map(i -> {
//            Column col = this.column.get(i);
//            return new ColumnInfo(i, col.data, col.type);
//        }).collect(Collectors.toList());
//        byte[] rawBlock;
//        try {
//            rawBlock = SerializeBlock.getRawBlock(collect, precision);
//        } catch (IOException e) {
//            throw new SQLException("data serialize error!", e);
//        }
//        Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.BIND.getAction(),
//                reqId, stmtId, BindType.BIND.get(), rawBlock);
//        if (Code.SUCCESS.getCode() != bindResp.getCode()) {
//            throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
//        }
//        this.clearParameters();
//        // send
//        Request request = RequestFactory.generateExec(stmtId, reqId);
//        ExecResp resp = (ExecResp) transport.send(request);
//        if (Code.SUCCESS.getCode() != resp.getCode()) {
//            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage(), "P0001", resp.getCode());
//        }
//
//        return resp.getAffected();

        return 0;
    }

    // set sub-table name


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
            case TSDB_DATA_TYPE_SMALLINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_SMALLINT));
                break;
            case TSDB_DATA_TYPE_INT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_INT));
                break;
            case TSDB_DATA_TYPE_BIGINT:
                tag.add(new ColumnInfo(index, nullTag, TSDB_DATA_TYPE_BIGINT));
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
        tag.add(new ColumnInfo(index, Collections.singletonList(value), TSDB_DATA_TYPE_TIMESTAMP));
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
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_BOOL, parameterIndex));
                break;
            case Types.TINYINT:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_TINYINT, parameterIndex));
                break;
            case Types.SMALLINT:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_SMALLINT, parameterIndex));
                break;
            case Types.INTEGER:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_INT, parameterIndex));
                break;
            case Types.BIGINT:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_BIGINT, parameterIndex));
                break;
            case Types.FLOAT:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_FLOAT, parameterIndex));
                break;
            case Types.DOUBLE:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_DOUBLE, parameterIndex));
                break;
            case Types.TIMESTAMP:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
                break;
            case Types.BINARY:
            case Types.VARCHAR:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_BINARY, parameterIndex));
                break;
            case Types.VARBINARY:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_VARBINARY, parameterIndex));
                break;
            case Types.NCHAR:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_NCHAR, parameterIndex));
                break;
            // json
            case Types.OTHER:
                column.put(parameterIndex, new Column(null, TSDB_DATA_TYPE_JSON, parameterIndex));
                break;
            default:
                throw new SQLException("unsupported type: " + sqlType);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BOOL, parameterIndex));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_TINYINT, parameterIndex));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_SMALLINT, parameterIndex));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_INT, parameterIndex));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BIGINT, parameterIndex));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_FLOAT, parameterIndex));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_DOUBLE, parameterIndex));
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
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BINARY, parameterIndex));
    }

    public void setVarbinary(int parameterIndex, byte[] x) throws SQLException {
        // UTF-8
        if (x == null) {
            setNull(parameterIndex, Types.VARBINARY);
            return;
        }
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_VARBINARY, parameterIndex));
    }

    public void setGeometry(int parameterIndex, byte[] x) throws SQLException {
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_GEOMETRY, parameterIndex));
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
        column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
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
    public void clearParameters() throws SQLException {
        column.clear();
        tag.clear();

        tableInfo.setTableName("");
        tableInfo.getTagInfo().clear();
        tableInfo.getDataList().clear();
        tableInfoList.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        switch (targetSqlType) {
            case Types.BOOLEAN:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BOOL, parameterIndex));
                break;
            case Types.TINYINT:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_TINYINT, parameterIndex));
                break;
            case Types.SMALLINT:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_SMALLINT, parameterIndex));
                break;
            case Types.INTEGER:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_INT, parameterIndex));
                break;
            case Types.BIGINT:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BIGINT, parameterIndex));
                break;
            case Types.FLOAT:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_FLOAT, parameterIndex));
                break;
            case Types.DOUBLE:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_DOUBLE, parameterIndex));
                break;
            case Types.TIMESTAMP:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_TIMESTAMP, parameterIndex));
                break;
            case Types.BINARY:
            case Types.VARCHAR:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_BINARY, parameterIndex));
                break;
            case Types.VARBINARY:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_VARBINARY, parameterIndex));
                break;
            case Types.NCHAR:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_NCHAR, parameterIndex));
                break;
            // json
            case Types.OTHER:
                column.put(parameterIndex, new Column(x, TSDB_DATA_TYPE_JSON, parameterIndex));
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
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof LocalDateTime) {
            setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime)x));
        } else {
            throw new SQLException("Unsupported data type: " + x.getClass().getName());
        }
    }

    @Override
    public boolean execute() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        List<Object> list = new ArrayList<>();
//        if (!tag.isEmpty()) {
//            tag.keySet().stream().sorted().forEach(i -> {
//                Column col = this.tag.get(i);
//                list.add(col.data);
//            });
//        }
        if (!column.isEmpty()) {
            column.keySet().stream().sorted().forEach(i -> {
                Column col = this.column.get(i);
                list.add(col.data);
            });
        }
        Object[] parameters = list.toArray(new Object[0]);
        this.clearParameters();
        final String sql = Utils.getNativeSql(this.rawSql, parameters);
        return execute(sql);
    }

    @Override
    public void addBatch() throws SQLException {
//        List<Column> collect = column.keySet().stream().sorted().map(column::get).collect(Collectors.toList());
//        if (data.isEmpty()) {
//            for (Column col : collect) {
//                data.add(new ColumnInfo(col.index, col.data, col.type));
//            }
//        } else {
//            if (collect.size() != data.size()) {
//                throw new SQLException("batch add column size not match, expected: " + data.size() + ", actual: " + collect.size());
//            }
//
//            for (int i = 0; i < collect.size(); i++) {
//                Column col = collect.get(i);
//                ColumnInfo columnInfo = data.get(i);
//                if (columnInfo.getIndex() != col.index) {
//                    throw new SQLException("batch add column index not match, expected: " + columnInfo.getIndex() + ", actual: " + col.index);
//                }
//                if (columnInfo.getType() != col.type) {
//                    throw new SQLException("batch add column type not match, expected type: " + columnInfo.getType() + ", actual type: " + col.type);
//                }
//                columnInfo.add(col.data);
//            }
//        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
//        if (column.isEmpty())
//            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_BATCH_IS_EMPTY);
//
//        //set tag
//        if (!tag.isEmpty()) {
//            List<ColumnInfo> collect = tag.keySet().stream().sorted().map(i -> {
//                Column col = this.tag.get(i);
//                return new ColumnInfo(i, col.data, col.type);
//            }).collect(Collectors.toList());
//            byte[] tagBlock;
//            try {
//                tagBlock = SerializeBlock.getRawBlock(collect, precision);
//            } catch (IOException e) {
//                throw new SQLException("data serialize error!", e);
//            }
//            Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.SET_TAGS.getAction(),
//                    reqId, stmtId, BindType.TAG.get(), tagBlock);
//            if (Code.SUCCESS.getCode() != bindResp.getCode()) {
//                throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
//            }
//        }
//        // bind
//        byte[] rawBlock;
//        try {
//            rawBlock = SerializeBlock.getRawBlock(data, precision);
//        } catch (IOException e) {
//            throw new SQLException("data serialize error!", e);
//        }
//        Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.BIND.getAction(),
//                reqId, stmtId, BindType.BIND.get(), rawBlock);
//        if (Code.SUCCESS.getCode() != bindResp.getCode()) {
//            throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
//        }
//
//        this.clearParameters();
//        // send
//        Request request = RequestFactory.generateExec(stmtId, reqId);
//        ExecResp resp = (ExecResp) transport.send(request);
//        if (Code.SUCCESS.getCode() != resp.getCode()) {
//            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
//        }
//        int[] ints = new int[resp.getAffected()];
//        for (int i = 0, len = ints.length; i < len; i++)
//            ints[i] = SUCCESS_NO_INFO;
//        return ints;
        return null;
    }

    @Override
    public void close() throws SQLException {
        super.close();
        Request close = RequestFactory.generateClose(stmtId, reqId);
        transport.sendWithoutResponse(close);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (this.getResultSet() == null)
            return null;
        return getResultSet().getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
//        if (isClosed())
//            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
//        List<Object> list = new ArrayList<>();
//        if (!tag.isEmpty()) {
//            tag.keySet().stream().sorted().forEach(i -> {
//                Column col = this.tag.get(i);
//                list.add(col.data);
//            });
//        }
//        if (!column.isEmpty()) {
//            column.keySet().stream().sorted().forEach(i -> {
//                Column col = this.column.get(i);
//                list.add(col.data);
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
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
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
        column.put(parameterIndex, new Column(value, TSDB_DATA_TYPE_NCHAR, parameterIndex));
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
        List<Timestamp> collect = list.stream().map(x -> {
            if (x == null) {
                return null;
            }
            return new Timestamp(x);
        }).collect(Collectors.toList());
        setValueImpl(columnIndex, collect, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, Long.BYTES);
    }

    @Override
    public void setLong(int columnIndex, List<Long> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BIGINT, Long.BYTES);
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
        List<Object> listObject = list.stream()
                .map(Object.class::cast)
                .collect(Collectors.toList());
        ColumnInfo p = new ColumnInfo(columnIndex, listObject, type);
        queue.add(p);
    }

    @Override
    public void columnDataAddBatch() throws SQLException {
        while (!tag.isEmpty()){
            tableInfo.getTagInfo().add(tag.poll());
        }
        while (!queue.isEmpty()) {
            tableInfo.getDataList().add(queue.poll());
        }
        tableInfoList.add(tableInfo);
        tableInfo = TableInfo.getEmptyTableInfo();
    }

    @Override
    public void columnDataExecuteBatch() throws SQLException {
        if (tableInfoList.isEmpty()) {
            throw new SQLException("batch data is empty");
        }

        byte[] rawBlock;
        try {
            rawBlock = SerializeBlock.getStmt2BindBlock(reqId, stmtId, tableInfoList, toBeBindTableNameCount, toBeBindTagCount, toBeBindColCount, precision);
        } catch (IOException e) {
            throw new SQLException("data serialize error!", e);
        }
        Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.STMT2_BIND.getAction(),
                reqId, rawBlock);
        if (Code.SUCCESS.getCode() != bindResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
        }

        this.clearParameters();
        // send
        Request request = RequestFactory.generateExec(stmtId, reqId);
        Stmt2ExecResp resp = (Stmt2ExecResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }
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
