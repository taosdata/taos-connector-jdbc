package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.TSDBParameterMetaData;
import com.taosdata.jdbc.common.ColumnInfo;
import com.taosdata.jdbc.common.SerializeBlock;
import com.taosdata.jdbc.enums.BindType;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.stmt.entity.ExecResp;
import com.taosdata.jdbc.ws.stmt.entity.GetColFieldsResp;
import com.taosdata.jdbc.ws.stmt.entity.RequestFactory;
import com.taosdata.jdbc.ws.stmt.entity.StmtResp;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.utils.SqlSyntaxValidator.getDatabaseName;
import static com.taosdata.jdbc.utils.SqlSyntaxValidator.isUseSql;

public class TSWSPreparedStatement extends WSStatement implements PreparedStatement {
   public static final Pattern INSERT_PATTERN = Pattern.compile(
             "insert\\s+into\\s+([.\\w]+|\\?)\\s+(using\\s+([.\\w]+)(\\s*\\(.*\\)\\s*|\\s+)tags\\s*\\(.*\\))?\\s*(\\(.*\\))?\\s*values\\s*\\(.*\\)"
   );
    private final ConnectionParam param;
    private long reqId;
    private long stmtId;
    private final String rawSql;

    private int queryTimeout = 0;
    private int precision = TimestampPrecision.MS;

    private String insertDbName;
    static private Map<String, Integer> precisionHashMap = new ConcurrentHashMap<String, Integer>();
    private final Map<Integer, Column> column = new HashMap<>();

    private final Map<Integer, Column> tag = new HashMap<>();
    private final List<ColumnInfo> data = new ArrayList<>();

    private final PriorityQueue<ColumnInfo> queue = new PriorityQueue<>();

    public TSWSPreparedStatement(Transport transport, ConnectionParam param, String database, Connection connection, String sql) throws SQLException {
        super(transport, database, connection);
        this.rawSql = sql;
        this.param = param;
        this.insertDbName = database;
        if (!sql.contains("?"))
            return;

        String useDb = null;
        Matcher matcher = INSERT_PATTERN.matcher(sql);
        if (matcher.find()) {
            if (matcher.group(1).equals("?") && matcher.group(3) != null) {
                String usingGroup = matcher.group(3);
                if (usingGroup.contains(".")) {
                    String[] split = usingGroup.split("\\.");
                    useDb = split[0];
                }
            } else {
                String usingGroup = matcher.group(1);
                if (usingGroup.contains(".")) {
                    String[] split = usingGroup.split("\\.");
                    useDb = split[0];
                }
            }

            if (useDb == null && database != null) {
                useDb = database;
            }
            if (useDb != null) {
                insertDbName = useDb;
                Integer precisionObj = precisionHashMap.get(useDb);
                if (precisionObj != null){
                    precision = precisionObj;
                } else {
                    updatePrecision(useDb);
                }
            }
        }


        reqId = ReqId.getReqID();
        Request request = RequestFactory.generateInit(reqId);
        StmtResp resp = (StmtResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }
        stmtId = resp.getStmtId();
        Request prepare = RequestFactory.generatePrepare(stmtId, reqId, sql);
        StmtResp prepareResp = (StmtResp) transport.send(prepare);
        if (Code.SUCCESS.getCode() != prepareResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(prepareResp.getCode()) + "):" + prepareResp.getMessage());
        }
    }

    private void updatePrecision(String database) throws SQLException{
        try (ResultSet resultSet = this.executeQuery("select `precision` from information_schema.ins_databases where name = '" + database + "'")) {
            while (resultSet.next()) {
                String tmp = resultSet.getString(1);
                precision = TimestampPrecision.getPrecision(tmp);
                precisionHashMap.put(database, precision);
            }
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

    private void checkUseStatement(String sql) throws SQLException {
        if (sql == null || sql.isEmpty()) {
            throw new SQLException("sql is empty");
        }

        if (isUseSql(sql)) {
            String database = getDatabaseName(sql);
            if (null != database) {
                WSConnection.reInitTransport(transport, param, database);

                try (ResultSet resultSet = this.executeQuery("select `precision` from information_schema.ins_databases where name = '" + database + "'")) {
                    while (resultSet.next()) {
                        String tmp = resultSet.getString(1);
                        precision = TimestampPrecision.getPrecision(tmp);
                    }
                }

                reqId = ReqId.getReqID();
                Request request = RequestFactory.generateInit(reqId);
                StmtResp resp = (StmtResp) transport.send(request);
                if (Code.SUCCESS.getCode() != resp.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
                }
                stmtId = resp.getStmtId();
                Request prepare = RequestFactory.generatePrepare(stmtId, reqId, rawSql);
                StmtResp prepareResp = (StmtResp) transport.send(prepare);
                if (Code.SUCCESS.getCode() != prepareResp.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(prepareResp.getCode()) + "):" + prepareResp.getMessage());
                }
            }
        }
    }

    @Override
    public boolean execute(String sql, Long reqId) throws SQLException {
        checkUseStatement(sql);
        return super.execute(sql, reqId);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        List<Object> list = new ArrayList<>();
        if (!tag.isEmpty()) {
            tag.keySet().stream().sorted().forEach(i -> {
                Column col = this.tag.get(i);
                list.add(col.data);
            });
        }
        if (!column.isEmpty()) {
            column.keySet().stream().sorted().forEach(i -> {
                Column col = this.column.get(i);
                list.add(col.data);
            });
        }
        Object[] parameters = list.toArray(new Object[0]);
        this.clearParameters();

        final String sql = Utils.getNativeSql(this.rawSql, parameters);
        return executeQuery(sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (column.isEmpty())
            throw new SQLException("no parameter to execute");
        if (!data.isEmpty())
            throw TSDBError.undeterminedExecutionError();

        //set tag
        if (!tag.isEmpty()) {
            List<ColumnInfo> collect = tag.keySet().stream().sorted().map(i -> {
                Column col = this.tag.get(i);
                return new ColumnInfo(i, col.data, col.type);
            }).collect(Collectors.toList());
            byte[] tagBlock;
            try {
                tagBlock = SerializeBlock.getRawBlock(collect, precision);
            } catch (IOException e) {
                throw new SQLException("data serialize error!", e);
            }
            StmtResp bindResp = (StmtResp) transport.send(Action.SET_TAGS.getAction(),
                    reqId, stmtId, BindType.TAG.get(), tagBlock);
            if (Code.SUCCESS.getCode() != bindResp.getCode()) {
                throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
            }
        }
        // bind
        List<ColumnInfo> collect = column.keySet().stream().sorted().map(i -> {
            Column col = this.column.get(i);
            return new ColumnInfo(i, col.data, col.type);
        }).collect(Collectors.toList());
        byte[] rawBlock;
        try {
            rawBlock = SerializeBlock.getRawBlock(collect, precision);
        } catch (IOException e) {
            throw new SQLException("data serialize error!", e);
        }
        StmtResp bindResp = (StmtResp) transport.send(Action.BIND.getAction(),
                reqId, stmtId, BindType.BIND.get(), rawBlock);
        if (Code.SUCCESS.getCode() != bindResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
        }
        // add batch
        Request batch = RequestFactory.generateBatch(stmtId, reqId);
        Response send = transport.send(batch);
        StmtResp batchResp = (StmtResp) send;
        if (Code.SUCCESS.getCode() != batchResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(batchResp.getCode()) + "):" + batchResp.getMessage());
        }
        this.clearParameters();
        // send
        Request request = RequestFactory.generateExec(stmtId, reqId);
        ExecResp resp = (ExecResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            if (TIMESTAMP_DATA_OUT_OF_RANGE == resp.getCode()){
                updatePrecision(insertDbName);
            }
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage(), "P0001", resp.getCode());
        }

        return resp.getAffected();
    }

    // set sub-table name
    public void setTableName(String name) throws SQLException {
        Request request = RequestFactory.generateSetTableName(stmtId, reqId, name);
        StmtResp resp = (StmtResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }
    }

    public void setTagSqlTypeNull(int index, int type) throws SQLException {
        switch (type) {
            case Types.BOOLEAN:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_BOOL, index));
                break;
            case Types.TINYINT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_TINYINT, index));
                break;
            case Types.SMALLINT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_SMALLINT, index));
                break;
            case Types.INTEGER:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_INT, index));
                break;
            case Types.BIGINT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_BIGINT, index));
                break;
            case Types.FLOAT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_FLOAT, index));
                break;
            case Types.DOUBLE:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_DOUBLE, index));
                break;
            case Types.TIMESTAMP:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_TIMESTAMP, index));
                break;
            case Types.BINARY:
            case Types.VARCHAR:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_BINARY, index));
                break;
            case Types.VARBINARY:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_VARBINARY, index));
                break;
            case Types.NCHAR:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_NCHAR, index));
                break;
            // json
            case Types.OTHER:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_JSON, index));
                break;
            default:
                throw new SQLException("unsupported type: " + type);
        }
    }

    public void setTagNull(int index, int type) throws SQLException {
        switch (type) {
            case TSDB_DATA_TYPE_BOOL:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_BOOL, index));
                break;
            case TSDB_DATA_TYPE_TINYINT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_TINYINT, index));
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_SMALLINT, index));
                break;
            case TSDB_DATA_TYPE_INT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_INT, index));
                break;
            case TSDB_DATA_TYPE_BIGINT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_BIGINT, index));
                break;
            case TSDB_DATA_TYPE_FLOAT:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_FLOAT, index));
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_DOUBLE, index));
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_TIMESTAMP, index));
                break;
            case TSDB_DATA_TYPE_BINARY:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_BINARY, index));
                break;
            case TSDB_DATA_TYPE_VARBINARY:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_VARBINARY, index));
                break;
            case TSDB_DATA_TYPE_GEOMETRY:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_GEOMETRY, index));
                break;
            case TSDB_DATA_TYPE_NCHAR:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_NCHAR, index));
                break;
            // json
            case TSDB_DATA_TYPE_JSON:
                tag.put(index, new Column(null, TSDB_DATA_TYPE_JSON, index));
                break;
            default:
                throw new SQLException("unsupported type: " + type);
        }
    }

    public void setTagBoolean(int index, boolean value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_BOOL, index));
    }

    public void setTagByte(int index, byte value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_TINYINT, index));
    }

    public void setTagShort(int index, short value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_SMALLINT, index));
    }

    public void setTagInt(int index, int value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_INT, index));
    }

    public void setTagLong(int index, long value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_BIGINT, index));
    }

    public void setTagFloat(int index, float value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_FLOAT, index));
    }

    public void setTagDouble(int index, double value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_DOUBLE, index));
    }

    public void setTagTimestamp(int index, long value) {
        tag.put(index, new Column(new Timestamp(value), TSDB_DATA_TYPE_TIMESTAMP, index));
    }

    public void setTagTimestamp(int index, Timestamp value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_TIMESTAMP, index));
    }

    public void setTagString(int index, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        tag.put(index, new Column(bytes, TSDB_DATA_TYPE_BINARY, index));
    }

    public void setTagVarbinary(int index, byte[] value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_VARBINARY, index));
    }
    public void setTagGeometry(int index, byte[] value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_GEOMETRY, index));
    }

    public void setTagNString(int index, String value) {
        tag.put(index, new Column(value, TSDB_DATA_TYPE_NCHAR, index));
    }

    public void setTagJson(int index, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        tag.put(index, new Column(bytes, TSDB_DATA_TYPE_JSON, index));
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
        data.clear();
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
        if (!tag.isEmpty()) {
            tag.keySet().stream().sorted().forEach(i -> {
                Column col = this.tag.get(i);
                list.add(col.data);
            });
        }
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
        List<Column> collect = column.keySet().stream().sorted().map(column::get).collect(Collectors.toList());
        if (data.isEmpty()) {
            for (Column col : collect) {
                data.add(new ColumnInfo(col.index, col.data, col.type));
            }
        } else {
            if (collect.size() != data.size()) {
                throw new SQLException("batch add column size not match, expected: " + data.size() + ", actual: " + collect.size());
            }

            for (int i = 0; i < collect.size(); i++) {
                Column col = collect.get(i);
                ColumnInfo columnInfo = data.get(i);
                if (columnInfo.getIndex() != col.index) {
                    throw new SQLException("batch add column index not match, expected: " + columnInfo.getIndex() + ", actual: " + col.index);
                }
                if (columnInfo.getType() != col.type) {
                    throw new SQLException("batch add column type not match, expected type: " + columnInfo.getType() + ", actual type: " + col.type);
                }
                columnInfo.add(col.data);
            }
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (column.isEmpty())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_BATCH_IS_EMPTY);

        //set tag
        if (!tag.isEmpty()) {
            List<ColumnInfo> collect = tag.keySet().stream().sorted().map(i -> {
                Column col = this.tag.get(i);
                return new ColumnInfo(i, col.data, col.type);
            }).collect(Collectors.toList());
            byte[] tagBlock;
            try {
                tagBlock = SerializeBlock.getRawBlock(collect, precision);
            } catch (IOException e) {
                throw new SQLException("data serialize error!", e);
            }
            StmtResp bindResp = (StmtResp) transport.send(Action.SET_TAGS.getAction(),
                    reqId, stmtId, BindType.TAG.get(), tagBlock);
            if (Code.SUCCESS.getCode() != bindResp.getCode()) {
                throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
            }
        }
        // bind
        byte[] rawBlock;
        try {
            rawBlock = SerializeBlock.getRawBlock(data, precision);
        } catch (IOException e) {
            throw new SQLException("data serialize error!", e);
        }
        StmtResp bindResp = (StmtResp) transport.send(Action.BIND.getAction(),
                reqId, stmtId, BindType.BIND.get(), rawBlock);
        if (Code.SUCCESS.getCode() != bindResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
        }
        // add batch
        Request batch = RequestFactory.generateBatch(stmtId, reqId);
        Response send = transport.send(batch);
        StmtResp batchResp = (StmtResp) send;
        if (Code.SUCCESS.getCode() != batchResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(batchResp.getCode()) + "):" + batchResp.getMessage());
        }

        this.clearParameters();
        // send
        Request request = RequestFactory.generateExec(stmtId, reqId);
        ExecResp resp = (ExecResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }
        int[] ints = new int[resp.getAffected()];
        for (int i = 0, len = ints.length; i < len; i++)
            ints[i] = SUCCESS_NO_INFO;
        return ints;
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
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        List<Object> list = new ArrayList<>();
        if (!tag.isEmpty()) {
            tag.keySet().stream().sorted().forEach(i -> {
                Column col = this.tag.get(i);
                list.add(col.data);
            });
        }
        if (!column.isEmpty()) {
            column.keySet().stream().sorted().forEach(i -> {
                Column col = this.column.get(i);
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

    public void setInt(int columnIndex, List<Integer> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_INT, Integer.BYTES);
    }

    public void setFloat(int columnIndex, List<Float> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_FLOAT, Float.BYTES);
    }

    public void setTimestamp(int columnIndex, List<Long> list) throws SQLException {
        List<Timestamp> collect = list.stream().map(x -> {
            if (x == null) {
                return null;
            }
            return new Timestamp(x);
        }).collect(Collectors.toList());
        setValueImpl(columnIndex, collect, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, Long.BYTES);
    }

    public void setLong(int columnIndex, List<Long> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BIGINT, Long.BYTES);
    }

    public void setDouble(int columnIndex, List<Double> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_DOUBLE, Double.BYTES);
    }

    public void setBoolean(int columnIndex, List<Boolean> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BOOL, Byte.BYTES);
    }

    public void setByte(int columnIndex, List<Byte> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_TINYINT, Byte.BYTES);
    }

    public void setShort(int columnIndex, List<Short> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_SMALLINT, Short.BYTES);
    }

    public void setString(int columnIndex, List<String> list, int size) throws SQLException {
        List<byte[]> collect = list.stream().map(x -> {
            if (x == null) {
                return null;
            }
            return x.getBytes(StandardCharsets.UTF_8);
        }).collect(Collectors.toList());
        setValueImpl(columnIndex, collect, TSDBConstants.TSDB_DATA_TYPE_BINARY, size);
    }

    public void setVarbinary(int columnIndex, List<byte[]> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDB_DATA_TYPE_VARBINARY, size);
    }
    public void setGeometry(int columnIndex, List<byte[]> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDB_DATA_TYPE_GEOMETRY, size);
    }
    // note: expand the required space for each NChar character
    public void setNString(int columnIndex, List<String> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_NCHAR, size * Integer.BYTES);
    }

    public <T> void setValueImpl(int columnIndex, List<T> list, int type, int bytes) throws SQLException {
        List<Object> listObject = list.stream()
                .map(Object.class::cast)
                .collect(Collectors.toList());
        ColumnInfo p = new ColumnInfo(columnIndex, listObject, type, null);
        queue.add(p);
    }

    public void columnDataAddBatch() throws SQLException {
        while (!queue.isEmpty()) {
            data.add(queue.poll());
        }
    }

    public void columnDataExecuteBatch() throws SQLException {
        //set tag
        if (!tag.isEmpty()) {
            List<ColumnInfo> collect = tag.keySet().stream().sorted().map(i -> {
                Column col = this.tag.get(i);
                return new ColumnInfo(i, col.data, col.type);
            }).collect(Collectors.toList());
            byte[] tagBlock;
            try {
                tagBlock = SerializeBlock.getRawBlock(collect, precision);
            } catch (IOException e) {
                throw new SQLException("data serialize error!", e);
            }
            StmtResp bindResp = (StmtResp) transport.send(Action.SET_TAGS.getAction(),
                    reqId, stmtId, BindType.TAG.get(), tagBlock);
            if (Code.SUCCESS.getCode() != bindResp.getCode()) {
                throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
            }
        }
        // bind
        byte[] rawBlock;
        try {
            rawBlock = SerializeBlock.getRawBlock(data, precision);
        } catch (IOException e) {
            throw new SQLException("data serialize error!", e);
        }
        StmtResp bindResp = (StmtResp) transport.send(Action.BIND.getAction(),
                reqId, stmtId, BindType.BIND.get(), rawBlock);
        if (Code.SUCCESS.getCode() != bindResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
        }
        // add batch
        Request batch = RequestFactory.generateBatch(stmtId, reqId);
        Response send = transport.send(batch);
        StmtResp batchResp = (StmtResp) send;
        if (Code.SUCCESS.getCode() != batchResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(batchResp.getCode()) + "):" + batchResp.getMessage());
        }

        this.clearParameters();
        // send
        Request request = RequestFactory.generateExec(stmtId, reqId);
        ExecResp resp = (ExecResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }
    }

    public void columnDataCloseBatch() throws SQLException {
        this.close();
    }
}
