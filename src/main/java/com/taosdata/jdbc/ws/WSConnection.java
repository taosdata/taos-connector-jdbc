package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.entity.*;
import com.taosdata.jdbc.ws.schemaless.InsertReq;
import com.taosdata.jdbc.ws.schemaless.SchemalessAction;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.RequestFactory;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class WSConnection extends AbstractConnection {

    public static final AtomicBoolean g_FirstConnection = new AtomicBoolean(true);
    private final Transport transport;
    private final DatabaseMetaData metaData;
    private String database;
    private final ConnectionParam param;
    private final AtomicLong insertId = new AtomicLong(0);

    public WSConnection(String url, Properties properties, Transport transport, ConnectionParam param, String serverVersion) {
        super(properties, serverVersion);
        this.transport = transport;
        this.database = param.getDatabase();
        this.param = param;
        this.metaData = new WSDatabaseMetaData(url, properties.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        if (this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME) != null)
            database = this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        WSStatement statement = new WSStatement(transport, database, this, idGenerator.getAndIncrement(), param.getZoneId());

        statementsMap.put(statement.getInstanceId(), statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        if (this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME) != null) {
            database = this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        }

        boolean efficientWritingSql = false;
        if (sql.startsWith("ASYNC_INSERT")){
            sql = sql.substring("ASYNC_".length());
            efficientWritingSql = true;
        }

        if (!sql.contains("?")){
            return new TSWSPreparedStatement(transport,
                    param,
                    database,
                    this,
                    sql,
                    idGenerator.getAndIncrement());
        }

        if (transport != null && !transport.isClosed()) {
            long reqId = ReqId.getReqID();
            Request request = com.taosdata.jdbc.ws.stmt2.entity.RequestFactory.generateInit(reqId, true, true);
            Stmt2Resp resp = (Stmt2Resp) transport.send(request);
            if (Code.SUCCESS.getCode() != resp.getCode()) {
                throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
            }
            long stmtId = resp.getStmtId();
            Request prepare = RequestFactory.generatePrepare(stmtId, reqId, sql);
            Stmt2PrepareResp prepareResp = (Stmt2PrepareResp) transport.send(prepare);
            if (Code.SUCCESS.getCode() != prepareResp.getCode()) {
                Request close = RequestFactory.generateClose(stmtId, reqId);
                transport.sendWithoutResponse(close);
                throw new SQLException("(0x" + Integer.toHexString(prepareResp.getCode()) + "):" + prepareResp.getMessage());
            }

            boolean isInsert = prepareResp.isInsert();
            boolean isSuperTable = false;
            if (isInsert){
                for (Field field : prepareResp.getFields()){
                    if (field.getBindType() == FieldBindType.TAOS_FIELD_TBNAME.getValue()){
                        isSuperTable = true;
                        break;
                    }
                }
            }

            if ((efficientWritingSql || "STMT".equalsIgnoreCase(param.getAsyncWrite())) && isInsert && isSuperTable) {
                return new WSEWPreparedStatement(transport,
                        param,
                        database,
                        this,
                        sql,
                        idGenerator.getAndIncrement(),
                        prepareResp);
            } else {
                if ("line".equalsIgnoreCase(param.getPbsMode())){
                    if (super.supportLineBind) {
                        return new WSRowPreparedStatement(transport,
                                param,
                                database,
                                this,
                                sql,
                                idGenerator.getAndIncrement(),
                                prepareResp);
                    } else {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_LINE_BIND_MODE_UNSUPPORTED_IN_SERVER);
                    }
                }
                return new TSWSPreparedStatement(transport,
                        param,
                        database,
                        this,
                        sql,
                        idGenerator.getAndIncrement(),
                        prepareResp);
            }
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
    }

    @Override
    public void close() throws SQLException {
        for (Map.Entry<Long, Statement> entry : statementsMap.entrySet()) {
            Statement value = entry.getValue();
            value.close();
        }
        statementsMap.clear();
        transport.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return transport.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return this.metaData;
    }

    public static void reInitTransport(Transport transport, ConnectionParam param, String db) throws SQLException {
        transport.disconnectAndReconnect();

        ConnectReq connectReq = new ConnectReq(param);
        ConnectResp auth = (ConnectResp) transport.send(new Request(Action.CONN.getAction(), connectReq));

        if (Code.SUCCESS.getCode() != auth.getCode()) {
            transport.close();
            throw new SQLException("(0x" + Integer.toHexString(auth.getCode()) + "):" + "auth failure:" + auth.getMessage());
        }
    }

    public ConnectionParam getParam() {
        return param;
    }

    @Override
    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        for (String line : lines) {
            InsertReq insertReq = new InsertReq();
            insertReq.setReqId(insertId.getAndIncrement());
            insertReq.setProtocol(protocolType.ordinal());
            insertReq.setPrecision(timestampType.getType());
            insertReq.setData(line);
            if (ttl != null)
                insertReq.setTtl(ttl);
            if (reqId != null)
                insertReq.setReqId(reqId);
            CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq));
            if (Code.SUCCESS.getCode() != response.getCode()) {
                throw new SQLException("0x" + Integer.toHexString(response.getCode()) + ":" + response.getMessage());
            }
        }
    }

    @Override
    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        InsertReq insertReq = new InsertReq();
        insertReq.setReqId(insertId.getAndIncrement());
        insertReq.setProtocol(protocolType.ordinal());
        insertReq.setPrecision(timestampType.getType());
        insertReq.setData(line);
        if (ttl != null)
            insertReq.setTtl(ttl);
        if (reqId != null)
            insertReq.setReqId(reqId);
        CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq));
        if (Code.SUCCESS.getCode() != response.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(response.getCode()) + "):" + response.getMessage());
        }
        // websocket don't return the num of schemaless insert
        return 0;
    }
}
