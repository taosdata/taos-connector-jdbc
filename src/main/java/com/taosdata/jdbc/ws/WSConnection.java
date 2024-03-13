package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.rs.RestfulDatabaseMetaData;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.entity.*;
import com.taosdata.jdbc.ws.schemaless.CommonResp;
import com.taosdata.jdbc.ws.schemaless.InsertReq;
import com.taosdata.jdbc.ws.schemaless.SchemalessAction;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class WSConnection extends AbstractConnection {
    private final Transport transport;
    private final DatabaseMetaData metaData;
    private String database;
    private final ConnectionParam param;
    CopyOnWriteArrayList<Statement> statementList = new CopyOnWriteArrayList<>();
    private final AtomicLong insertId = new AtomicLong(0);

    public WSConnection(String url, Properties properties, Transport transport, ConnectionParam param) {
        super(properties);
        this.transport = transport;
        this.database = param.getDatabase();
        this.param = param;
        this.metaData = new RestfulDatabaseMetaData(url, properties.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        if (this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME) != null)
            database = this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        WSStatement statement = new WSStatement(transport, database, this);

        statementList.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        if (this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME) != null)
            database = this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);

        if (transport != null && !transport.isClosed()) {
            return new TSWSPreparedStatement(transport, param, database, this, sql);
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
    }

    @Override
    public void close() throws SQLException {
        for (Statement statement : statementList) {
            statement.close();
        }
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

        ConnectReq connectReq = new ConnectReq();
        connectReq.setReqId(ReqId.getReqID());
        connectReq.setUser(param.getUser());
        connectReq.setPassword(param.getPassword());
        connectReq.setDb(db);
        // 目前仅支持bi模式，下游接口值为0，此处做转换
        if(param.getConnectMode() == ConnectionParam.CONNECT_MODE_BI){
            connectReq.setMode(0);
        }

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
