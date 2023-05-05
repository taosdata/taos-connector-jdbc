package com.taosdata.jdbc.ws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.rs.RestfulDatabaseMetaData;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.stmt.entity.ConnReq;
import com.taosdata.jdbc.ws.stmt.entity.ConnResp;
import com.taosdata.jdbc.ws.stmt.entity.STMTAction;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

public class WSConnection extends AbstractConnection {
    private final Transport transport;
    private final DatabaseMetaData metaData;
    private String database;
    private final ConnectionParam param;

    // prepare statement
    private Transport prepareTransport;

    CopyOnWriteArrayList<Statement> statementList = new CopyOnWriteArrayList<>();

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

        if (prepareTransport != null && !prepareTransport.isClosed()) {
            return new TSWSPreparedStatement(transport, prepareTransport, param, database, this, sql);
        } else {
            synchronized (this) {
                if (prepareTransport != null && !prepareTransport.isClosed()) {
                    return new TSWSPreparedStatement(transport, prepareTransport, param, database, this, sql);
                } else {
                    this.prepareTransport = WSConnection.initPrepareTransport(param, database);
                }
            }
        }
        TSWSPreparedStatement preparedStatement = new TSWSPreparedStatement(transport, prepareTransport, param, database, this, sql);
        statementList.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public void close() throws SQLException {
        for (Statement statement : statementList) {
            statement.close();
        }
        transport.close();
        if (prepareTransport != null) {
            prepareTransport.close();
        }
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

    public static Transport initPrepareTransport(ConnectionParam param, String db) throws SQLException {
        InFlightRequest inFlightRequest = new InFlightRequest(param.getRequestTimeout(), param.getMaxRequest());
        Transport ts = new Transport(WSFunction.STMT, param, inFlightRequest);

        ts.setTextMessageHandler(message -> {
            JSONObject jsonObject = JSON.parseObject(message);
            STMTAction action = STMTAction.of(jsonObject.getString("action"));
            Response response = jsonObject.toJavaObject(action.getClazz());
            FutureResponse remove = inFlightRequest.remove(response.getAction(), response.getReqId());
            if (null != remove) {
                remove.getFuture().complete(response);
            }
        });

        Transport.checkConnection(ts, param.getConnectTimeout());

        ConnReq connectReq = new ConnReq();
        connectReq.setReqId(ReqId.getReqID());
        connectReq.setUser(param.getUser());
        connectReq.setPassword(param.getPassword());
        connectReq.setDb(db);
        ConnResp auth = (ConnResp) ts.send(new Request(STMTAction.CONN.getAction(), connectReq));

        if (Code.SUCCESS.getCode() != auth.getCode()) {
            throw new SQLException("0x" + Integer.toHexString(auth.getCode()) + ":" + "prepareStatement auth failure:" + auth.getMessage());
        }

        return ts;
    }
}
