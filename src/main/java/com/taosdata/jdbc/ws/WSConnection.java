package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.rs.RestfulDatabaseMetaData;
import com.taosdata.jdbc.ws.entity.*;

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
        connectReq.setReqId(1);
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
}
