package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.*;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.WSConnection;
import com.taosdata.jdbc.ws.entity.*;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Base64;
import java.util.Properties;
import java.util.logging.Logger;

public class RestfulDriver extends AbstractDriver {

    private static final String URL_PREFIX = "jdbc:TAOS-RS://";

    static {
        try {
            DriverManager.registerDriver(new RestfulDriver());
        } catch (SQLException e) {
            throw TSDBError.createRuntimeException(TSDBErrorNumbers.ERROR_URL_NOT_SET, e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // throw SQLException if url is null
        if (url == null || url.trim().isEmpty() || url.trim().replaceAll("\\s", "").isEmpty())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_URL_NOT_SET);

        // return null if url is not be accepted
        if (!acceptsURL(url))
            return null;

        Properties props = parseURL(url, info);
        ConnectionParam param = ConnectionParam.getParam(props);
        String batchLoad = info.getProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD);
        if (Boolean.parseBoolean(batchLoad)) {
            return getWSConnection(url, param, props);
        }
        HttpClientPoolUtil.init(props);

        String auth = null;

        if (param.getUser() != null && param.getPassword() != null) {
            auth = Base64.getEncoder().encodeToString(
                    (param.getUser() + ":" + param.getPassword()).getBytes(StandardCharsets.UTF_8));
        }

        RestfulConnection conn = new RestfulConnection(param.getHost(), param.getPort(), props, param.getDatabase(),
                url, auth, param.isUseSsl(), param.getCloudToken(), param.getTz());
        if (param.getDatabase() != null && !param.getDatabase().trim().replaceAll("\\s", "").isEmpty()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("use " + param.getDatabase());
            }
        }
        return conn;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_URL_NOT_SET);
        return url.trim().length() > 0 && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }
        if (acceptsURL(url)) {
            info = parseURL(url, info);
        }
        return getPropertyInfo(info);
    }

    @Override
    public int getMajorVersion() {
        return 3;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    private Connection getWSConnection(String url, ConnectionParam param, Properties props) throws SQLException {
        InFlightRequest inFlightRequest = new InFlightRequest(param.getRequestTimeout(), param.getMaxRequest());
        Transport transport = new Transport(WSFunction.WS, param, inFlightRequest);

        transport.setTextMessageHandler(message -> {
            JSONObject jsonObject = JSON.parseObject(message);
            Action action = Action.of(jsonObject.getString("action"));
            Response response = jsonObject.toJavaObject(action.getResponseClazz());
            FutureResponse remove = inFlightRequest.remove(response.getAction(), response.getReqId());
            if (null != remove) {
                remove.getFuture().complete(response);
            }
        });
        transport.setBinaryMessageHandler(byteBuffer -> {
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.position(8);
            long id = byteBuffer.getLong();
            FutureResponse remove = inFlightRequest.remove(Action.FETCH_BLOCK.getAction(), id);
            if (null != remove) {
                FetchBlockResp fetchBlockResp = new FetchBlockResp(id, byteBuffer);
                remove.getFuture().complete(fetchBlockResp);
            }
        });

        Transport.checkConnection(transport, param.getConnectTimeout());

        ConnectReq connectReq = new ConnectReq();
        connectReq.setReqId(1);
        connectReq.setUser(param.getUser());
        connectReq.setPassword(param.getPassword());
        connectReq.setDb(param.getDatabase());
        ConnectResp auth = (ConnectResp) transport.send(new Request(Action.CONN.getAction(), connectReq));

        if (Code.SUCCESS.getCode() != auth.getCode()) {
            throw new SQLException("0x" + Integer.toHexString(auth.getCode()) + ":" + "auth failure:" + auth.getMessage());
        }

        TaosGlobalConfig.setCharset(props.getProperty(TSDBDriver.PROPERTY_KEY_CHARSET));
        return new WSConnection(url, props, transport, param.getDatabase());
    }

    static class ConnectReq extends Payload {
        private String user;
        private String password;
        private String db;

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDb() {
            return db;
        }

        public void setDb(String db) {
            this.db = db;
        }
    }
}
