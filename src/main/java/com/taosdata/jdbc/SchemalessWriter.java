package com.taosdata.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.enums.ConnectionType;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.schemaless.CommonResp;
import com.taosdata.jdbc.ws.schemaless.ConnReq;
import com.taosdata.jdbc.ws.schemaless.InsertReq;
import com.taosdata.jdbc.ws.schemaless.SchemalessAction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class SchemalessWriter {
    protected Connection connection;

    // jni
    private TSDBJNIConnector connector;
    // websocket
    private Transport transport;
    private AtomicLong insertId = new AtomicLong(0);

    // meta
    private String dbName;
    private ConnectionType type;

    public SchemalessWriter(Connection connection) {
        this.connection = connection;
    }

    public SchemalessWriter(String url) throws SQLException {
        this(url, null, null, null);
    }

    public SchemalessWriter(String url, String username, String password, String db) throws SQLException {
        String t = "jni";
        if (!url.startsWith(TSDBDriver.URL_PREFIX)) {
            t = "ws";
        }
        Properties properties = StringUtils.parseUrl(url, null);
        String name = db != null ? db : properties.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME);
        String port = properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT);
        String host = properties.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        String user = null != username ? username : properties.getProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        String pass = null != password ? password : properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        init(host, port, user, pass, name, t);
    }

    public SchemalessWriter(String host, String port, String user, String password, String dbName, String type) throws SQLException {
        init(host, port, user, password, dbName, type);
    }

    private void init(String host, String port, String user, String password, String dbName, String type) throws SQLException {
        this.dbName = dbName;
        if (null == type || ConnectionType.JNI.getType().equalsIgnoreCase(type)) {
            this.type = ConnectionType.JNI;
            int p = Integer.parseInt(port);
            this.connector = new TSDBJNIConnector();
            this.connector.connect(host, p, dbName, user, password);
        } else if (ConnectionType.WEBSOCKET.getType().equalsIgnoreCase(type) || ConnectionType.WS.getType().equalsIgnoreCase(type)) {
            this.type = ConnectionType.WS;
            int timeout = Integer.parseInt(HttpClientPoolUtil.DEFAULT_SOCKET_TIMEOUT);
            int connectTime = Integer.parseInt(HttpClientPoolUtil.DEFAULT_CONNECT_TIMEOUT);
            ConnectionParam param = new ConnectionParam.Builder(host, port)
                    .setUserAndPassword(user, password)
                    .setDatabase(dbName)
                    .setConnectionTimeout(connectTime)
                    .setRequestTimeout(timeout)
                    .build();
            InFlightRequest inFlightRequest = new InFlightRequest(1000, 1);
            this.transport = new Transport(WSFunction.SCHEMALESS, param, inFlightRequest);

            this.transport.setTextMessageHandler(message -> {
                JSONObject jsonObject = JSON.parseObject(message);
                Response response = jsonObject.toJavaObject(CommonResp.class);
                FutureResponse remove = inFlightRequest.remove(response.getAction(), response.getReqId());
                if (null != remove) {
                    remove.getFuture().complete(response);
                }
            });

            Transport.checkConnection(transport, connectTime);

            ConnReq connectReq = new ConnReq();
            connectReq.setReqId(1);
            connectReq.setUser(param.getUser());
            connectReq.setPassword(param.getPassword());
            CommonResp auth = (CommonResp) transport.send(new Request(SchemalessAction.CONN.getAction(), connectReq));

            if (Code.SUCCESS.getCode() != auth.getCode()) {
                throw new SQLException("0x" + Integer.toHexString(auth.getCode()) + ":" + "auth failure: " + auth.getMessage());
            }
        }
    }

    /**
     * batch schemaless lines write to db
     *
     * @param lines         schemaless lines
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute exception
     */
    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        write(lines, protocolType, timestampType, null, 0);
    }

    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, String name, int ttl) throws SQLException {
        if (null != connection) {
            TSDBConnection tsdbConnection = (TSDBConnection) connection;
            tsdbConnection.getConnector().insertLines(lines, protocolType, timestampType);
        } else {
            switch (type) {
                case JNI:
                    connector.insertLines(lines, protocolType, timestampType);
                    break;
                case WS: {
                    for (String line : lines) {
                        InsertReq insertReq = new InsertReq();
                        insertReq.setReqId(insertId.getAndIncrement());
                        insertReq.setProtocol(protocolType.ordinal());
                        insertReq.setPrecision(timestampType.getType());
                        if (name == null) {
                            insertReq.setDb(dbName);
                        } else {
                            insertReq.setDb(name);
                        }
                        insertReq.setTtl(ttl);
                        insertReq.setData(line);
                        CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq));
                        if (Code.SUCCESS.getCode() != response.getCode()) {
                            throw new SQLException("0x" + Integer.toHexString(response.getCode()) + ":" + response.getMessage());
                        }
                    }
                    break;
                }
                default:
                    // nothing
            }
        }
    }

    /**
     * only one line writes to db
     *
     * @param line          schemaless line
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute exception
     */
    public void write(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        write(new String[]{line}, protocolType, timestampType);
    }

    // only valid in websocket connection
    public void write(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, String dbName, int ttl) throws SQLException {
        write(new String[]{line}, protocolType, timestampType, dbName, ttl);
    }

    /**
     * batch schemaless lines write to db with list
     *
     * @param lines         schemaless list
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute exception
     */
    public void write(List<String> lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        String[] strings = lines.toArray(new String[0]);
        write(strings, protocolType, timestampType);
    }
}
