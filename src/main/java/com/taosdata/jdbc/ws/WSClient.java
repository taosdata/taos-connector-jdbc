package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Payload;
import com.taosdata.jdbc.ws.entity.Request;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class WSClient extends WebSocketClient implements AutoCloseable {
    private final String user;
    private final String password;
    private final String database;

    ThreadPoolExecutor executor;
    private int reqId;

    private Consumer<String> textMessageHandler;
    private Consumer<ByteBuffer> binaryMessageHandler;

    public void setTextMessageHandler(Consumer<String> textMessageHandler) {
        this.textMessageHandler = textMessageHandler;
    }
    public void setBinaryMessageHandler(Consumer<ByteBuffer> binaryMessageHandler) {
        this.binaryMessageHandler = binaryMessageHandler;
    }

    /**
     * create websocket connection client
     *
     * @param serverUri connection url
     * @param user      database user
     * @param password  database password
     * @param database  connection database
     */
    public WSClient(URI serverUri, String user, String password, String database) {
        super(serverUri, new HashMap<>());
        this.user = user;
        this.password = password;
        this.database = database;
        executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(),
                r -> {
                    Thread t = new Thread(r);
                    t.setName("parse-message-" + t.getId());
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        // certification
        ConnectReq connectReq = new ConnectReq(++reqId, user, password, database);
        this.send(new Request(Action.CONN.getAction(), connectReq).toString());
    }

    @Override
    public void onMessage(String message) {
        if (!"".equals(message)) {
            executor.submit(() -> {
                textMessageHandler.accept(message);
            });
        }
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        binaryMessageHandler.accept(bytes);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        if (remote) {
            throw new RuntimeException("The remote server closed the connection: " + reason);
        } else {
            throw new RuntimeException("close connection: " + reason);
        }
    }

    @Override
    public void onError(Exception e) {
        this.close();
    }

    @Override
    public void close() {
        super.close();
        executor.shutdown();
    }

    static class ConnectReq extends Payload {
        private String user;
        private String password;
        private String db;

        public ConnectReq(long reqId, String user, String password, String db) {
            super(reqId);
            this.user = user;
            this.password = password;
            this.db = db;
        }

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

    public static WSClient getInstance(ConnectionParam params) throws SQLException {
        String protocol = "ws";
        if (params.isUseSsl()) {
            protocol = "wss";
        }
        String loginUrl = protocol + "://" + params.getHost() + ":" + params.getPort() + "/rest/ws";
        if (null != params.getCloudToken()) {
            loginUrl = loginUrl + "?token=" + params.getCloudToken();
        }

        URI urlPath = null;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath, params.getUser(), params.getPassword(), params.getDatabase());
    }
}
