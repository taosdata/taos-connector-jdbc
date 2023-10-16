package com.taosdata.jdbc.ws;

import com.google.common.base.Strings;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.rs.RestfulDriver;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.entity.*;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class WSClient extends WebSocketClient implements AutoCloseable {

    ThreadPoolExecutor executor;
    Transport transport;

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
     */
    public WSClient(URI serverUri, Transport transport) {
        super(serverUri, new HashMap<>());
        this.transport = transport;
        executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
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
    }

    @Override
    public void onMessage(String message) {
        if (!"".equals(message)) {
            System.out.println("*******1            " + message);
            executor.submit(() -> textMessageHandler.accept(message));
        }
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        executor.submit(() -> binaryMessageHandler.accept(bytes));
    }

    @Override
    @SuppressWarnings("all")
    public void onClose(int code, String reason, boolean remote) {
//        if (remote) {
//            transport.close();
//            throw new RuntimeException("The remote server closed the connection: " + reason);
//        } else {
//            throw new RuntimeException("close connection: " + reason);
//        }
    }

    @Override
    public void onError(Exception e) {
        this.close();
    }

    @Override
    public void close() {
        super.close();
//        if (executor != null && !executor.isShutdown())
//            executor.shutdown();
    }

    @Override
    public boolean reconnectBlocking() throws InterruptedException {
        if (super.reconnectBlocking()){
            // send con msg
            ConnectReq connectReq = new ConnectReq();
            connectReq.setReqId(ReqId.getReqID());
            connectReq.setUser("root");
            connectReq.setPassword("taosdata");
            connectReq.setDb("test");
            ConnectResp auth = null;
            try {
                auth = (ConnectResp) transport.send(new Request(Action.CONN.getAction(), connectReq));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            if (Code.SUCCESS.getCode() != auth.getCode()) {
                transport.close();
                return false;
            }
            return true;
        }
        return false;
    }

    public static WSClient getInstance(ConnectionParam params, WSFunction function, Transport transport) throws SQLException {
        if (Strings.isNullOrEmpty(function.getFunction())) {
            throw new SQLException("websocket url error");
        }
        String protocol = "ws";
        if (params.isUseSsl()) {
            protocol = "wss";
        }
        String port = "";
        if (null != params.getPort()) {
            port = ":" + params.getPort();
        }

        String wsFunction = "/ws";
        if (function.equals(WSFunction.TMQ)){
            wsFunction = "/rest/tmq";
        }
        String loginUrl = protocol + "://" + params.getHost() + port + wsFunction;

        if (null != params.getCloudToken()) {
            loginUrl = loginUrl + "?token=" + params.getCloudToken();
        }

        URI urlPath;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath, transport);
    }
}
