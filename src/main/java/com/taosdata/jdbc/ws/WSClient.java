package com.taosdata.jdbc.ws;

import com.google.common.base.Strings;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.rs.RestfulDriver;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.entity.*;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

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

    private final Logger log = LoggerFactory.getLogger(WSClient.class);


    ThreadPoolExecutor executor;
    Transport transport;

    ConnectionParam connectionParam;

    private Consumer<String> textMessageHandler;
    private Consumer<ByteBuffer> binaryMessageHandler;
    private final WSFunction wsFunction;

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
    public WSClient(URI serverUri, Transport transport, ConnectionParam connectionParam, WSFunction function) {
        super(serverUri, new HashMap<>());
        this.transport = transport;
        this.connectionParam = connectionParam;
        this.wsFunction = function;
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
        // do nothing, wait next send to retry.
        log.error("code : " + code + " , reason: " + reason + " remote:" + remote + " wsclient:" + this );
    }

    @Override
    public void onError(Exception e) {
        this.close();
    }

    public void shutdown() {
        super.close();
        if (executor != null && !executor.isShutdown())
            executor.shutdown();
    }

    public boolean reconnectBlockingWithoutRetry() throws InterruptedException {
        return super.reconnectBlocking();
    }

    @Override
    public boolean reconnectBlocking(){
        if (WSFunction.TMQ.equals(this.wsFunction)){
            return false;
        }

        final int MAX_CONNECT_RETRY_COUNT = 3;
        for (int retryTimes = 0; retryTimes < MAX_CONNECT_RETRY_COUNT; retryTimes++){
            try {
                if (super.reconnectBlocking()) {
                    // send con msgs
                    ConnectReq connectReq = new ConnectReq();
                    connectReq.setReqId(ReqId.getReqID());
                    connectReq.setUser(connectionParam.getUser());
                    connectReq.setPassword(connectionParam.getPassword());
                    connectReq.setDb(connectionParam.getDatabase());

                    if (connectionParam.getConnectMode() != 0){
                        connectReq.setMode(connectionParam.getConnectMode());
                    }

                    ConnectResp auth;
                    auth = (ConnectResp) transport.send(new Request(Action.CONN.getAction(), connectReq));

                    if (Code.SUCCESS.getCode() == auth.getCode()) {
                        return true;
                    }
                }
                Thread.sleep(2000);
            }catch (Exception e){
                log.error("try connect remote server failed!", e);
            }
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
        //        String loginUrl = protocol + "://" + params.getHost() + port + "/rest/" + function.getFunction();

        if (null != params.getCloudToken()) {
            loginUrl = loginUrl + "?token=" + params.getCloudToken();
        }

        URI urlPath;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath, transport, params, function);
    }
}
