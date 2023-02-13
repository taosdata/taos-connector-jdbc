package com.taosdata.jdbc.ws;

import com.google.common.base.Strings;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
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

    ThreadPoolExecutor executor;

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
    public WSClient(URI serverUri) {
        super(serverUri, new HashMap<>());
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
    }

    @Override
    public void onMessage(String message) {
        if (!"".equals(message)) {
            executor.submit(() -> textMessageHandler.accept(message));
        }
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        binaryMessageHandler.accept(bytes);
    }

    @Override
    @SuppressWarnings("all")
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

    public static WSClient getInstance(ConnectionParam params, WSFunction function) throws SQLException {
        if (Strings.isNullOrEmpty(function.getFunction())) {
            throw new SQLException("websocket url error");
        }
        String protocol = "ws";
        if (params.isUseSsl()) {
            protocol = "wss";
        }
        String loginUrl = protocol + "://" + params.getHost() + ":" + params.getPort() + "/rest/" + function.getFunction();
        if (null != params.getCloudToken()) {
            loginUrl = loginUrl + "?token=" + params.getCloudToken();
        }

        URI urlPath;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath);
    }
}
