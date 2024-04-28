package com.taosdata.jdbc.ws;

import com.google.common.base.Strings;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.entity.*;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.extensions.permessage_deflate.PerMessageDeflateExtension;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Draft perMessageDeflateDraft = new Draft_6455(
            new PerMessageDeflateExtension());
    ThreadPoolExecutor executor;
    Transport transport;
    private Consumer<String> textMessageHandler;
    private Consumer<ByteBuffer> binaryMessageHandler;

    public final String serverUri;
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
        super(serverUri, connectionParam.isEnableCompression() ? perMessageDeflateDraft : new Draft_6455(), new HashMap<>());
        this.transport = transport;
        this.serverUri = serverUri.toString();
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
        if (remote){
            log.error("disconnect uri: {},  code : {} , reason: {}, remote: {}", serverUri, code, reason, remote);
        }else{
            log.debug("disconnect uri: {},  code : {} , reason: {}, remote: {}", serverUri, code, reason, remote);
        }
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
        return new WSClient(urlPath, transport, params, function);
    }
    public static WSClient getSlaveInstance(ConnectionParam params, WSFunction function, Transport transport) throws SQLException {
        if (StringUtils.isEmpty(params.getSlaveClusterHost()) || StringUtils.isEmpty(params.getSlaveClusterHost())){
            return null;
        }

        if (Strings.isNullOrEmpty(function.getFunction())) {
            throw new SQLException("websocket url error");
        }
        String protocol = "ws";
        if (params.isUseSsl()) {
            protocol = "wss";
        }
        String port = ":" + params.getSlaveClusterPort();

        String wsFunction = "/ws";

        if (!function.equals(WSFunction.WS)){
            throw new SQLException("slave cluster is not supported!");
        }

        String loginUrl = protocol + "://" + params.getSlaveClusterHost() + port + wsFunction;

        URI urlPath;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Slave cluster websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath, transport, params, function);
    }
}
