package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.SerializeBlock;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.entity.*;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.net.SocketFactory;
import javax.net.ssl.*;
import java.net.URI;
import java.security.cert.X509Certificate;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT;

/**
 * send message
 */

public class Transport implements AutoCloseable {
    private final Logger log = LoggerFactory.getLogger(Transport.class);

    public static final int DEFAULT_MESSAGE_WAIT_TIMEOUT = 60_000;

    public static final int TSDB_CODE_RPC_NETWORK_UNAVAIL = 0x0B;
    public static final int TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED = 0x20;



    private final ArrayList<WSClient> clientArr = new ArrayList<>();;
    private final InFlightRequest inFlightRequest;
    private long timeout;
    private volatile boolean  closed = false;

    private final ConnectionParam connectionParam;
    private final WSFunction wsFunction;
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private int currentNodeIndex = 0;
    public Transport(WSFunction function, ConnectionParam param, InFlightRequest inFlightRequest) throws SQLException {
        WSClient master = WSClient.getInstance(param, function, this);
        WSClient slave = WSClient.getSlaveInstance(param, function, this);

        if (param.isDisableSslCertValidation()){
            // creat a TrustManager that trusts all certificates
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                        @SuppressWarnings("unused")
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {
                            // Intentionally left blank to accept all certificates
                        }
                        @SuppressWarnings("unused")
                        public void checkServerTrusted(X509Certificate[] certs, String authType) {
                            // Intentionally left blank to accept all certificates
                        }
                    }
            };

            try{
                // create a custom SSLContext
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
                // get SSLContext SocketFactory
                final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
                master.setSocketFactory(sslSocketFactory);

                if (slave != null){
                    slave.setSocketFactory(sslSocketFactory);
                }
            } catch (Exception e){
                throw new SQLException("setSocketFactory failed ", e);
            }
        }

        this.clientArr.add(master);
        if (slave != null){
            this.clientArr.add(slave);
        }

        this.inFlightRequest = inFlightRequest;
        this.connectionParam = param;
        this.wsFunction = function;

        this.timeout = param.getRequestTimeout();
    }

    public void setTextMessageHandler(Consumer<String> textMessageHandler) {
        for (WSClient wsClient : clientArr){
            wsClient.setTextMessageHandler(textMessageHandler);
        }
    }

    public void setBinaryMessageHandler(Consumer<ByteBuffer> binaryMessageHandler) {
        for (WSClient wsClient : clientArr) {
            wsClient.setBinaryMessageHandler(binaryMessageHandler);
        }
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    private void reconnect() throws SQLException {
        for (int i = 0; i < clientArr.size() && this.connectionParam.isEnableAutoConnect(); i++){
            boolean reconnected = reconnectCurNode();
            if (reconnected){
                log.debug("reconnect success to {}", StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri));
                return;
            }

            log.debug("reconnect failed to {}", StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri));

            currentNodeIndex =  (currentNodeIndex + 1) % clientArr.size();
        }

        close();
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
    }

    private void tmqRethrowConnectionCloseException() throws SQLException {
        // TMQ reconnect will be handled in poll
        if (WSFunction.TMQ.equals(this.wsFunction)){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
        }
    }
    @SuppressWarnings("all")
    public Response send(Request request) throws SQLException {
        if (isClosed()){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
        }

        Response response = null;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        String reqString = request.toString();

        try {
            inFlightRequest.put(new FutureResponse(request.getAction(), request.id(), completableFuture));
        } catch (InterruptedException | TimeoutException e) {
            throw new SQLException(e);
        }

        try {
            clientArr.get(currentNodeIndex).send(reqString);
        } catch (WebsocketNotConnectedException e) {
            tmqRethrowConnectionCloseException();
            reconnect();
            try {
                clientArr.get(currentNodeIndex).send(reqString);
            }catch (Exception ex){
                inFlightRequest.remove(request.getAction(), request.id());
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
            }
        }

        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(
                completableFuture, timeout, TimeUnit.MILLISECONDS, reqString);
        try {
            response = responseFuture.get();
            handleErrInMasterSlaveMode(response);
        } catch (InterruptedException | ExecutionException e) {
            inFlightRequest.remove(request.getAction(), request.id());
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        }
        return response;
    }
    public Response send(String action, long reqId, long resultId, long type, byte[] rawData) throws SQLException {
        return send(action, reqId, resultId, type, rawData, EMPTY_BYTE_ARRAY);
    }

    public Response send(String action, long reqId, long resultId, long type, byte[] rawData, byte[] rawData2) throws SQLException {
        if (isClosed()){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            buffer.write(SerializeBlock.longToBytes(reqId));
            buffer.write(SerializeBlock.longToBytes(resultId));
            buffer.write(SerializeBlock.longToBytes(type));
            buffer.write(rawData);
            buffer.write(rawData2);
        } catch (IOException e) {
            throw new SQLException("data serialize error!", e);
        }

        Response response;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try {
            inFlightRequest.put(new FutureResponse(action, reqId, completableFuture));
        } catch (InterruptedException | TimeoutException e) {
            throw new SQLException(e);
        }

        try {
            clientArr.get(currentNodeIndex).send(buffer.toByteArray());
        } catch (WebsocketNotConnectedException e) {
            tmqRethrowConnectionCloseException();
            reconnect();
            try {
                clientArr.get(currentNodeIndex).send(buffer.toByteArray());
            }catch (Exception ex){
                inFlightRequest.remove(action, reqId);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
            }
        }

        String reqString = "action:" + action + ", reqId:" + reqId + ", resultId:" + resultId + ", actionType" + type;
        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(completableFuture, timeout, TimeUnit.MILLISECONDS, reqString);
        try {
            response = responseFuture.get();
            handleErrInMasterSlaveMode(response);
        } catch (InterruptedException | ExecutionException e) {
            inFlightRequest.remove(action, reqId);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        }
        return response;
    }
    private void handleErrInMasterSlaveMode(Response response) throws InterruptedException{
        if (clientArr.size() > 1 && response instanceof CommonResp){
            CommonResp commonResp = (CommonResp) response;
            if (TSDB_CODE_RPC_NETWORK_UNAVAIL == commonResp.getCode() || TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED == commonResp.getCode()) {
                clientArr.get(currentNodeIndex).closeBlocking();
            }
        }
    }

    public Response sendWithoutRetry(Request request) throws SQLException {
        if (isClosed()){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
        }

        Response response;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        String reqString = request.toString();

        try {
            inFlightRequest.put(new FutureResponse(request.getAction(), request.id(), completableFuture));
        } catch (InterruptedException | TimeoutException e) {
            throw new SQLException(e);
        }

        try {
            clientArr.get(currentNodeIndex).send(reqString);
        } catch (Exception e) {
            inFlightRequest.remove(request.getAction(), request.id());
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
        }

        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(
                completableFuture, timeout, TimeUnit.MILLISECONDS, reqString);
        try {
            response = responseFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            inFlightRequest.remove(request.getAction(), request.id());
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        }
        return response;
    }

    public void sendWithoutResponse(Request request) throws SQLException  {
        if (isClosed()){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
        }

        try {
            clientArr.get(currentNodeIndex).send(request.toString());
        } catch (WebsocketNotConnectedException e) {
            tmqRethrowConnectionCloseException();
            reconnect();
            try {
                clientArr.get(currentNodeIndex).send(request.toString());
            }catch (Exception ex){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
            }
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isConnectionLost() {
        return clientArr.get(currentNodeIndex).isClosed();
    }

    public void disconnectAndReconnect() throws SQLException {
        try {
            clientArr.get(currentNodeIndex).closeBlocking();
            if (!clientArr.get(currentNodeIndex).reconnectBlockingWithoutRetry()){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, "websocket reconnect failed!");
            }
        } catch (Exception e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
        }
    }
    @Override
    public synchronized void close() {
        if (isClosed()){
            return;
        }
        closed = true;
        inFlightRequest.close();
        for (WSClient wsClient : clientArr){
            wsClient.shutdown();
        }
    }

    public void checkConnection(int connectTimeout) throws SQLException {
        try {
            if (WSConnection.g_FirstConnection && clientArr.size() > 1) {
                // 测试所有节点，如果连接失败，直接异常
                for (WSClient wsClient : clientArr){
                    if (!wsClient.connectBlocking(connectTimeout, TimeUnit.MILLISECONDS)) {
                        close();
                        throw TSDBError.createSQLException(ERROR_CONNECTION_TIMEOUT,
                                "can't create connection with server " + wsClient.serverUri.toString() + " within: " + connectTimeout + " milliseconds");
                    }
                    log.debug("connect success to {}", StringUtils.getBasicUrl(wsClient.serverUri));
                }

                // 断开其他节点
                for (int i = 0; i < clientArr.size(); i++){
                    if (i != currentNodeIndex) {
                        clientArr.get(i).closeBlocking();
                        log.debug("disconnect success to {}", StringUtils.getBasicUrl(clientArr.get(i).serverUri));
                    }
                }
            } else {
                if (!clientArr.get(currentNodeIndex).connectBlocking(connectTimeout, TimeUnit.MILLISECONDS)) {
                    close();
                    throw TSDBError.createSQLException(ERROR_CONNECTION_TIMEOUT,
                            "can't create connection with server within: " + connectTimeout + " milliseconds");
                }
                log.debug("connect success to {}", StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            close();
            throw new SQLException("create websocket connection has been Interrupted ", e);
        }
    }

    public void shutdown() {
        closed = true;
        if (inFlightRequest.hasInFlightRequest()) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(timeout);
                } catch (InterruptedException e) {
                    // ignore
                }
            });
            future.thenRun(this::close);
        } else {
            close();
        }
    }

    public boolean doReconnectCurNode() throws SQLException {
        boolean reconnected = false;
        for (int retryTimes = 0; retryTimes < connectionParam.getReconnectRetryCount(); retryTimes++) {
            try {
                reconnected = clientArr.get(currentNodeIndex).reconnectBlocking();
                if (reconnected) {
                    break;
                }
                Thread.sleep(connectionParam.getReconnectIntervalMs());
            } catch (Exception e) {
                log.error("try connect remote server failed!", e);
            }
        }
        return reconnected;
    }

    public boolean reconnectCurNode() throws SQLException {
        for (int retryTimes = 0; retryTimes < connectionParam.getReconnectRetryCount(); retryTimes++) {
            try {
                boolean reconnected = clientArr.get(currentNodeIndex).reconnectBlocking();
                if (reconnected) {
                    // send con msgs
                    ConnectReq connectReq = new ConnectReq();
                    connectReq.setReqId(ReqId.getReqID());
                    connectReq.setUser(connectionParam.getUser());
                    connectReq.setPassword(connectionParam.getPassword());
                    connectReq.setDb(connectionParam.getDatabase());

                    if (connectionParam.getConnectMode() != 0) {
                        connectReq.setMode(connectionParam.getConnectMode());
                    }

                    ConnectResp auth;
                    auth = (ConnectResp) sendWithoutRetry(new Request(Action.CONN.getAction(), connectReq));

                    if (Code.SUCCESS.getCode() == auth.getCode()) {
                        return true;
                    } else {
                        clientArr.get(currentNodeIndex).closeBlocking();
                        log.error("reconnect failed, code: {}, msg: {}", auth.getCode(), auth.getMessage());
                    }
                }
                Thread.sleep(connectionParam.getReconnectIntervalMs());
            } catch (Exception e) {
                log.error("try connect remote server failed!", e);
            }
        }
        return false;
    }
}
