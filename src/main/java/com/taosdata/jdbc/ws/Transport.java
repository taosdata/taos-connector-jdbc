package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT;

/**
 * send message
 */
public class Transport implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Transport.class);
    private static final boolean isTest = "test".equalsIgnoreCase(System.getProperty("ENV_TAOS_JDBC_TEST"));

    public static final int DEFAULT_MESSAGE_WAIT_TIMEOUT = 60_000;

    public static final int TSDB_CODE_RPC_NETWORK_UNAVAIL = 0x0B;
    public static final int TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED = 0x20;

    private final AtomicInteger reconnectCount = new AtomicInteger(0);

    private final ArrayList<WSClient> clientArr = new ArrayList<>();
    private final InFlightRequest inFlightRequest;
    private long timeout;
    private volatile boolean  closed = false;

    private final ConnectionParam connectionParam;
    private final WSFunction wsFunction;
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private int currentNodeIndex;

    protected Transport() {
        this.inFlightRequest = null;
        this.connectionParam = null;
        this.wsFunction = null;
    }
    public Transport(WSFunction function,
                     ConnectionParam param,
                     InFlightRequest inFlightRequest) throws SQLException {
        // master slave mode
        WSClient slave = WSClient.getSlaveInstance(param, function, this);
        if (slave != null){
            WSClient master = WSClient.getInstance(param, 0, function, this);
            this.clientArr.add(master);
            this.clientArr.add(slave);
            currentNodeIndex = 0;
        } else {
            for (int i = 0; i < param.getEndpoints().size(); i++){
                WSClient client = WSClient.getInstance(param, i, function, this);
                this.clientArr.add(client);
            }
            currentNodeIndex = getRandomClientIndex();
        }

        this.inFlightRequest = inFlightRequest;
        this.connectionParam = param;
        this.wsFunction = function;

        setTimeout(param.getRequestTimeout());
    }
    public void setTimeout(long timeout) {
        if (timeout < 0){
            timeout = DEFAULT_MESSAGE_WAIT_TIMEOUT;
        } else if (timeout == 0){
            timeout = Integer.MAX_VALUE;
        }
        this.timeout = timeout;
    }

    private void reconnect() throws SQLException {
        synchronized (this) {
            if (isConnected()){
                return;
            }

            for (int i = 0; i < clientArr.size() && this.connectionParam.isEnableAutoConnect(); i++) {
                boolean reconnected = reconnectCurNode();
                if (reconnected) {
                    reconnectCount.incrementAndGet();
                    log.debug("reconnect success to {}", StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri.toString()));
                    return;
                }

                log.debug("reconnect failed to {}", StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri.toString()));

                currentNodeIndex = (currentNodeIndex + 1) % clientArr.size();
            }

            close();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
        }
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

        int totalLength = 24 + rawData.length + rawData2.length;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);

        buffer.writeLongLE(reqId);
        buffer.writeLongLE(resultId);
        buffer.writeLongLE(type);
        buffer.writeBytes(rawData);
        buffer.writeBytes(rawData2);

        Response response;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try {
            inFlightRequest.put(new FutureResponse(action, reqId, completableFuture));
        } catch (InterruptedException | TimeoutException e) {
            throw new SQLException(e);
        }

        try {
            Utils.retainByteBuf(buffer);
            clientArr.get(currentNodeIndex).send(buffer);
        } catch (WebsocketNotConnectedException e) {
            tmqRethrowConnectionCloseException();
            reconnect();
            try {
                Utils.retainByteBuf(buffer);
                clientArr.get(currentNodeIndex).send(buffer);
            }catch (Exception ex){
                inFlightRequest.remove(action, reqId);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
            }
        } finally {
            Utils.releaseByteBuf(buffer);
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

    public void sendFetchBlockAsync(long reqId,
                                    long resultId) throws SQLException {
        final byte[] version = {1, 0};

        if (isClosed()){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
        }

        int totalLength = 26;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);

        buffer.writeLongLE(reqId);
        buffer.writeLongLE(resultId);
        buffer.writeLongLE(7); // fetch block action type
        buffer.writeBytes(version);

        try {
            Utils.retainByteBuf(buffer);
            clientArr.get(currentNodeIndex).send(buffer);
        } catch (WebsocketNotConnectedException e) {
            reconnect();
            try {
                Utils.retainByteBuf(buffer);
                clientArr.get(currentNodeIndex).send(buffer);
            }catch (Exception ex){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
            }
        } finally {
            Utils.releaseByteBuf(buffer);
        }
    }

    public Response send(String action, long reqId, ByteBuf buffer) throws SQLException {
        if (isClosed()){
            Utils.releaseByteBuf(buffer);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "Websocket Not Connected Exception");
        }

        Response response;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try {
            inFlightRequest.put(new FutureResponse(action, reqId, completableFuture));
        } catch (InterruptedException | TimeoutException e) {
            throw new SQLException(e);
        }

        try {
            Utils.retainByteBuf(buffer);
            clientArr.get(currentNodeIndex).send(buffer);
        } catch (WebsocketNotConnectedException e) {
            tmqRethrowConnectionCloseException();
            reconnect();
            try {
                Utils.retainByteBuf(buffer);
                clientArr.get(currentNodeIndex).send(buffer);
            }catch (Exception ex){
                inFlightRequest.remove(action, reqId);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
            }
        } finally {
            Utils.releaseByteBuf(buffer);
        }

        String reqString = "action:" + action + ", reqId:" + reqId;
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
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage() == null ? "" : e.getMessage());
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
            wsClient.close();
        }
    }

    public void checkConnection(int connectTimeout) throws SQLException {
        if (WSConnection.g_FirstConnection.compareAndSet(true, false) && !StringUtils.isEmpty(connectionParam.getSlaveClusterHost())) {
            // test all nodes, if connection failed, throw exception
            for (WSClient wsClient : clientArr){
                if (!wsClient.connectBlocking()) {
                    close();
                    throw TSDBError.createSQLException(ERROR_CONNECTION_TIMEOUT,
                            "can't create connection with server " + wsClient.serverUri.toString() + " within: " + connectTimeout + " milliseconds");
                }
                log.debug("connect success to {}", StringUtils.getBasicUrl(wsClient.serverUri.toString()));
            }

            // disconnect all nodes except current node
            for (int i = 0; i < clientArr.size(); i++){
                if (i != currentNodeIndex) {
                    clientArr.get(i).closeBlocking();
                    log.debug("disconnect success to {}", StringUtils.getBasicUrl(clientArr.get(i).serverUri.toString()));
                }
            }
        } else {
            // test all nodes, until one node connected success
            for (int i = 0; i < clientArr.size(); i++){
                currentNodeIndex = currentNodeIndex % clientArr.size();
                if (clientArr.get(currentNodeIndex).connectBlocking()) {
                    log.debug("connect success to {}", StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri.toString()));
                    return;
                }
                currentNodeIndex = (currentNodeIndex + 1) % clientArr.size();
            }
            close();
            throw TSDBError.createSQLException(ERROR_CONNECTION_TIMEOUT,
                    "can't create connection with any server within: " + connectTimeout + " milliseconds");
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
                    ConnectReq connectReq = new ConnectReq(connectionParam);

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

    public int getReconnectCount() {
        return reconnectCount.get();
    }

    public boolean isConnected() {
        return clientArr.get(currentNodeIndex).isOpen();
    }

    private int getRandomClientIndex(){
        if (isTest) {
            return 0;
        }
        return ThreadLocalRandom.current().nextInt(clientArr.size());
    }
    public final ConnectionParam getConnectionParam() {
        return connectionParam;
    }
}
