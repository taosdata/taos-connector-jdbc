package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * send message
 */
public class Transport implements AutoCloseable {

    public static final int DEFAULT_MESSAGE_WAIT_TIMEOUT = 3_000;

    private final WSClient client;
    private final InFlightRequest inFlightRequest;
    private final int timeout;

    public Transport(WSClient client, InFlightRequest inFlightRequest, int timeout) {
        this.client = client;
        this.inFlightRequest = inFlightRequest;
        this.timeout = timeout;
    }

    public Response send(Request request) throws SQLException {

        Response response = null;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try {
            inFlightRequest.put(new FutureResponse(request.getAction(), request.id(), completableFuture));
            client.send(request.toString());
        } catch (InterruptedException | TimeoutException e) {
            throw new SQLException(e);
        }
        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(completableFuture, timeout, TimeUnit.MILLISECONDS);
        try {
            response = responseFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            inFlightRequest.remove(request.getAction(), request.id());
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFul_Client_IOException, e.getMessage());
        }
        return response;
    }

    public void sendWithoutRep(Request request) {
        client.send(request.toString());
    }

    public boolean isClosed() throws SQLException {
        return client.isClosed();
    }

    @Override
    public void close() {
        inFlightRequest.close();
        client.close();
    }

}
