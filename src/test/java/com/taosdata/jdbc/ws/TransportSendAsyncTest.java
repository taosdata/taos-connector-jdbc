package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.CommonResp;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.schemaless.InsertReq;
import com.taosdata.jdbc.ws.schemaless.SchemalessAction;
import org.junit.BeforeClass;
import org.junit.Test;
import org.objenesis.ObjenesisStd;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransportSendAsyncTest {

    @BeforeClass
    public static void initNetty() {
        Utils.initEventLoopGroup();
    }

    @Test
    public void closedConnectionCompletesWithSqlException() throws Exception {
        Transport transport = new Transport() {
        };
        Field closed = Transport.class.getDeclaredField("closed");
        closed.setAccessible(true);
        closed.set(transport, true);

        try {
            transport.sendAsync(insertRequest(1L), true, 1000).get(2, TimeUnit.SECONDS);
            fail("expected closed connection");
        } catch (ExecutionException e) {
            assertSqlException(e.getCause(), TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
    }

    @Test
    public void sendFailureCompletesWithSqlExceptionAndClearsInFlight() throws Exception {
        InFlightRequest inFlightRequest = new InFlightRequest(16);
        Transport transport = buildTransport(inFlightRequest, new RejectedExecutionException("closing"));

        try {
            transport.sendAsync(insertRequest(2L), true, 1000).get(2, TimeUnit.SECONDS);
            fail("expected send failure");
        } catch (ExecutionException e) {
            assertSqlException(e.getCause(), TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION);
        }
        assertFalse(inFlightRequest.hasInFlightRequest());
    }

    @Test
    public void timeoutCompletesWithSqlExceptionAndClearsInFlight() throws Exception {
        InFlightRequest inFlightRequest = new InFlightRequest(16);
        Transport transport = buildTransport(inFlightRequest, null);

        try {
            transport.sendAsync(insertRequest(3L), true, 50).get(2, TimeUnit.SECONDS);
            fail("expected timeout");
        } catch (ExecutionException e) {
            assertSqlException(e.getCause(), TSDBErrorNumbers.ERROR_QUERY_TIMEOUT);
        }
        assertFalse(inFlightRequest.hasInFlightRequest());
    }

    @Test
    public void closeMapsInFlightToConnectionClosedSqlException() throws Exception {
        InFlightRequest inFlightRequest = new InFlightRequest(16);
        Transport transport = buildTransport(inFlightRequest, null);

        CompletableFuture<Response> future = transport.sendAsync(insertRequest(4L), true, 5000);
        transport.close();
        try {
            future.get(2, TimeUnit.SECONDS);
            fail("expected closed in-flight");
        } catch (ExecutionException e) {
            assertSqlException(e.getCause(), TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
    }

    @Test
    public void successCompletesOnCallingThreadNotCommonPool() throws Exception {
        InFlightRequest inFlightRequest = new InFlightRequest(16);
        Transport transport = buildTransport(inFlightRequest, null);

        AtomicReference<String> threadName = new AtomicReference<>();
        CompletableFuture<Response> future = transport.sendAsync(insertRequest(5L), true, 5000)
                .whenComplete((response, error) -> threadName.set(Thread.currentThread().getName()));

        CommonResp resp = new CommonResp();
        resp.setCode(0);
        completeInsert(inFlightRequest, 5L, resp);

        future.get(2, TimeUnit.SECONDS);
        assertFalse("async success must not hop to ForkJoinPool.commonPool",
                threadName.get() != null && threadName.get().startsWith("ForkJoinPool"));
    }

    @Test
    public void blockingCleanupExecutorIsPerTransportAndShutdownOnClose() throws Exception {
        List<Endpoint> ha = Arrays.asList(
                new Endpoint("127.0.0.1", 6041, false),
                new Endpoint("127.0.0.1", 6042, false));
        InFlightRequest firstInFlight = new InFlightRequest(16);
        InFlightRequest secondInFlight = new InFlightRequest(16);
        Transport first = buildTransport(firstInFlight, null, ha);
        Transport second = buildTransport(secondInFlight, null, ha);

        CommonResp networkUnavail = new CommonResp();
        networkUnavail.setCode(Transport.TSDB_CODE_RPC_NETWORK_UNAVAIL);

        CompletableFuture<Response> firstFuture = first.sendAsync(insertRequest(11L), true, 5000);
        completeInsert(firstInFlight, 11L, networkUnavail);
        firstFuture.get(2, TimeUnit.SECONDS);

        CompletableFuture<Response> secondFuture = second.sendAsync(insertRequest(12L), true, 5000);
        completeInsert(secondInFlight, 12L, networkUnavail);
        secondFuture.get(2, TimeUnit.SECONDS);

        Field executorField = Transport.class.getDeclaredField("blockingCleanupExecutor");
        executorField.setAccessible(true);
        assertFalse(Modifier.isStatic(executorField.getModifiers()));
        ExecutorService firstExecutor = (ExecutorService) executorField.get(first);
        ExecutorService secondExecutor = (ExecutorService) executorField.get(second);
        assertTrue(firstExecutor != null);
        assertTrue(secondExecutor != null);
        assertFalse(firstExecutor == secondExecutor);

        first.close();
        assertTrue(firstExecutor.isShutdown());
        assertFalse(secondExecutor.isShutdown());
        second.close();
        assertTrue(secondExecutor.isShutdown());
    }

    @Test
    public void rejectedCleanupExecutorCompletesInsteadOfHanging() throws Exception {
        List<Endpoint> ha = Arrays.asList(
                new Endpoint("127.0.0.1", 6041, false),
                new Endpoint("127.0.0.1", 6042, false));
        InFlightRequest inFlightRequest = new InFlightRequest(16);
        Transport transport = buildTransport(inFlightRequest, null, ha);

        CommonResp networkUnavail = new CommonResp();
        networkUnavail.setCode(Transport.TSDB_CODE_RPC_NETWORK_UNAVAIL);

        CompletableFuture<Response> primed = transport.sendAsync(insertRequest(21L), true, 5000);
        completeInsert(inFlightRequest, 21L, networkUnavail);
        primed.get(2, TimeUnit.SECONDS);

        Field executorField = Transport.class.getDeclaredField("blockingCleanupExecutor");
        executorField.setAccessible(true);
        ((ExecutorService) executorField.get(transport)).shutdownNow();

        CompletableFuture<Response> rejected = transport.sendAsync(insertRequest(22L), true, 5000);
        completeInsert(inFlightRequest, 22L, networkUnavail);
        CommonResp rejectedResp = (CommonResp) rejected.get(2, TimeUnit.SECONDS);
        assertEquals(Transport.TSDB_CODE_RPC_NETWORK_UNAVAIL, rejectedResp.getCode());
    }

    @Test
    public void haCleanupClosesTheClientThatSentNotTheRebalancedClient() throws Exception {
        List<Endpoint> ha = Arrays.asList(
                new Endpoint("127.0.0.1", 6041, false),
                new Endpoint("127.0.0.1", 6042, false));
        ConnectionParam param = new ConnectionParam.Builder(ha).setRequestTimeout(5000).build();
        StubClient sent = new StubClient(param, null);
        StubClient other = new StubClient(param, null);
        InFlightRequest inFlightRequest = new InFlightRequest(16);
        Transport transport = buildTransport(inFlightRequest, param, Arrays.asList(sent, other));

        CompletableFuture<Response> future = transport.sendAsync(insertRequest(41L), true, 5000);
        Field managerField = Transport.class.getDeclaredField("connectionManager");
        managerField.setAccessible(true);
        setField(managerField.get(transport), "currentNodeIndex", 1);

        CommonResp networkUnavail = new CommonResp();
        networkUnavail.setCode(Transport.TSDB_CODE_RPC_NETWORK_UNAVAIL);
        completeInsert(inFlightRequest, 41L, networkUnavail);
        future.get(2, TimeUnit.SECONDS);

        assertEquals(1, sent.closeBlockingCalls.get());
        assertEquals(0, other.closeBlockingCalls.get());
        transport.close();
    }

    @Test
    public void disconnectedSendDoesNotBlockCaller() throws Exception {
        InFlightRequest inFlightRequest = new InFlightRequest(16);
        Transport transport = buildTransport(inFlightRequest, new WebsocketNotConnectedException());
        long start = System.nanoTime();
        CompletableFuture<Response> future = transport.sendAsync(insertRequest(31L), true, 5000);
        assertTrue(System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(200));
        try {
            future.get(2, TimeUnit.SECONDS);
            fail("expected reconnect failure");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof SQLException);
        }
        transport.close();
    }

    private static Transport buildTransport(InFlightRequest inFlightRequest, RuntimeException sendError) throws Exception {
        return buildTransport(inFlightRequest, sendError,
                Collections.singletonList(new Endpoint("127.0.0.1", 6041, false)));
    }

    private static Transport buildTransport(InFlightRequest inFlightRequest, RuntimeException sendError,
                                            List<Endpoint> endpoints) throws Exception {
        ConnectionParam param = new ConnectionParam.Builder(endpoints)
                .setRequestTimeout(5000)
                .build();
        return buildTransport(inFlightRequest, param, Collections.singletonList(new StubClient(param, sendError)));
    }

    private static Transport buildTransport(InFlightRequest inFlightRequest, ConnectionParam param,
                                            List<WSClient> clients) throws Exception {
        WSConnectionManager connectionManager = new ObjenesisStd().newInstance(WSConnectionManager.class);
        setField(connectionManager, "connectionParam", param);
        setField(connectionManager, "closed", true);
        setField(connectionManager, "clientArr", new ArrayList<>(clients));
        setField(connectionManager, "currentNodeIndex", 0);

        Transport transport = new Transport() {
        };
        setField(transport, "connectionManager", connectionManager);
        setField(transport, "inFlightRequest", inFlightRequest);
        return transport;
    }

    private static Request insertRequest(long reqId) {
        InsertReq insertReq = new InsertReq();
        insertReq.setReqId(reqId);
        insertReq.setData("st,t1=1i64 c1=1i64 1");
        return new Request(SchemalessAction.INSERT.getAction(), insertReq);
    }

    @SuppressWarnings("unchecked")
    private static void completeInsert(InFlightRequest inFlightRequest, long reqId, Response response) throws Exception {
        Field futureMap = InFlightRequest.class.getDeclaredField("futureMap");
        futureMap.setAccessible(true);
        Map<String, ConcurrentHashMap<Long, FutureResponse>> map =
                (Map<String, ConcurrentHashMap<Long, FutureResponse>>) futureMap.get(inFlightRequest);
        FutureResponse pending = map.get(SchemalessAction.INSERT.getAction()).get(reqId);
        pending.getFuture().complete(response);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Class<?> type = target.getClass();
        Field field = null;
        while (type != null && field == null) {
            try {
                field = type.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        if (field == null) {
            throw new NoSuchFieldException(fieldName);
        }
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void assertSqlException(Throwable cause, int errorCode) {
        assertTrue(cause instanceof SQLException);
        assertFalse(cause instanceof CompletionException);
        assertEquals(errorCode, ((SQLException) cause).getErrorCode());
    }

    private static final class StubClient extends WSClient {
        private final RuntimeException sendError;
        private final java.util.concurrent.atomic.AtomicInteger closeBlockingCalls = new java.util.concurrent.atomic.AtomicInteger();

        StubClient(ConnectionParam param, RuntimeException sendError) {
            super(URI.create("ws://127.0.0.1:6041"), param);
            this.sendError = sendError;
        }

        @Override
        public void send(String strData) {
            if (sendError != null) {
                throw sendError;
            }
        }

        @Override
        public void close() {
            // no-op for unit tests
        }

        @Override
        public void closeBlocking() {
            closeBlockingCalls.incrementAndGet();
        }
    }
}
