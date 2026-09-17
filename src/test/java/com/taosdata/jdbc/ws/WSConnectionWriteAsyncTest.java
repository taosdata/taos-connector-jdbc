package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.ws.entity.CommonResp;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WSConnectionWriteAsyncTest {

    @Test
    public void serverErrorCompletesWithSqlExceptionNotCompletionException() throws Exception {
        CommonResp resp = new CommonResp();
        resp.setCode(1);
        resp.setMessage("fail");
        Transport transport = new ReplyTransport(CompletableFuture.completedFuture(resp));
        WSConnection connection = newConnection(transport);

        AtomicReference<Throwable> observed = new AtomicReference<>();
        try {
            connection.writeAsync("st,t1=1i64 c1=1i64 1", SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS)
                    .whenComplete((response, error) -> observed.set(error))
                    .get(2, TimeUnit.SECONDS);
            fail("expected server error");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof SQLException);
            assertFalse(e.getCause() instanceof CompletionException);
            assertTrue(e.getCause().getMessage().contains("0x1"));
            assertTrue(observed.get() instanceof SQLException);
            assertFalse(observed.get() instanceof CompletionException);
        }
    }

    @Test
    public void transportSqlExceptionIsNotRewrappedAsCompletionException() throws Exception {
        CompletableFuture<Response> failed = new CompletableFuture<>();
        failed.completeExceptionally(new SQLException("closed"));
        Transport transport = new ReplyTransport(failed);
        WSConnection connection = newConnection(transport);

        AtomicReference<Throwable> observed = new AtomicReference<>();
        try {
            connection.writeAsync("st,t1=1i64 c1=1i64 1", SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS)
                    .whenComplete((response, error) -> observed.set(error))
                    .get(2, TimeUnit.SECONDS);
            fail("expected transport error");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof SQLException);
            assertFalse(e.getCause() instanceof CompletionException);
            assertTrue(observed.get() instanceof SQLException);
            assertFalse(observed.get() instanceof CompletionException);
        }
    }

    private static WSConnection newConnection(Transport transport) {
        ConnectionParam param = new ConnectionParam.Builder(
                Collections.singletonList(new Endpoint("127.0.0.1", 6041, false)))
                .setDatabase("db")
                .setRequestTimeout(5000)
                .setStmtCacheSize(0)
                .build();
        return new WSConnection("jdbc:TAOS-WS://127.0.0.1:6041/", new Properties(), transport, param, "3.0.0");
    }

    private static final class ReplyTransport extends Transport {
        private final CompletableFuture<Response> reply;

        ReplyTransport(CompletableFuture<Response> reply) {
            this.reply = reply;
        }

        @Override
        public CompletableFuture<Response> sendAsync(Request request, long timeout) {
            return reply;
        }
    }
}
