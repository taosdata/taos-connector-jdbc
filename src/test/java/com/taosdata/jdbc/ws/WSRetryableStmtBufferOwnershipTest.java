package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ResourceLeakDetector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WSRetryableStmtBufferOwnershipTest {

    @BeforeClass
    public static void setUpClass() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDownClass() {
        System.gc();
    }

    @Test
    public void writeBlockWithRetrySync_releasesCallerBufferWhenTransportConsumesSentReference() throws Exception {
        ConnectionParam param = buildConnectionParam();
        Transport transport = buildTransport(param, true);
        WSRetryableStmt stmt = buildStmt(param, transport);
        ByteBuf rawBlock = newRawBlock();
        try {
            stmt.writeBlockWithRetrySync(rawBlock);
            assertEquals(0, rawBlock.refCnt());
        } finally {
            if (rawBlock.refCnt() > 0) {
                rawBlock.release();
            }
        }
    }

    @Test
    public void writeBlockWithRetrySync_keepsCallerReferenceWhenTransportDoesNotConsumeSentReference() throws Exception {
        ConnectionParam param = buildConnectionParam();
        Transport transport = buildTransport(param, false);
        WSRetryableStmt stmt = buildStmt(param, transport);
        ByteBuf rawBlock = newRawBlock();
        try {
            stmt.writeBlockWithRetrySync(rawBlock);
            assertEquals(1, rawBlock.refCnt());
        } finally {
            if (rawBlock.refCnt() > 0) {
                rawBlock.release();
            }
        }
    }

    private static WSRetryableStmt buildStmt(ConnectionParam param, Transport transport) {
        WSConnection connection = mock(WSConnection.class);
        when(connection.supportsStmt2BindExec()).thenReturn(true);

        StmtInfo stmtInfo = new StmtInfo("INSERT INTO test VALUES(?)");
        stmtInfo.setStmtId(12345L);

        return new WSRetryableStmt(
                connection,
                param,
                "test_db",
                transport,
                1L,
                stmtInfo,
                new AtomicInteger(0)
        );
    }

    private static Transport buildTransport(ConnectionParam param, boolean consumeSentReference) throws Exception {
        Transport transport = new Transport();
        InFlightRequest inFlightRequest = new InFlightRequest(8);

        WSClient client = mock(WSClient.class);
        doAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            long reqId = buffer.getLongLE(0);
            FutureResponse future = inFlightRequest.remove(Action.STMT2_BIND_EXEC.getAction(), reqId);
            assertNotNull("pending bind-exec future should exist", future);

            if (consumeSentReference) {
                buffer.release();
            }

            Stmt2ExecResp resp = new Stmt2ExecResp();
            resp.setCode(Code.SUCCESS.getCode());
            resp.setStmtId(12345L);
            resp.setAffected(1);
            future.getFuture().complete(resp);
            return null;
        }).when(client).send(any(ByteBuf.class));

        WSConnectionManager connectionManager = mock(WSConnectionManager.class);
        when(connectionManager.getConnectionParam()).thenReturn(param);
        when(connectionManager.getReconnectCount()).thenReturn(0);
        when(connectionManager.isConnected()).thenReturn(true);
        when(connectionManager.getCurrentClient()).thenReturn(client);
        when(connectionManager.getCurrentEndpoint()).thenReturn(mock(Endpoint.class));

        injectField(transport, "connectionManager", connectionManager);
        injectField(transport, "inFlightRequest", inFlightRequest);
        return transport;
    }

    private static ConnectionParam buildConnectionParam() {
        ConnectionParam param = mock(ConnectionParam.class);
        when(param.isEnableAutoConnect()).thenReturn(false);
        when(param.getRetryTimes()).thenReturn(1);
        when(param.getRequestTimeout()).thenReturn(5000);
        when(param.getZoneId()).thenReturn(ZoneId.systemDefault());
        return param;
    }

    private static ByteBuf newRawBlock() {
        ByteBuf rawBlock = Unpooled.buffer(32);
        rawBlock.writeLongLE(0L);
        rawBlock.writeLongLE(12345L);
        rawBlock.writeIntLE(1);
        return rawBlock;
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = Transport.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
