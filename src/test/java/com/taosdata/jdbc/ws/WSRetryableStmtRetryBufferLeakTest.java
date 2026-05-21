package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Regression tests for the Task 4 ByteBuf leak fix in WSRetryableStmt.
 *
 * <p>Problem: executeWithRetry() creates orgRawBlock.copy() for retry iterations
 * (i > 0) but never released those copies, leaking direct/heap memory.
 *
 * <p>Fix: the inner finally block now calls rawBlock.release() when i > 0.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Retry copies are fully released after a retrying bind-exec write.</li>
 *   <li>All retry copies (across multiple failures) are released.</li>
 *   <li>The original (caller-owned) buffer is NOT over-released.</li>
 *   <li>The legacy path (STMT2_BIND + STMT2_EXEC) also releases retry copies.</li>
 * </ul>
 */
public class WSRetryableStmtRetryBufferLeakTest {

    // -----------------------------------------------------------------------
    // FakeTransport infrastructure (reused from WSRetryableStmtRealBehaviorTest)
    // -----------------------------------------------------------------------

    /**
     * Transport that records every ByteBuf it receives, fails the first
     * {@code failCount} send calls with a retriable error, then succeeds.
     */
    static class CapturingRetryTransport extends Transport {
        private final int failCount;
        /** Identity hash codes of every rawBlock seen in send(action, …). */
        private final List<ByteBuf> capturedBuffers = new ArrayList<>();
        private int callCount = 0;

        CapturingRetryTransport(ConnectionParam param, int failCount) throws Exception {
            super();
            java.lang.reflect.Field cmField = Transport.class.getDeclaredField("connectionManager");
            cmField.setAccessible(true);
            WSConnectionManager mockManager = mock(WSConnectionManager.class);
            when(mockManager.getConnectionParam()).thenReturn(param);
            when(mockManager.getReconnectCount()).thenReturn(0);
            when(mockManager.isConnected()).thenReturn(true);
            com.taosdata.jdbc.common.Endpoint mockEndpoint =
                    mock(com.taosdata.jdbc.common.Endpoint.class);
            when(mockManager.getCurrentEndpoint()).thenReturn(mockEndpoint);
            cmField.set(this, mockManager);
            this.failCount = failCount;
        }

        @Override
        public Response send(String action, long reqId, ByteBuf buffer,
                             boolean resend, long timeout) throws SQLException {
            capturedBuffers.add(buffer);
            callCount++;
            if (callCount <= failCount) {
                // Retriable timeout – triggers shouldRetry() == true
                throw new SQLException("simulated timeout",
                        null, TSDBErrorNumbers.ERROR_QUERY_TIMEOUT);
            }
            if ("stmt2_bind_exec".equals(action)) {
                Stmt2ExecResp r = new Stmt2ExecResp();
                r.setCode(Code.SUCCESS.getCode());
                r.setAffected(1);
                return r;
            }
            if ("stmt2_bind".equals(action)) {
                Stmt2Resp r = new Stmt2Resp();
                r.setCode(Code.SUCCESS.getCode());
                r.setStmtId(12345L);
                return r;
            }
            throw new SQLException("Unexpected action: " + action);
        }

        @Override
        public Response send(Request request, boolean resend, long timeout)
                throws SQLException {
            if ("stmt2_exec".equals(request.getAction())) {
                Stmt2ExecResp r = new Stmt2ExecResp();
                r.setCode(Code.SUCCESS.getCode());
                r.setAffected(1);
                return r;
            }
            throw new SQLException("Unexpected request: " + request.getAction());
        }

        @Override
        public int getReconnectCount() { return 0; }

        @Override
        public boolean isClosed() { return false; }

        List<ByteBuf> getCapturedBuffers() { return capturedBuffers; }
    }

    // -----------------------------------------------------------------------
    // Test fixtures
    // -----------------------------------------------------------------------

    private ConnectionParam param;
    private StmtInfo stmtInfo;
    private AtomicInteger batchRows;

    @Before
    public void setUp() {
        param = mock(ConnectionParam.class);
        when(param.isEnableAutoConnect()).thenReturn(true);  // enables retry loop
        when(param.getRetryTimes()).thenReturn(5);
        when(param.getRequestTimeout()).thenReturn(5000);
        when(param.getZoneId()).thenReturn(java.time.ZoneId.systemDefault());

        stmtInfo = new StmtInfo("INSERT INTO t VALUES(?, ?)");
        stmtInfo.setStmtId(12345L);

        batchRows = new AtomicInteger(0);
    }

    /** Helper: allocate a minimal 16-byte raw block (reqId + stmtId). */
    private static ByteBuf newRawBlock() {
        ByteBuf buf = Unpooled.buffer(16);
        buf.writeLongLE(0L);   // reqId placeholder
        buf.writeLongLE(12345L); // stmtId placeholder
        return buf;
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /**
     * Core regression: bind-exec path, one retry.
     *
     * <p>i=0 → duplicate (caller owns), fails.
     * <p>i=1 → copy (method owns), succeeds; fix must release copy.
     */
    @Test
    public void testOneRetry_bindExecPath_copyIsReleased() throws Exception {
        CapturingRetryTransport transport = new CapturingRetryTransport(param, 1);
        WSConnection wsConn = mock(WSConnection.class);
        when(wsConn.supportsStmt2BindExec()).thenReturn(true);

        WSRetryableStmt stmt = new WSRetryableStmt(
                wsConn, param, "db", transport, 1L, stmtInfo, batchRows);

        ByteBuf raw = newRawBlock();
        // writeBlockWithRetrySync(ByteBuf, boolean) – Task 4 per-call bind-exec overload
        stmt.writeBlockWithRetrySync(raw, true);

        List<ByteBuf> captured = transport.getCapturedBuffers();
        assertEquals("Two send calls expected (fail + succeed)", 2, captured.size());

        // captured[0] is the duplicate of raw (i=0); shares raw's ref-count
        // captured[1] is the copy created at i=1; must have been released
        ByteBuf retryCopy = captured.get(1);
        assertEquals("Retry copy must be released (refCnt == 0)", 0, retryCopy.refCnt());

        // Original buffer is intact: caller still owns it
        assertEquals("Original buffer refCnt must be unchanged after call", 1, raw.refCnt());
        raw.release();
    }

    /**
     * Two retries before success: both copies at i=1 and i=2 must be released.
     */
    @Test
    public void testTwoRetries_bindExecPath_allCopiesReleased() throws Exception {
        CapturingRetryTransport transport = new CapturingRetryTransport(param, 2);
        WSConnection wsConn = mock(WSConnection.class);
        when(wsConn.supportsStmt2BindExec()).thenReturn(true);

        WSRetryableStmt stmt = new WSRetryableStmt(
                wsConn, param, "db", transport, 1L, stmtInfo, batchRows);

        ByteBuf raw = newRawBlock();
        stmt.writeBlockWithRetrySync(raw, true);

        List<ByteBuf> captured = transport.getCapturedBuffers();
        assertEquals("Three send calls expected", 3, captured.size());

        // i=1 copy
        assertEquals("First retry copy refCnt == 0", 0, captured.get(1).refCnt());
        // i=2 copy
        assertEquals("Second retry copy refCnt == 0", 0, captured.get(2).refCnt());

        assertEquals("Original buffer refCnt must be unchanged", 1, raw.refCnt());
        raw.release();
    }

    /**
     * Legacy path (STMT2_BIND + STMT2_EXEC), one retry.
     * Verifies the fix applies to both bind-exec and legacy branches.
     */
    @Test
    public void testOneRetry_legacyPath_copyIsReleased() throws Exception {
        CapturingRetryTransport transport = new CapturingRetryTransport(param, 1);
        AbstractConnection conn = mock(AbstractConnection.class); // non-WSConnection → legacy

        WSRetryableStmt stmt = new WSRetryableStmt(
                conn, param, "db", transport, 1L, stmtInfo, batchRows);

        ByteBuf raw = newRawBlock();
        stmt.writeBlockWithRetrySync(raw); // uses constructor-wide useBindExec=false

        List<ByteBuf> captured = transport.getCapturedBuffers();
        assertEquals("Two send calls expected (fail + succeed)", 2, captured.size());

        ByteBuf retryCopy = captured.get(1);
        assertEquals("Retry copy must be released (refCnt == 0)", 0, retryCopy.refCnt());

        assertEquals("Original buffer refCnt must be unchanged", 1, raw.refCnt());
        raw.release();
    }

    /**
     * No retry (all attempts fail): verifies copies are released even when
     * writeBlockWithRetrySync eventually throws.
     */
    @Test
    public void testAllAttemptsFail_allCopiesReleased() throws Exception {
        // Fail all 5 attempts; retryTimes=5 means i=0..4
        CapturingRetryTransport transport = new CapturingRetryTransport(param, 99);
        WSConnection wsConn = mock(WSConnection.class);
        when(wsConn.supportsStmt2BindExec()).thenReturn(true);

        WSRetryableStmt stmt = new WSRetryableStmt(
                wsConn, param, "db", transport, 1L, stmtInfo, batchRows);

        ByteBuf raw = newRawBlock();
        try {
            stmt.writeBlockWithRetrySync(raw, true);
            fail("Expected SQLException from exhausted retries");
        } catch (SQLException expected) {
            // expected
        }

        List<ByteBuf> captured = transport.getCapturedBuffers();
        assertEquals("5 send calls expected (all fail)", 5, captured.size());

        // i=0 is duplicate (caller-owned) – we must NOT assert refCnt==0 for it
        for (int i = 1; i < captured.size(); i++) {
            assertEquals("Copy at iteration " + i + " must be released",
                    0, captured.get(i).refCnt());
        }

        assertEquals("Original buffer refCnt must be unchanged", 1, raw.refCnt());
        raw.release();
    }

    /**
     * No-retry baseline: single successful attempt (i=0, duplicate path).
     * The duplicate is caller-managed; method must not release it.
     * refCnt of raw must be 1 after the call.
     */
    @Test
    public void testNoRetry_singleSuccess_originalNotOverReleased() throws Exception {
        when(param.isEnableAutoConnect()).thenReturn(false); // single attempt
        CapturingRetryTransport transport = new CapturingRetryTransport(param, 0);
        WSConnection wsConn = mock(WSConnection.class);
        when(wsConn.supportsStmt2BindExec()).thenReturn(true);

        WSRetryableStmt stmt = new WSRetryableStmt(
                wsConn, param, "db", transport, 1L, stmtInfo, batchRows);

        ByteBuf raw = newRawBlock();
        stmt.writeBlockWithRetrySync(raw, true);

        assertEquals("Single send call expected", 1, transport.getCapturedBuffers().size());
        assertEquals("Original buffer refCnt must be 1 (not over-released)", 1, raw.refCnt());
        raw.release();
    }

    @Test
    public void testNoRetry_timeoutIsPropagated() throws Exception {
        when(param.isEnableAutoConnect()).thenReturn(false); // single attempt
        CapturingRetryTransport transport = new CapturingRetryTransport(param, 1);
        WSConnection wsConn = mock(WSConnection.class);
        when(wsConn.supportsStmt2BindExec()).thenReturn(true);

        WSRetryableStmt stmt = new WSRetryableStmt(
                wsConn, param, "db", transport, 1L, stmtInfo, batchRows);

        ByteBuf raw = newRawBlock();
        try {
            stmt.writeBlockWithRetrySync(raw, true);
            fail("Expected timeout to be propagated when retries are disabled");
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getErrorCode());
        }

        assertEquals("Single send call expected", 1, transport.getCapturedBuffers().size());
        assertEquals("Original buffer refCnt must be 1 (not over-released)", 1, raw.refCnt());
        raw.release();
    }
}
