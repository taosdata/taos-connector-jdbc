package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.StmtUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.*;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for statements with retryable writes and statement-cache support.
 *
 * <p>Lifecycle state machine (JDBC connections are single-threaded):
 * <ul>
 *   <li><b>in use</b>: handed out by {@link WSConnection#prepareStatement(String)};
 *       {@link #isInUse()} is true.</li>
 *   <li><b>cached/idle</b>: the application called {@link #close()}, but the
 *       statement was kept in the connection's statement cache instead of being
 *       closed. {@code closed} stays false so that a later prepare of the same
 *       SQL can reopen the same object via {@link #reopenFromCache()}; therefore
 *       {@link #isClosed()} is intentionally NOT true here, and the application
 *       must not touch its reference after {@code close()} regardless.</li>
 *   <li><b>closed</b>: not cached; {@code closed=true} and the server-side
 *       stmt2 resource has been released via STMT2_CLOSE.</li>
 * </ul>
 *
 * <p>{@link #close()} is final and routes through {@link #tryCache()}: cacheable
 * statements get {@link #resetForReuse()} + {@link #markIdle()}; all others go
 * through the original close + {@link #releaseServerResource()} path.
 *
 * <p>Subclass contract: a subclass that supports caching (e.g.
 * {@link AbsWSPreparedStatement}, {@code WSColumnPreparedStatement},
 * {@code AbstractWSEWPreparedStatement}) must override {@link #resetForReuse()},
 * and the override MUST call {@code super.resetForReuse()} so that inherited
 * per-use state (bound parameters, affected rows, batch state) is cleared.
 * Skipping the super call leaks the previous borrower's bindings into the next
 * one.
 */
public class WSRetryableStmt extends WSStatement {
    private static final Logger log = LoggerFactory.getLogger(WSRetryableStmt.class);

    // Operation type constants
    private static final int OPERATION_TYPE_WRITE = 1;
    private static final int OPERATION_TYPE_QUERY = 2;

    protected final ConnectionParam param;
    protected StmtInfo stmtInfo;
    protected final AtomicReference<SQLException> lastError = new AtomicReference<>(null);
    protected final AtomicInteger batchInsertedRowsInner;
    private long reconnectCount;
    // Whether write operations on this statement may use STMT2_BIND_EXEC.
    // Query operations always stay on the legacy bind + exec flow.
    private final boolean useBindExec;

    public WSRetryableStmt(AbstractConnection connection,
                           ConnectionParam param,
                           String database,
                           Transport transport,
                           Long instanceId,
                           StmtInfo stmtInfo,
                           AtomicInteger batchInsertedRows) {
        super(transport, database, connection, instanceId, param.getZoneId());
        this.param = param;
        this.stmtInfo = stmtInfo;
        this.batchInsertedRowsInner = batchInsertedRows;
        this.reconnectCount = transport.getReconnectCount();
        this.useBindExec = connection instanceof WSConnection && ((WSConnection) connection).supportsStmt2BindExec();
    }

    public void initStmt(int retryTimes) throws SQLException {
        Stmt2PrepareResp prepareResp = StmtUtils.initStmtWithRetry(transport, stmtInfo.getSql(), retryTimes);
        stmtInfo.setStmtId(prepareResp.getStmtId());
    }

    private void modifyStmtIdAndReqId(ByteBuf rawBlock, long stmtId, long reqId) {
        int originalWriterIndex = rawBlock.writerIndex();
        try {
            rawBlock.writerIndex(0);
            rawBlock.writeLongLE(reqId);
            rawBlock.writeLongLE(stmtId);
        } finally {
            rawBlock.writerIndex(originalWriterIndex);
        }
    }

    public void writeBlockWithRetry(ByteBuf rawBlock) throws SQLException {
        Utils.retainByteBuf(rawBlock);
        try {
            executeWithRetry(rawBlock, OPERATION_TYPE_WRITE, param.isEnableAutoConnect());
        } finally {
            Utils.releaseByteBuf(rawBlock);
        }
    }

    /**
     * Sends one stmt2 write request through the retry/reconnect loop.
     *
     * <p>The caller is responsible for allocating {@code rawBlock} with the header
     * layout expected by {@link #modifyStmtIdAndReqId}: reqId (8 bytes LE) at offset 0
     * followed by stmtId (8 bytes LE) at offset 8, then the payload.
     *
     * <p>Protocol selection is internal to {@link #executeWithRetry(ByteBuf, int, boolean)}:
     * write operations use {@code STMT2_BIND_EXEC} when the owning {@link WSConnection}
     * reports bind-exec capability, while queries always stay on the legacy
     * {@code STMT2_BIND} + {@code STMT2_EXEC} flow.
     *
     * @param rawBlock the raw binary payload to send
     */
    protected void writeBlockWithRetrySync(ByteBuf rawBlock) throws SQLException {
        Utils.retainByteBuf(rawBlock);
        try {
            executeWithRetry(rawBlock, OPERATION_TYPE_WRITE, param.isEnableAutoConnect());
            if (lastError.get() != null) {
                SQLException e = lastError.get();
                lastError.set(null);
                throw e;
            }
        } finally {
            Utils.releaseByteBuf(rawBlock);
        }
    }

    public ResultResp queryWithRetry(ByteBuf rawBlock) throws SQLException {
        Utils.retainByteBuf(rawBlock);
        try {
            ResultResp resultResp = (ResultResp) executeWithRetry(rawBlock, OPERATION_TYPE_QUERY, param.isEnableAutoConnect());
            if (lastError.get() != null) {
                SQLException e = lastError.get();
                lastError.set(null);
                throw e;
            }
            return resultResp;
        } finally {
            Utils.releaseByteBuf(rawBlock);
        }
    }

    private Object executeWithRetry(ByteBuf orgRawBlock, int operationType, boolean isRetry) throws SQLException {
        ByteBuf rawBlock = orgRawBlock.duplicate();
        int originalReaderIndex = orgRawBlock.readerIndex();
        int originalWriterIndex = orgRawBlock.writerIndex();

        int retryCount = 1;
        if (isRetry) {
            retryCount = param.getRetryTimes();
        }
        for (int i = 0; i < retryCount; i++) {
            long reqId = ReqId.getReqID();
            try {
                if (reconnectCount != transport.getReconnectCount() && param.isEnableAutoConnect()) {
                    initStmt(1);
                    reconnectCount = transport.getReconnectCount();
                }

                if (i > 0) {
                    rawBlock = orgRawBlock.copy();
                    rawBlock.readerIndex(originalReaderIndex);
                    rawBlock.writerIndex(originalWriterIndex);
                }

                modifyStmtIdAndReqId(rawBlock, stmtInfo.getStmtId(), reqId);
                Stmt2ExecResp resp;

                if (useBindExec && operationType == OPERATION_TYPE_WRITE) {
                    // New path: STMT2_BIND_EXEC combines bind and exec
                    resp = (Stmt2ExecResp) transport.send(Action.STMT2_BIND_EXEC.getAction(),
                            reqId, rawBlock, false, this.getQueryTimeoutInMs());
                    if (Code.SUCCESS.getCode() != resp.getCode()) {
                        throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
                    }
                } else {
                    // Legacy path: STMT2_BIND + STMT2_EXEC
                    // Execute bind operation
                    Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.STMT2_BIND.getAction(),
                            reqId, rawBlock, false, this.getQueryTimeoutInMs());
                    if (Code.SUCCESS.getCode() != bindResp.getCode()) {
                        throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
                    }

                    // Execute operation
                    reqId = ReqId.getReqID();
                    Request request = RequestFactory.generateExec(stmtInfo.getStmtId(), reqId);
                    resp = (Stmt2ExecResp) transport.send(request, false, this.getQueryTimeoutInMs());
                    if (Code.SUCCESS.getCode() != resp.getCode()) {
                        throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
                    }
                }

                // Process result based on operation type
                if (operationType == OPERATION_TYPE_WRITE) {
                    int affectedRows = resp.getAffected();
                    batchInsertedRowsInner.addAndGet(affectedRows);
                    return affectedRows;
                } else if (operationType == OPERATION_TYPE_QUERY) {
                    // Get query result
                    reqId = ReqId.getReqID();
                    Request request = RequestFactory.generateUseResult(stmtInfo.getStmtId(), reqId);
                    ResultResp useResultResp = (ResultResp) transport.send(request, false, this.getQueryTimeoutInMs());
                    if (Code.SUCCESS.getCode() != useResultResp.getCode()) {
                        throw new SQLException("(0x" + Integer.toHexString(useResultResp.getCode()) + "):" + useResultResp.getMessage());
                    }
                    return useResultResp;
                } else {
                    throw new IllegalArgumentException("Unknown operation type: " + operationType);
                }
            } catch (SQLException e) {
                // Handle exception based on operation type
                boolean shouldContinue = handleException(e, i, retryCount, reconnectCount, operationType);
                if (!shouldContinue) {
                    lastError.set(e);
                    break;
                }

                // Check if connection is reestablished, if so need to reinitialize stmt object
                if (reconnectCount != transport.getReconnectCount()) {
                    log.error("connection reestablished, need to init stmt obj");
                    initStmt(1);
                    reconnectCount = transport.getReconnectCount();
                }
            } finally {
                log.trace("buffer {}, refCnt: {}", Integer.toHexString(System.identityHashCode(rawBlock)), rawBlock.refCnt());
            }
        }

        return null;
    }

    private boolean handleException(SQLException e, int retryIndex, int maxAttempts, long reconnectCount, int operationType) {
        String operationName = (operationType == OPERATION_TYPE_WRITE) ? "writeBlockWithRetry" : "queryWithRetry";

        if (retryIndex >= maxAttempts - 1) {
            lastError.set(e);
            return false; // Exception will be thrown externally
        }

        log.error("Error in {}, stmt id: {}, retry times: {}, code: {}, msg: {}",
                operationName, stmtInfo.getStmtId(), retryIndex, e.getErrorCode(), e.getMessage());

        // Check if retry is needed
        return shouldRetry(e, reconnectCount);
    }

    private boolean shouldRetry(SQLException e, long reconnectCount) {
        // Check if connection is reestablished
        int realReconnectCount = transport.getReconnectCount();
        if (reconnectCount != realReconnectCount) {
            return true;
        }

        // Timeout error, retry immediately without waiting
        if (e.getErrorCode() == TSDBErrorNumbers.ERROR_QUERY_TIMEOUT) {
            return true;
        }

        // Network issue, retry after waiting
        if (e.getErrorCode() == TSDBErrorNumbers.ERROR_CONNECTION_CLOSED ||
                e.getErrorCode() == TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION) {
            return true;
        }

        return false;
    }

    public void releaseStmt() throws SQLException {
        try {
            if (stmtInfo.getStmtId() != 0 && transport.isConnected()) {
                long reqId = ReqId.getReqID();
                Request close = RequestFactory.generateClose(stmtInfo.getStmtId(), reqId);
                transport.send(close, this.getQueryTimeoutInMs());
            }
        } finally {
            // All call sites are terminal (EW worker teardown, EW constructor
            // error path). Zeroing guarantees STMT2_CLOSE is sent at most
            // once even when several release paths run.
            stmtInfo.setStmtId(0);
        }
    }

    /**
     * Single close template for all retryable statements; final so subclasses
     * cannot bypass the statement cache decision.
     */
    @Override
    public final void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        awaitPendingWrites();

        if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close();
            resultSet = null;
        }

        if (tryCache()) {
            // Cached: statement stays alive and registered with the connection.
            return;
        }

        // Not cached: original close behavior, then full resource release
        // (subclass cleanup lives in the doReleaseServerResource() hook).
        try {
            super.close();   // WSStatement.close(): unregister + closed.set(true) + resultSet
        } finally {
            releaseServerResource();
        }
    }

    /**
     * Await in-flight asynchronous work before the close decision.
     * Default is a no-op; efficient-write statements override this to drain
     * their write queues before the statement may be cached.
     */
    protected void awaitPendingWrites() throws SQLException {
    }

    // PreparedStatement cache support (shared by AbsWSPreparedStatement,
    // WSColumnPreparedStatement and the EW AbstractWSEWPreparedStatement family)

    /**
     * Try to cache this statement; returns true if the statement is now cached
     * (and must stay alive), false if the caller should run the original close
     * path.
     */
    protected boolean tryCache() throws SQLException {
        if (!(this instanceof PreparedStatement)) {
            // WorkerThread (and any future non-PreparedStatement subclass) is
            // never cached.
            return false;
        }
        return ((WSConnection) connection).tryCache((PreparedStatement) this);
    }

    /** Whether this statement is currently in use (handed out to application code). */
    public boolean isInUse() {
        return inUse;
    }

    void markInUse() {
        this.inUse = true;
    }

    void markIdle() {
        this.inUse = false;
    }

    /** Return the sql for cache key construction. */
    String getSql() {
        return this.stmtInfo.getSql();
    }

    /** Return the database for cache key construction. */
    String getDatabase() {
        return this.database;
    }

    /** Return the stmtInfo for cache decisions. */
    StmtInfo getStmtInfo() {
        return this.stmtInfo;
    }

    /**
     * Release the server-side stmt2 resource; final so subclasses cannot
     * bypass the release ordering: mark closed first, then subclass cleanup,
     * then STMT2_CLOSE. Marking closed first is required — EW worker threads
     * only exit after closed=true, so awaiting them before this point would
     * deadlock on cached (never-closed) statements.
     */
    final void releaseServerResource() throws SQLException {
        connection.unregisterStatement(this.instanceId);
        closed.set(true);
        inUse = false;
        try {
            doReleaseServerResource();
        } finally {
            try {
                if (transport.isConnected() && stmtInfo.getStmtId() != 0) {
                    Request closeReq = RequestFactory.generateClose(stmtInfo.getStmtId(), ReqId.getReqID());
                    transport.send(closeReq, getQueryTimeoutInMs());
                }
            } catch (SQLException e) {
                // STMT2_CLOSE is best-effort: transport already retried the
                // send; on failure just close locally and let the server
                // reclaim the handle when the connection drops.
                log.warn("Failed to send STMT2_CLOSE for stmtId {}, closing locally", stmtInfo.getStmtId(), e);
            } finally {
                stmtInfo.setStmtId(0);
            }
        }
    }

    /** Subclass cleanup hook invoked by {@link #releaseServerResource()}. */
    protected void doReleaseServerResource() throws SQLException {
        // Default: nothing to clean up.
    }

    /** Reopen a cached statement so it can be used again. */
    void reopenFromCache() {
        closed.set(false);
    }

    /**
     * Reset per-use state before returning to cache. Subclasses that support
     * caching must override, and the override MUST call
     * {@code super.resetForReuse()} so inherited bindings and counters are
     * cleared.
     */
    protected void resetForReuse() throws SQLException {
        // Default: no-op (worker threads, query statements don't need caching)
    }

    private volatile boolean inUse = false;

    /**
     * Package-private, test-only hook that exposes whether this statement is eligible to
     * use {@code STMT2_BIND_EXEC} for write operations.
     *
     * <p>This does not mean every operation uses bind-exec: query operations still run
     * through the legacy bind + exec flow.
     *
     * @return true if writes may use {@code STMT2_BIND_EXEC}; false if all operations
     * use the legacy {@code STMT2_BIND} + {@code STMT2_EXEC} flow
     */
    boolean isUsingBindExec() {
        return useBindExec;
    }
}
