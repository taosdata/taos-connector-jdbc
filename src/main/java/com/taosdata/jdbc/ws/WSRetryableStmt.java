package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.*;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class WSRetryableStmt extends WSStatement {
    private static final Logger log = LoggerFactory.getLogger(WSRetryableStmt.class);

    protected final ConnectionParam param;
    protected StmtInfo stmtInfo;
    protected volatile SQLException lastError = null;
    protected final AtomicInteger batchInsertedRows;
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
        this.batchInsertedRows = batchInsertedRows;
    }

    public void initStmt(boolean retry) throws SQLException {
        long tmpReqID = ReqId.getReqID();
        Request request = RequestFactory.generateInit(tmpReqID, true, true);
        Stmt2Resp resp = (Stmt2Resp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
        }
        long tmpStmtId = resp.getStmtId();
        stmtInfo.setStmtId(tmpStmtId);

        tmpReqID = ReqId.getReqID();
        Request prepare = RequestFactory.generatePrepare(tmpStmtId, tmpReqID, stmtInfo.getSql());
        long localReconnectCount = transport.getReconnectCount();

        Stmt2PrepareResp prepareResp = (Stmt2PrepareResp) transport.send(prepare, false);
        if (localReconnectCount != transport.getReconnectCount() && retry) {
            // after reconnect, need reprepare
            initStmt(false);
            return;
        }

        if (Code.SUCCESS.getCode() != prepareResp.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(prepareResp.getCode()) + "):" + prepareResp.getMessage());
        }
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
            writeBlockWithRetryInner(rawBlock);
        } finally {
            Utils.releaseByteBuf(rawBlock);
        }
    }

    private void writeBlockWithRetryInner(ByteBuf orgRawBlock) throws SQLException {
        ByteBuf rawBlock = orgRawBlock.duplicate();
        int originalReaderIndex = orgRawBlock.readerIndex();
        int originalWriterIndex = orgRawBlock.writerIndex();
        for (int i = 0; i < param.getRetryTimes(); i++) {
            if (i > 0) {
                rawBlock = orgRawBlock.copy();
                rawBlock.readerIndex(originalReaderIndex);
                rawBlock.writerIndex(originalWriterIndex);
            }
            long reqId = ReqId.getReqID();
            long reconnectCount = transport.getReconnectCount();
            try{
                // modify stmt id
                modifyStmtIdAndReqId(rawBlock, stmtInfo.getStmtId(), reqId);

                // bind
                Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.STMT2_BIND.getAction(),
                        reqId, rawBlock, false);
                if (bindResp == null){
                    throw new SQLException("reconnect, need to resend bind msg");
                }
                if (Code.SUCCESS.getCode() != bindResp.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
                }

                // execute
                reqId = ReqId.getReqID();
                Request request = RequestFactory.generateExec(stmtInfo.getStmtId(), reqId);
                Stmt2ExecResp resp = (Stmt2ExecResp) transport.send(request);
                if (resp == null){
                    throw new SQLException("reconnect, need to resend execute msg");
                }

                if (Code.SUCCESS.getCode() != resp.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
                }

                int affectedRows = resp.getAffected();
                batchInsertedRows.addAndGet(affectedRows);
                return;
            } catch (SQLException e) {
                if (i == param.getRetryTimes() - 1) {
                    lastError = e;
                }
                log.error("Error in writeBlockWithRetry, stmt id: {}, req id: {}" +
                                "retry times: {}, code: {}, msg: {}",
                        stmtInfo.getStmtId(),
                        reqId,
                        i,
                        e.getErrorCode(), e.getMessage());

                // check if connection is reestablished, if so, need to reinit stmt obj and retry
                int realReconnectCount = transport.getReconnectCount();
                if (reconnectCount != realReconnectCount) {
                    log.error("connection reestablished, need to init stmt obj");

                    initStmt(false);
                    continue;
                }

                // retry timeout without waiting
                if (e.getErrorCode() == TSDBErrorNumbers.ERROR_QUERY_TIMEOUT){
                    continue;
                }

                // network issue, retry with waiting
                if (e.getErrorCode() == TSDBErrorNumbers.ERROR_CONNECTION_CLOSED
                        || e.getErrorCode() == TSDBErrorNumbers.ERROR_RESTFul_Client_IOException) {
                    continue;
                }

                lastError = e;
                break;
            } finally {
                log.trace("buffer {}, refCnt: {}", Integer.toHexString(System.identityHashCode(rawBlock)), rawBlock.refCnt());
            }
        }
    }

    public void releaseStmt() throws SQLException {
        if (stmtInfo.getStmtId() != 0 && transport.isConnected()){
            long reqId = ReqId.getReqID();
            Request close = RequestFactory.generateClose(stmtInfo.getStmtId(), reqId);
            transport.send(close);
        }
    }
}
