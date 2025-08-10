package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.BlockData;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.enums.FetchState;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.FetchBlockNewResp;
import com.taosdata.jdbc.ws.entity.QueryResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.*;

public abstract class FetchBlockData extends AbstractResultSet {
    private static final Logger log = LoggerFactory.getLogger(FetchBlockData.class);

    protected final Transport transport;
    protected final long queryId;
    protected final long reqId;

    protected volatile boolean isClosed;
    private static final int CACHE_SIZE = 5;
    BlockingQueue<BlockData> blockingQueueOut = new LinkedBlockingQueue<>(CACHE_SIZE);
    ForkJoinPool dataHandleExecutor = getForkJoinPool();
    FetchState fetchState = FetchState.PAUSED;

    protected FetchBlockData(Statement statement, Transport transport,
                             QueryResp response, String database) throws SQLException {
        this.transport = transport;
        this.queryId = response.getId();
        this.reqId = response.getReqId();
    }

    private void startBackendFetch(){
        backFetchExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        backFetchExecutor.submit(() -> {
            try {
                while (!isClosed){
                    BlockData blockData = BlockData.getEmptyBlockData(fields, timestampPrecision);

                    byte[] version = {1, 0};
                    FetchBlockNewResp resp = (FetchBlockNewResp) transport.send(Action.FETCH_BLOCK_NEW.getAction(),
                            reqId, queryId, 7, version);
                    resp.init();

                    if (Code.SUCCESS.getCode() != resp.getCode()) {
                        blockData.setReturnCode(resp.getCode());
                        blockData.setErrorMessage(resp.getMessage());
                        blockingQueueOut.put(blockData);
                        break;
                    }
                    if (resp.isCompleted() || isClosed) {
                        blockData.setCompleted(true);
                        blockingQueueOut.put(blockData);
                        break;
                    }

                    blockData.setBuffer(resp.getBuffer());
                    blockingQueueOut.put(blockData);

                    dataHandleExecutor.submit(blockData::handleData);
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("fetch block error", e);
                BlockData blockData = BlockData.getEmptyBlockData(fields, timestampPrecision);
                while (!isClosed) {
                    try {
                        if (blockingQueueOut.offer(blockData, 10, TimeUnit.MILLISECONDS)){
                            break;
                        }
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        });
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        }

        if (this.forward()) {
            return true;
        }

        fetchBlockNum++;
        if (fetchBlockNum > START_BACKEND_FETCH_BLOCK_NUM) {
            if (backFetchExecutor == null) {
                startBackendFetch();
            }
            BlockData blockData;
            try {
                blockData = blockingQueueOut.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "FETCH DATA INTERRUPTED");
            }

            if (blockData.getReturnCode() != Code.SUCCESS.getCode()) {
                throw TSDBError.createSQLException(blockData.getReturnCode(), "FETCH DATA ERROR:" + blockData.getErrorMessage());
            }
            this.reset();
            if (blockData.isCompleted()) {
                this.isCompleted = true;
                return false;
            }
            blockData.waitTillOK();
            this.result = blockData.getData();
            this.numOfRows = blockData.getNumOfRows();
        } else {
            byte[] version = {1, 0};
            FetchBlockNewResp resp = (FetchBlockNewResp) transport.send(Action.FETCH_BLOCK_NEW.getAction(),
                    reqId, queryId, 7, version);
            resp.init();

            if (Code.SUCCESS.getCode() != resp.getCode()) {
                throw TSDBError.createSQLException(resp.getCode(), "FETCH DATA ERROR:" + resp.getMessage());
            }
            this.reset();
            BlockData blockData = BlockData.getEmptyBlockData(fields, timestampPrecision);

            if (resp.isCompleted() || isClosed) {
                blockData.setCompleted(true);
                isCompleted = true;
                return false;
            }

            blockData.setBuffer(resp.getBuffer());
            blockData.handleData();

            this.result = blockData.getData();
            this.numOfRows = blockData.getNumOfRows();
        }

        return true;
    }

}
