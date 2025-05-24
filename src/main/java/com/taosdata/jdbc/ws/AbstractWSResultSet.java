package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.BlockData;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;
import com.taosdata.jdbc.ws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public abstract class AbstractWSResultSet extends AbstractResultSet {
    private final Logger log = LoggerFactory.getLogger(Transport.class);

    protected final Statement statement;
    protected final Transport transport;
    protected final long queryId;
    protected final long reqId;

    protected volatile boolean isClosed;
    private boolean isCompleted = false;
    // meta
    protected final ResultSetMetaData metaData;
    protected final List<RestfulResultSet.Field> fields = new ArrayList<>();
    protected final List<String> columnNames;
    // data
    protected List<List<Object>> result = new ArrayList<>();

    protected int numOfRows = 0;
    protected int rowIndex = 0;
    private static final int CACHE_SIZE = 5;
    BlockingQueue<BlockData> blockingQueueOut = new LinkedBlockingQueue<>(CACHE_SIZE);
    ThreadPoolExecutor backFetchExecutor;
    ForkJoinPool dataHandleExecutor = getForkJoinPool();

    private int fetchBlockNum = 0;
    private final int START_BACKEND_FETCH_BLOCK_NUM = 3;
    protected AbstractWSResultSet(Statement statement, Transport transport,
                               QueryResp response, String database) throws SQLException {
        this.statement = statement;
        this.transport = transport;
        this.queryId = response.getId();
        this.reqId = response.getReqId();
        columnNames = Arrays.asList(response.getFieldsNames());
        for (int i = 0; i < response.getFieldsCount(); i++) {
            String colName = response.getFieldsNames()[i];
            int taosType = response.getFieldsTypes()[i];
            int jdbcType = DataType.convertTaosType2DataType(taosType).getJdbcTypeValue();
            int length = response.getFieldsLengths()[i];
            int scale = 0;

            if (response.getFieldsScales() != null)
            {
                scale = response.getFieldsScales()[i];
            }
            fields.add(new RestfulResultSet.Field(colName, jdbcType, length, "", taosType, scale));
        }
        this.metaData = new RestfulResultSetMetaData(database, fields);
        this.timestampPrecision = response.getPrecision();
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

    private boolean forward() {
        if (this.rowIndex > this.numOfRows) {
            return false;
        }

        return ((++this.rowIndex) < this.numOfRows);
    }

    public void reset() {
        this.rowIndex = 0;
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
                throw TSDBError.createSQLException(blockData.getReturnCode(), "FETCH DATA ERROR");
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
//            this.fetchBlockNewResp.init();
//            FetchBlockNewResp resp = this.fetchBlockNewResp;
//
//
//            if (Code.SUCCESS.getCode() != resp.getCode()) {
//                throw TSDBError.createSQLException(resp.getCode(), "FETCH DATA ERROR");
//            }
//            this.reset();
//            BlockData blockData = BlockData.getEmptyBlockData(fields, timestampPrecision);
//
//            if (resp.isCompleted() || isClosed) {
//                blockData.setCompleted(true);
//                isCompleted = true;
//                return false;
//            }
//
//            blockData.setBuffer(resp.getBuffer());
//            blockData.handleData();
//
//            this.result = blockData.getData();
//            this.numOfRows = blockData.getNumOfRows();


            byte[] version = {1, 0};
            FetchBlockNewResp resp = (FetchBlockNewResp) transport.send(Action.FETCH_BLOCK_NEW.getAction(),
                    reqId, queryId, 7, version);
            resp.init();

            if (Code.SUCCESS.getCode() != resp.getCode()) {
                throw TSDBError.createSQLException(resp.getCode(), "FETCH DATA ERROR");
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

    @Override
    public void close() throws SQLException {
        synchronized (this) {
            if (!this.isClosed) {
                this.isClosed = true;

                // wait backFetchExecutor to finish
                if (backFetchExecutor != null) {
                    while (backFetchExecutor.getActiveCount() != 0) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    if (!backFetchExecutor.isShutdown()) {
                        backFetchExecutor.shutdown();
                    }
                }

                if (!isCompleted) {
                    FetchReq closeReq = new FetchReq();
                    closeReq.setReqId(queryId);
                    closeReq.setId(queryId);
                    transport.sendWithoutResponse(new Request(Action.FREE_RESULT.getAction(), closeReq));
                }
            }
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.metaData;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }
}
