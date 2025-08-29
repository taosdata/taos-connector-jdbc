package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.common.*;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.SyncObj;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.*;
import com.taosdata.jdbc.ws.stmt2.entity.*;
import com.taosdata.jdbc.ws.stmt2.entity.RequestFactory;
import io.netty.buffer.ByteBuf;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

// Efficient Writing Mode PreparedStatement
public class WSEWPreparedStatement extends AbsWSPreparedStatement {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(WSEWPreparedStatement.class);

    private final boolean copyData;
    private final int writeThreadNum;
    private final ThreadPoolExecutor writerThreads;
    private final ArrayList<EWBackendThreadInfo> backendThreadInfoList;
    private final AtomicInteger remainingUnprocessedRows = new AtomicInteger(0);
    private final AtomicInteger batchInsertedRows = new AtomicInteger(0);
    private final AtomicInteger flushIn = new AtomicInteger(0);
    private final List<WorkerThread> workerThreadList;
    private final SyncObj syncObj = new SyncObj();
    private static final ForkJoinPool serializeExecutor = Utils.getForkJoinPool();
    private int addBatchCounts = 0;
    private final StmtInfo stmtInfo;

    public WSEWPreparedStatement(Transport transport, ConnectionParam param, String database, AbstractConnection connection, String sql, Long instanceId, Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp);
        stmtInfo = new StmtInfo(0, 0, toBeBindTableNameIndex, toBeBindTagCount, toBeBindColCount, precision, fields, sql);
        copyData = param.isCopyData();
        writeThreadNum = param.getBackendWriteThreadNum();

        writerThreads =  (ThreadPoolExecutor) Executors.newFixedThreadPool(writeThreadNum);
        backendThreadInfoList = new ArrayList<>(writeThreadNum);
        for (int i = 0; i < writeThreadNum; i++){
            backendThreadInfoList.add(new EWBackendThreadInfo(param.getCacheSizeByRow(), param.getCacheSizeByRow() / param.getBatchSizeByRow()));
        }

        workerThreadList = new ArrayList<>(writeThreadNum);

        CommonResp res = null;
        for (int i = 0; i < writeThreadNum; i++){
            WorkerThread workerThread = new WorkerThread(
                    backendThreadInfoList.get(i),
                    stmtInfo,
                    transport,
                    param,
                    closed,
                    remainingUnprocessedRows,
                    batchInsertedRows,
                    flushIn,
                    syncObj
                    );
            workerThreadList.add(workerThread);
           res = workerThread.initStmt();
           if (res.getCode() != Code.SUCCESS.getCode()){
               break;
           }
        }

        if (res != null && res.getCode() != Code.SUCCESS.getCode()){
            for (WorkerThread workerThread : workerThreadList){
                workerThread.releaseStmt();
            }
            throw new SQLException("(0x" + Integer.toHexString(res.getCode()) + "):" + res.getMessage());
        }

        for (WorkerThread workerThread : workerThreadList){
            writerThreads.submit(workerThread);
        }
    }

    private Map<Integer, Column> copyMap(Map<Integer, Column> originalMap) {
        Map<Integer, Column> dstMap = new HashMap<>();
        originalMap.forEach((key, src) -> {
            Column dst = src;
            if (copyData && src.getData() instanceof byte[]) {
                byte[] srcBytes = (byte[]) src.getData();
                byte[] copiedValue = new byte[srcBytes.length];
                System.arraycopy(srcBytes, 0, copiedValue, 0, srcBytes.length);
                dst = new Column(copiedValue, src.getType(), src.getIndex());
            }

            dstMap.put(key, dst);
        });
        return dstMap;
    }


    @Override
    public boolean execute() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        if (isInsert){
            executeUpdate();
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support insert.");
        }

        return !isInsert;
    }
    @Override
    public ResultSet executeQuery() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support insert.");
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        waitWriteCompleted();

        Exception lastError = null;
        for (WorkerThread workerThread : workerThreadList) {
            Exception tempEx = workerThread.getAndClearLastError();
            if (tempEx != null && lastError == null) {
                lastError = tempEx;
            }
        }

        int totalRowsInserted = batchInsertedRows.getAndSet(0);
        if (lastError != null) {
            throw new SQLException("InsertedRows: " + totalRowsInserted + ", ErrorInfo: " + lastError.getMessage(), "", TSDBErrorNumbers.ERROR_FW_WRITE_ERROR);
        }
        return totalRowsInserted;
    }

    private void waitWriteCompleted() {
        flushIn.incrementAndGet();
        while (remainingUnprocessedRows.get() != 0) {
            try {
                syncObj.await();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void checkDataLength(Map<Integer, Column> map) throws SQLException {
        for (int i = 0; i < fields.size(); i++){
            Field field = fields.get(i);
            Column column = map.get(i + 1);
            if (DataLengthCfg.getDataLength(column.getType()) == null && field.getBindType() != FieldBindType.TAOS_FIELD_TBNAME.getValue()){
                if (column.getData() instanceof byte[]){
                    byte[] data = (byte[]) column.getData();
                    if (data.length > field.getBytes()){
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "data length is too long, column index " + i);
                    }
                }
                if (column.getData() instanceof String){
                    String data = (String) column.getData();
                    if (data.getBytes().length > field.getBytes()){
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "data length is too long, column index " + i);
                    }
                }
            }
        }
    }

    private static void triggerSerializeProgressive(EWBackendThreadInfo ewBackendThreadInfo,
                                                 StmtInfo stmtInfo,
                                                    int batchSize) {
        if (!ewBackendThreadInfo.getWriteQueue().isEmpty()
                && ewBackendThreadInfo.getSerialQueue().remainingCapacity() > 0
                && ewBackendThreadInfo.getSerializeRunning().compareAndSet(false, true)) {
            WSEWSerializationTask serializeTask = new WSEWSerializationTask(
                    ewBackendThreadInfo,
                    Math.min(ewBackendThreadInfo.getWriteQueue().size(), batchSize),
                    stmtInfo,
                    true);
            serializeExecutor.submit(serializeTask);
        }
    }
    private static void triggerSerializeIfNeeded(EWBackendThreadInfo ewBackendThreadInfo,
                                                 StmtInfo stmtInfo,
                                                 int batchSize) {
        if (ewBackendThreadInfo.getWriteQueue().size() >= batchSize
                && ewBackendThreadInfo.getSerialQueue().remainingCapacity() > 0
                && ewBackendThreadInfo.getSerializeRunning().compareAndSet(false, true)) {
            WSEWSerializationTask serializeTask = new WSEWSerializationTask(
                    ewBackendThreadInfo,
                    batchSize,
                    stmtInfo,
                    false);
            serializeExecutor.submit(serializeTask);
        }
    }
    @Override
    public void addBatch() throws SQLException {
        if (colOrderedMap.size() == fields.size()){
            // jdbc standard bind api
            Map<Integer, Column> map = copyMap(colOrderedMap);

            if (param.isStrictCheck()){
                checkDataLength(map);
            }

            int hashCode;
            Object o = map.get(toBeBindTableNameIndex + 1).getData();
            if (o instanceof String){
                hashCode = o.hashCode();
            } else if (o instanceof byte[]){
                hashCode = Arrays.hashCode((byte[]) o);
            } else {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "error type tbname.");
            }

            if (hashCode < 0){
                hashCode = -hashCode;
            }

            int index = hashCode % writeThreadNum;
            EWBackendThreadInfo ewBackendThreadInfo = backendThreadInfoList.get(index);

            try {
                ewBackendThreadInfo.getWriteQueue().put(map);
            } catch (InterruptedException ignored) {
            }

            triggerSerializeIfNeeded(ewBackendThreadInfo, stmtInfo, param.getBatchSizeByRow());

            remainingUnprocessedRows.incrementAndGet();
            addBatchCounts++;
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support standard jdbc bind api.");
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {

        int[] ints = new int[addBatchCounts];
        for (int i = 0, len = ints.length; i < len; i++){
            ints[i] = SUCCESS_NO_INFO;
        }

        addBatchCounts = 0;
        return ints;
    }

    @Override
    public void close() throws SQLException {
        waitWriteCompleted();
        if (isClosed())
            return;

        super.close();

        // wait backFetchExecutor to finish
        if (writerThreads != null) {
            while (writerThreads.getActiveCount() != 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            if (!writerThreads.isShutdown()) {
                writerThreads.shutdown();
            }
        }

        for (WorkerThread workerThread : workerThreadList){
            workerThread.releaseStmt();
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "Fast write mode only support insert.");
    }

    @Override
    public void columnDataAddBatch() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }


    @Override
    public void columnDataExecuteBatch() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void columnDataCloseBatch() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    static class WSEWSerializationTask extends RecursiveAction {
        final ArrayBlockingQueue<Map<Integer, Column>> writeQueue;
        final ArrayBlockingQueue<EWRawBlock> serialQueue;
        final int batchSize;

        private TableInfo tableInfo = TableInfo.getEmptyTableInfo();
        private final HashMap<ByteBuffer, TableInfo> tableInfoMap = new HashMap<>();
        private final StmtInfo stmtInfo;
        private final AtomicBoolean running;
        private final boolean isProgressive;

        public WSEWSerializationTask(EWBackendThreadInfo ewBackendThreadInfo,
                                     int batchSize,
                                     StmtInfo stmtInfo,
                                     boolean isProgressive) {
            this.writeQueue = ewBackendThreadInfo.getWriteQueue();
            this.serialQueue = ewBackendThreadInfo.getSerialQueue();
            this.running = ewBackendThreadInfo.getSerializeRunning();
            this.batchSize = batchSize;
            this.stmtInfo = stmtInfo;
            this.isProgressive = isProgressive;
        }

        public void processOneRow(Map<Integer, Column> colOrderedMap) throws SQLException {
            if (isTableInfoEmpty()) {
                // first time, bind all
                bindAllToTableInfo(stmtInfo.getFields(), colOrderedMap, tableInfo);
            } else {
                Object tbname = colOrderedMap.get(stmtInfo.getToBeBindTableNameIndex() + 1).getData();
                ByteBuffer tempTableName;
                if (tbname instanceof String){
                    tempTableName = ByteBuffer.wrap(((String)tbname).getBytes());
                } else if (tbname instanceof byte[]){
                    tempTableName = ByteBuffer.wrap((byte[]) tbname);
                } else {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name must be string or binary");
                }

                if (tableInfo.getTableName().equals(tempTableName)){
                    // same table, only bind col
                    bindColToTableInfo(tableInfo, colOrderedMap);
                } else if (tableInfoMap.containsKey(tempTableName)){
                    // same table, only bind col
                    TableInfo tbInfo = tableInfoMap.get(tempTableName);
                    bindColToTableInfo(tbInfo, colOrderedMap);
                } else {
                    // different table, flush tableInfo and create a new one
                    tableInfoMap.put(tableInfo.getTableName(), tableInfo);
                    tableInfo = TableInfo.getEmptyTableInfo();
                    bindAllToTableInfo(stmtInfo.getFields(), colOrderedMap, tableInfo);
                }
            }
        }

        private boolean isTableInfoEmpty(){
            return tableInfo.getTableName().capacity() == 0
                    && tableInfo.getTagInfo().isEmpty()
                    && tableInfo.getDataList().isEmpty();
        }

        private void bindColToTableInfo(TableInfo tableInfo, Map<Integer, Column> colOrderedMap){
            for (ColumnInfo columnInfo : tableInfo.getDataList()) {
                columnInfo.add(colOrderedMap.get(columnInfo.getIndex()).getData());
            }
        }

        private void putEWRawBlock(EWRawBlock ewRawBlock){
            try {
                serialQueue.put(ewRawBlock);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        @Override
        protected void compute() {
            Exception lastError = null;
            while (writeQueue.size() >= batchSize && serialQueue.remainingCapacity() > 0) {
                for (int i = 0; i < batchSize; i++) {
                    try {
                        Map<Integer, Column> map = writeQueue.take();
                        processOneRow(map);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (SQLException e) {
                        lastError = e;
                    }
                }
                if (!isTableInfoEmpty()) {
                    tableInfoMap.put(tableInfo.getTableName(), tableInfo);
                }

                if (lastError != null) {
                    tableInfo = TableInfo.getEmptyTableInfo();
                    tableInfoMap.clear();
                    putEWRawBlock(new EWRawBlock(null, batchSize, lastError));
                    break;
                }

                try {
                    ByteBuf rawBlock = SerializeBlock.getStmt2BindBlock(
                            tableInfoMap,
                            stmtInfo);
                    putEWRawBlock(new EWRawBlock(rawBlock, batchSize, lastError));
                    log.trace("buffer allocated: {}", Integer.toHexString(System.identityHashCode(rawBlock)));
                } catch (Exception e) {
                    lastError = e;
                    putEWRawBlock(new EWRawBlock(null, batchSize, lastError));
                    log.error("Error in serialize data to block, stmt id: {}, req id: {}", stmtInfo.getStmtId(), stmtInfo.getReqId(), e);
                    break;
                } finally {
                    tableInfo = TableInfo.getEmptyTableInfo();
                    tableInfoMap.clear();
                }
                if (isProgressive){
                    break;
                }
            }
            running.set(false);
        }
    }

    static class WorkerThread implements Runnable {
        private static final org.slf4j.Logger log = LoggerFactory.getLogger(WorkerThread.class);
        private final EWBackendThreadInfo backendThreadInfo;
        private final StmtInfo stmtInfo;
        private long reqId;
        private long stmtId = 0;
        private int reconnectCount = 0;
        private final Transport transport;
        private final ConnectionParam connectionParam;
        private final AtomicBoolean isClosed;
        private final AtomicInteger remainingUnprocessedRows;
        private final AtomicInteger batchInsertedRows;
        private final AtomicInteger flushIn;
        private final SyncObj syncObj;
        private Exception lastError;

        public WorkerThread(EWBackendThreadInfo backendThreadInfo,
                            StmtInfo stmtInfo,
                            Transport transport,
                            ConnectionParam param,
                            AtomicBoolean isClosed,
                            AtomicInteger remainingUnprocessedRows,
                            AtomicInteger batchInsertedRows,
                            AtomicInteger flushIn,
                            SyncObj syncObj) {
            this.backendThreadInfo = backendThreadInfo;
            this.stmtInfo = stmtInfo;
            this.transport = transport;
            this.connectionParam = param;
            this.isClosed = isClosed;
            this.remainingUnprocessedRows = remainingUnprocessedRows;
            this.batchInsertedRows = batchInsertedRows;
            this.flushIn = flushIn;
            this.syncObj = syncObj;
        }

        public CommonResp initStmt() throws SQLException {
            long tmpReqID = ReqId.getReqID();
            Request request = RequestFactory.generateInit(reqId, true, true);
            Stmt2Resp resp = (Stmt2Resp) transport.send(request);
            if (Code.SUCCESS.getCode() != resp.getCode()) {
                return resp;
            }
            long tmpStmtId = resp.getStmtId();
            this.reqId = tmpReqID;
            this.stmtId = tmpStmtId;

            Request prepare = RequestFactory.generatePrepare(stmtId, reqId, stmtInfo.getSql());
            Stmt2PrepareResp prepareResp = (Stmt2PrepareResp) transport.send(prepare);
            if (Code.SUCCESS.getCode() != prepareResp.getCode()) {
                return resp;
            }

            return prepareResp;
        }

        public void releaseStmt() throws SQLException {
            if (stmtId != 0 && transport.isConnected()){
                Request close = RequestFactory.generateClose(stmtId, reqId);
                transport.send(close);
            }
        }

        @Override
        public void run() {
            int flushInLocal = 0;
            ArrayBlockingQueue<Map<Integer, Column>> writeQueue = backendThreadInfo.getWriteQueue();
            ArrayBlockingQueue<EWRawBlock> serialQueue = backendThreadInfo.getSerialQueue();
            AtomicBoolean serializeRunning = backendThreadInfo.getSerializeRunning();
            while (!(isClosed.get()
                    && serialQueue.isEmpty()
                    && writeQueue.isEmpty()
                    && !serializeRunning.get())) {
                EWRawBlock ewRawBlock;
                int rowCount = 0;
                try {
                    if (flushIn.get() != flushInLocal) {
                        flushInLocal = flushIn.get();
                        syncObj.signal();
                    }

                    ewRawBlock = serialQueue.poll(50, TimeUnit.MILLISECONDS);
                    if (ewRawBlock == null) {
                        triggerSerializeProgressive(backendThreadInfo, stmtInfo, connectionParam.getBatchSizeByRow());
                        continue;
                    }

                    rowCount = ewRawBlock.getRowCount();
                    lastError = ewRawBlock.getLastError();
                    if (lastError == null) {
                        writeBlockWithRetry(ewRawBlock.getByteBuf());
                        triggerSerializeIfNeeded(backendThreadInfo, stmtInfo, connectionParam.getBatchSizeByRow());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (SQLException e) {
                    lastError = e;
                    log.error("Error in write data to server, stmt id: {}, req id: {}" +
                                    "rows: {}, code: {}, msg: {}",
                            stmtId,
                            reqId,
                            rowCount, e.getErrorCode(), e.getMessage());
                } finally {
                    if (rowCount > 0) {
                        remainingUnprocessedRows.addAndGet(-rowCount);
                    }
                }
            }

            syncObj.signal();
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

        private void writeBlockWithRetry(ByteBuf rawBlock) throws SQLException {
            Utils.retainByteBuf(rawBlock);
            writeBlockWithRetryInner(rawBlock);
            Utils.releaseByteBuf(rawBlock);
        }
        private void writeBlockWithRetryInner(ByteBuf orgRawBlock) throws SQLException {
            ByteBuf rawBlock = orgRawBlock.duplicate();
            int originalReaderIndex = orgRawBlock.readerIndex();
            int originalWriterIndex = orgRawBlock.writerIndex();
            for (int i = 0; i < connectionParam.getRetryTimes(); i++) {
                if (i > 0) {
                    rawBlock = orgRawBlock.copy();
                    rawBlock.readerIndex(originalReaderIndex);
                    rawBlock.writerIndex(originalWriterIndex);
                }
                try{
                    reqId = ReqId.getReqID();
                    // modify stmt id
                    modifyStmtIdAndReqId(rawBlock, stmtId, reqId);

                    // bind
                    Stmt2Resp bindResp = (Stmt2Resp) transport.send(Action.STMT2_BIND.getAction(),
                            reqId, rawBlock);
                    if (Code.SUCCESS.getCode() != bindResp.getCode()) {
                        throw new SQLException("(0x" + Integer.toHexString(bindResp.getCode()) + "):" + bindResp.getMessage());
                    }

                    // execute
                    Request request = RequestFactory.generateExec(stmtId, reqId);
                    Stmt2ExecResp resp = (Stmt2ExecResp) transport.send(request);
                    if (Code.SUCCESS.getCode() != resp.getCode()) {
                        throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
                    }

                    int affectedRows = resp.getAffected();
                    batchInsertedRows.addAndGet(affectedRows);
                    return;
                } catch (SQLException e) {
                    if (i == connectionParam.getRetryTimes() - 1) {
                        lastError = e;
                    }
                    log.error("Error in writeBlockWithRetry, stmt id: {}, req id: {}" +
                                    "retry times: {}, code: {}, msg: {}",
                            stmtId,
                            reqId,
                            i,
                            e.getErrorCode(), e.getMessage());

                    // check if connection is reestablished, if so, need to reinit stmt obj and retry
                    int realReconnectCount = transport.getReconnectCount();
                    if (reconnectCount != realReconnectCount) {
                        log.error("connection reestablished, need to init stmt obj");

                        reconnectCount = realReconnectCount;
                        initStmt();
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
                    break;
                } finally {
                    log.trace("buffer {}, refCnt: {}", Integer.toHexString(System.identityHashCode(rawBlock)), rawBlock.refCnt());
                }
            }
        }
        public Exception getAndClearLastError() {
            Exception tmp = lastError;
            lastError = null;
            return tmp;
        }
    }
}
