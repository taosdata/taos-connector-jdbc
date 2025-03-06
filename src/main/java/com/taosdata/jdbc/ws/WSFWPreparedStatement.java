package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.common.*;
import com.taosdata.jdbc.enums.FeildBindType;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.entity.*;
import com.taosdata.jdbc.ws.stmt2.entity.*;
import com.taosdata.jdbc.ws.stmt2.entity.RequestFactory;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class WSFWPreparedStatement extends AbsWSPreparedStatement {
    private final org.slf4j.Logger log = LoggerFactory.getLogger(AbstractDriver.class);

    private final boolean copyData;
    private final int writeThreadNum;
    private ThreadPoolExecutor writerThreads;
    private ArrayList<LinkedBlockingQueue<Map<Integer, Column>>> writeQueueList;
    private final AtomicInteger remainingUnprocessedRows = new AtomicInteger(0);
    private final AtomicInteger batchInsertedRows = new AtomicInteger(0);
    private volatile SQLException lastException;
    private List<WorkerThread> workerThreadList;
    private int addBatchCounts = 0;

    public WSFWPreparedStatement(Transport transport, ConnectionParam param, String database, AbstractConnection connection, String sql, Long instanceId, Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp);

        copyData = param.isCopyData();
        writeThreadNum = param.getBackendWriteThreadNum();

        writerThreads =  (ThreadPoolExecutor) Executors.newFixedThreadPool(writeThreadNum);
        writeQueueList = new ArrayList<>(writeThreadNum);
        for (int i = 0; i < writeThreadNum; i++){
            writeQueueList.add(new LinkedBlockingQueue<>(param.getCacheSizeByRow()));
        }

        workerThreadList = new ArrayList<>(writeThreadNum);

        CommonResp res = null;
        for (int i = 0; i < writeThreadNum; i++){
            WorkerThread workerThread = new WorkerThread(
                    writeQueueList.get(i),
                    sql,
                    transport,
                    param,
                    closed,
                    remainingUnprocessedRows,
                    batchInsertedRows
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

        return batchInsertedRows.getAndSet(0);
    }

    private void waitWriteCompleted() {
        while (remainingUnprocessedRows.get() != 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void checkDataLength(Map<Integer, Column> map) throws SQLException {
        for (int i = 0; i < fields.size(); i++){
            Field field = fields.get(i);
            Column column = map.get(i + 1);
            if (DataLengthCfg.getDataLength(column.getType()) == null && field.getBindType() != FeildBindType.TAOS_FIELD_TBNAME.getValue()){
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
            try {
                writeQueueList.get(index).put(map);
            } catch (InterruptedException ignored) {
            }

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
        if (isClosed())
            return;

        super.close();

        waitWriteCompleted();

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


    static class WorkerThread implements Runnable {
        private final org.slf4j.Logger log = LoggerFactory.getLogger(AbstractDriver.class);

        private final LinkedBlockingQueue<Map<Integer, Column>> dataQueue;
        private final int batchSize;
        private final String sql;
        private long reqId;
        private long stmtId = 0;
        private List<Field> fields;
        private int toBeBindTableNameIndex;
        private int toBeBindTagCount;
        private int toBeBindColCount;
        private int precision;
        private int reconnectCount = 0;

        private final List<TableInfo> tableInfoList = new ArrayList<>();
        private TableInfo tableInfo = TableInfo.getEmptyTableInfo();


        private final Transport transport;
        private final ConnectionParam connectionParam;

        private final AtomicBoolean isClosed;
        private final AtomicInteger remainingUnprocessedRows;
        private final AtomicInteger batchInsertedRows;

        public WorkerThread(LinkedBlockingQueue<Map<Integer, Column>> taskQueue,
                            String sql,
                            Transport transport,
                            ConnectionParam param,
                            AtomicBoolean isClosed,
                            AtomicInteger remainingUnprocessedRows,
                            AtomicInteger batchInsertedRows) {
            this.dataQueue = taskQueue;
            this.batchSize = param.getBatchSizeByRow();
            this.sql = sql;
            this.transport = transport;
            this.connectionParam = param;
            this.isClosed = isClosed;
            this.remainingUnprocessedRows = remainingUnprocessedRows;
            this.batchInsertedRows = batchInsertedRows;
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

            Request prepare = RequestFactory.generatePrepare(stmtId, reqId, sql);
            Stmt2PrepareResp prepareResp = (Stmt2PrepareResp) transport.send(prepare);

            fields = prepareResp.getFields();
            if (!fields.isEmpty()){
                precision = fields.get(0).getPrecision();
            }

            toBeBindTagCount = 0;
            toBeBindColCount = 0;
            for (int i = 0; i < fields.size(); i++){
                Field field = fields.get(i);
                if (field.getBindType() == FeildBindType.TAOS_FIELD_TBNAME.getValue()){
                    toBeBindTableNameIndex = i;
                }
                if (field.getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()){
                    toBeBindTagCount++;
                }
                if (field.getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()){
                    toBeBindColCount++;
                }
            }

            return prepareResp;
        }

        public void releaseStmt() throws SQLException {
            if (stmtId != 0){
                Request close = RequestFactory.generateClose(stmtId, reqId);
                transport.send(close);
            }
        }

        @Override
        public void run() {
            while (!(isClosed.get() && dataQueue.isEmpty())) {
                int polledRow = 0;
                int size = 0;
                try {
                    if (dataQueue.isEmpty()) {
                        Map<Integer, Column> map = dataQueue.poll(10, TimeUnit.MILLISECONDS);
                        if (map == null) {
                            continue;
                        }
                        processOneRow(map);
                        polledRow++;
                    }

                    size = Math.min(dataQueue.size(), this.batchSize - polledRow);
                    for (int i = 0; i < size; i++) {
                        Map<Integer, Column> map = dataQueue.take();
                        processOneRow(map);
                    }

                    if (!isTableInfoEmpty()) {
                        tableInfoList.add(tableInfo);
                    }

                    writeBlockWithRetry();
                    tableInfo = TableInfo.getEmptyTableInfo();
                    tableInfoList.clear();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (SQLException e) {
                    log.error("Error in write data to server, rows: {}, code: {}, msg: {}",
                            (polledRow + size), e.getErrorCode(), e.getMessage());
                } finally {
                    remainingUnprocessedRows.addAndGet(-size - polledRow);
                }
            }
        }


        private void writeBlockWithRetry() throws SQLException {
            for (int i = 0; i < connectionParam.getRetryTimes(); i++) {
                byte[] rawBlock = null;
                try {
                    rawBlock = SerializeBlock.getStmt2BindBlock(
                            reqId,
                            stmtId,
                            tableInfoList,
                            toBeBindTableNameIndex,
                            toBeBindTagCount,
                            toBeBindColCount,
                            precision);
                } catch (SQLException e) {
                    log.error("Error in serialize data to block, code: {}, msg: {}",
                            e.getErrorCode(), e.getMessage());
                    break;
                }

                try{
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
                    break;
                } catch (SQLException e) {
                    log.error("Error in writeBlockWithRetry, retry times: {},  code: {}, msg: {}", i,
                            e.getErrorCode(), e.getMessage());

                    // check if connection is reestablished, if so, need to reinit stmt obj and retry
                    int trueReconnectCoount = transport.getReconnectCount();
                    if (reconnectCount != trueReconnectCoount) {
                        log.error("connection reestablished, need to init stmt obj");

                        reconnectCount = trueReconnectCoount;
                        initStmt();
                        continue;
                    }

                    // only retry timeout
                    if (e.getErrorCode() != TSDBErrorNumbers.ERROR_QUERY_TIMEOUT) {
                        break;
                    }
                }
            }
        }

        private boolean isTableInfoEmpty(){
            return StringUtils.isEmpty(tableInfo.getTableName())
                    && tableInfo.getTagInfo().isEmpty()
                    && tableInfo.getDataList().isEmpty();
        }

        public void processOneRow(Map<Integer, Column> colOrderedMap) {
            if (isTableInfoEmpty()) {
                // first time, bind all
                bindAllToTableInfo(fields, colOrderedMap, tableInfo);
            } else {
                Object tbname = colOrderedMap.get(toBeBindTableNameIndex + 1).getData();
                if ((tbname instanceof String && tableInfo.getTableName().equals(tbname))
                        || (tbname instanceof byte[] && tableInfo.getTableName().equals(new String((byte[]) tbname, StandardCharsets.UTF_8)))) {
                    // same table, only bind col
                    for (ColumnInfo columnInfo : tableInfo.getDataList()) {
                        columnInfo.add(colOrderedMap.get(columnInfo.getIndex()).getData());
                    }
                } else {
                    // different table, flush tableInfo and create a new one
                    tableInfoList.add(tableInfo);
                    tableInfo = TableInfo.getEmptyTableInfo();
                    bindAllToTableInfo(fields, colOrderedMap, tableInfo);
                }
            }
        }
    }
}
