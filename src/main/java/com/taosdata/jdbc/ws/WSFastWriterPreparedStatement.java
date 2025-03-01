package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ColumnInfo;
import com.taosdata.jdbc.common.SerializeBlock;
import com.taosdata.jdbc.common.TableInfo;
import com.taosdata.jdbc.enums.FastWriterMsgType;
import com.taosdata.jdbc.enums.FeildBindType;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.FetchBlockNewResp;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.stmt2.entity.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.taosdata.jdbc.TSDBConstants.*;

public class WSFastWriterPreparedStatement extends AbstractWSPreparedStatement {

    private boolean copyData = false;
    private final long[] reqIdArray;
    private final long[] stmtIdArray;

    private int addBatchCounts = 0;

    private final int writeThreadNum;
    private final LinkedBlockingQueue<Map<Integer, Column>> originDataCache = new LinkedBlockingQueue<>();


    private final LinkedBlockingQueue<FastWriteMsg> msgQueue = new LinkedBlockingQueue<>();
//    private ThreadPoolExecutor dataDistributionThread =  (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private ThreadPoolExecutor writerThreads;
    private ArrayList<LinkedBlockingQueue<Map<Integer, Column>>> writeQueueList;
    private static final ForkJoinPool calcThreadPool = Utils.getForkJoinPool();
    private final AtomicInteger totalWriteQueueSize;

    private volatile SQLException lastException;


    public WSFastWriterPreparedStatement(Transport transport, ConnectionParam param, String database, AbstractConnection connection, String sql, Long instanceId, Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp);

        copyData = param.isCopyData();
        writeThreadNum = param.getBackendWriteThreadNum();

        stmtIdArray = new long[param.getBackendWriteThreadNum()];
        reqIdArray = new long[param.getBackendWriteThreadNum()];

        for (int i = 0; i < param.getBackendWriteThreadNum(); i++){
            long reqId = ReqId.getReqID();
            Request request = RequestFactory.generateInit(reqId, true, true);
            Stmt2Resp resp = (Stmt2Resp) transport.send(request);
            if (Code.SUCCESS.getCode() != resp.getCode()) {
                // close other stmt
                closeAllStmt();
                throw new SQLException("(0x" + Integer.toHexString(resp.getCode()) + "):" + resp.getMessage());
            }

            long stmtId = resp.getStmtId();
            Request prepare = RequestFactory.generatePrepare(stmtId, reqId, sql);
            Stmt2PrepareResp res = (Stmt2PrepareResp) transport.send(prepare);
            if (Code.SUCCESS.getCode() != res.getCode()) {
                // close other stmt
                closeAllStmt();
                throw new SQLException("(0x" + Integer.toHexString(res.getCode()) + "):" + res.getMessage());
            }

            reqIdArray[i] = reqId;
            stmtIdArray[i] = stmtId;
        }

//        dataDistributionThread =  (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        writerThreads =  (ThreadPoolExecutor) Executors.newFixedThreadPool(writeThreadNum);
        writeQueueList = new ArrayList<>(writeThreadNum);
        for (int i = 0; i < writeThreadNum; i++){
            writeQueueList.add(new LinkedBlockingQueue<>(param.getCacheSizeByRow()));
        }

        totalWriteQueueSize = new AtomicInteger(param.getCacheSizeByRow() * writeThreadNum);

        for (int i = 0; i < writeThreadNum; i++){
            WorkerThread workerThread = new WorkerThread(
                    writeQueueList.get(i),
                    param.getBatchSizeByRow(),
                    fields,
                    reqIdArray[i],
                    stmtIdArray[i],
                    toBeBindTableNameIndex,
                    toBeBindTagCount,
                    toBeBindColCount,
                    precision,
                    transport,
                    closed
            );

            // 提交任务到线程池
            writerThreads.submit(workerThread);
        }






//        // start distribute thread
//        dataDistributionThread.submit(() -> {
//            try {
//                while (!isClosed()){
//
//                    FastWriteMsg msg = msgQueue.take();
//                    switch (msg.getMessageType()) {
//                        case EXCEEDS_BATCH_SIZE:
//                            if (originDataCache.size() < param.getBatchSizeByRow()){
//                                break;
//                            }
//                            if (totalWriteQueueSize.get() > 0) {
//                                totalWriteQueueSize.decrementAndGet();
//                                ArrayList<Map<Integer, Column>> batchList = new ArrayList<>(param.getBatchSizeByRow());
//                                for (int i = 0; i < param.getBatchSizeByRow(); i++) {
//                                    Map<Integer, Column> map = originDataCache.take();
//                                    batchList.add(map);
//                                }
//                                for (int i = 0; i < writeThreadNum; i++) {
//                                    LinkedBlockingQueue<byte[]> l = writeQueueList.get(i);
//                                    if (l.size() < WRITE_QUEUE_SIZE) {
//                                        SerializeBatchData serializeBatchData = new SerializeBatchData(batchList,
//                                                l,
//                                                fields,
//                                                reqIdArray[i],
//                                                stmtIdArray[i],
//                                                toBeBindTableNameIndex,
//                                                toBeBindTagCount,
//                                                toBeBindColCount,
//                                                precision,
//                                                lastException);
//                                        calcThreadPool.submit(serializeBatchData::clac);
//                                    }
//                                }
//                            }
//                            break;
//                        case LINGER_MS_EXPIRED:
//
//                            break;
//                        case WRITE_COMPLETED:
//                            break;
//                    }
//
//                }
//            } catch (InterruptedException ignored) {
//                Thread.currentThread().interrupt();
//            } catch (Exception e) {
//                log.error("fetch block error", e);
//                BlockData blockData = BlockData.getEmptyBlockData(fields, timestampPrecision);
//                while (!isClosed) {
//                    try {
//                        if (blockingQueueOut.offer(blockData, 10, TimeUnit.MILLISECONDS)){
//                            break;
//                        }
//                    } catch (InterruptedException ignored) {
//                        Thread.currentThread().interrupt();
//                        return;
//                    }
//                }
//            }
//        });
    }

    private void closeAllStmt() throws SQLException {
        for (int i = 0; i < param.getBackendWriteThreadNum(); i++){
            if (stmtIdArray[i] > 0){
                Request close = RequestFactory.generateClose(stmtIdArray[i], reqIdArray[i]);
                transport.sendWithoutResponse(close);
            }
        }
    }

    private Map<Integer, Column> copyMap(Map<Integer, Column> originalMap) {
        Map<Integer, Column> dstMap = new HashMap<>();
        originalMap.forEach((key, src) -> {
            Column dst = src;
            if (src.getData() instanceof Timestamp && copyData) {
                     dst = new Column(new Timestamp(((Timestamp) src.getData()).getTime()), src.getType(), src.getIndex());
            }

            if (src.getData() instanceof byte[] && copyData) {
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
    public boolean execute(String sql, Long reqId) throws SQLException {
        return super.execute(sql, reqId);
    }

    @Override
    public boolean execute() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        if (isInsert){
            executeUpdate();
        } else {
            executeQuery();
        }

        return !isInsert;
    }
    @Override
    public ResultSet executeQuery() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support insert.");
    }

    @Override
    public int executeUpdate() throws SQLException {
//        if (!this.isInsert){
//            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "The insert SQL must be prepared.");
//        }
//
//        if (fields.isEmpty()){
//            return this.executeUpdate(this.rawSql);
//        }
//
//        if (colOrderedMap.size() == fields.size()){
//            // bind all
//            bindAllColWithStdApi();
//        } else{
//            // mixed standard api and extended api, only support one table
//            onlyBindTag();
//            onlyBindCol();
//        }
//
//        if (isTableInfoEmpty()){
//            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEUPDATE, "no data to be bind");
//        }
//
//        tableInfoList.add(tableInfo);
//        return executeBatchImpl();

        return 0;
    }


    @Override
    public void addBatch() throws SQLException {
        if (colOrderedMap.size() == fields.size()){
            // jdbc standard bind api
            Map<Integer, Column> map = copyMap(colOrderedMap);

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

            addBatchCounts++;
        } else {
            addBatchCounts = 0;
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

        Stmt2Resp errRes = null;

        for (int i = 0; i < param.getBackendWriteThreadNum(); i++) {
            Request close = RequestFactory.generateClose(stmtIdArray[i], reqIdArray[i]);
            Stmt2Resp resp = (Stmt2Resp) transport.send(close);

            if (Code.SUCCESS.getCode() != resp.getCode()) {
                errRes = resp;
            }
        }
        if (errRes != null) {
            throw new SQLException("(0x" + Integer.toHexString(errRes.getCode()) + "):" + errRes.getMessage());
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (this.getResultSet() == null)
            return null;
        return getResultSet().getMetaData();
    }

    @Override
    public void columnDataAddBatch() throws SQLException {
//        if (!colOrderedMap.isEmpty()){
//            throw new SQLException("column data is not empty");
//        }
//
//        while (!tag.isEmpty()){
//            ColumnInfo columnInfo = tag.poll();
//            if (isInsert && columnInfo.getType() != tagTypeList.get(columnInfo.getIndex())){
//                tableInfo.getTagInfo().add(new ColumnInfo(columnInfo.getIndex(), columnInfo.getDataList(), tagTypeList.get(columnInfo.getIndex())));
//            } else {
//                tableInfo.getTagInfo().add(columnInfo);
//            }
//
//        }
//        while (!colListQueue.isEmpty()) {
//            ColumnInfo columnInfo = colListQueue.poll();
//            if (isInsert && columnInfo.getType() != colTypeList.get(columnInfo.getIndex())){
//                tableInfo.getDataList().add(new ColumnInfo(columnInfo.getIndex(), columnInfo.getDataList(), colTypeList.get(columnInfo.getIndex())));
//            } else {
//                tableInfo.getDataList().add(columnInfo);
//            }
//        }
//        tableInfoList.add(tableInfo);
//        tableInfo = TableInfo.getEmptyTableInfo();
    }


    @Override
    public void columnDataExecuteBatch() throws SQLException {

    }

    public void columnDataCloseBatch() throws SQLException {
        this.close();
    }


    static class WorkerThread implements Runnable {
        private final LinkedBlockingQueue<Map<Integer, Column>> dataQueue;
        private final int batchSize;
        private final List<Field> fields;
        private final long reqId;
        private final long stmtId;
        private final int toBeBindTableNameIndex;
        private final int toBeBindTagCount;
        private final int toBeBindColCount;
        private final int precision;

        private final List<TableInfo> tableInfoList = new ArrayList<>();
        private TableInfo tableInfo = TableInfo.getEmptyTableInfo();


        private final Transport transport;

        private final AtomicBoolean isClosed;

        public WorkerThread(LinkedBlockingQueue<Map<Integer, Column>> taskQueue,
                            int batchSize,
                            List<Field> fields,
                            long reqId,
                            long stmtId,
                            int toBeBindTableNameIndex,
                            int toBeBindTagCount,
                            int toBeBindColCount,
                            int precision,
                            Transport transport,
                            AtomicBoolean isClosed) {
            this.dataQueue = taskQueue;
            this.batchSize = batchSize;
            this.fields = fields;
            this.reqId = reqId;
            this.stmtId = stmtId;
            this.toBeBindTableNameIndex = toBeBindTableNameIndex;
            this.toBeBindTagCount = toBeBindTagCount;
            this.toBeBindColCount = toBeBindColCount;
            this.precision = precision;
            this.transport = transport;
            this.isClosed = isClosed;
        }

        @Override
        public void run() {
            int totalRow = 0;
            while (!isClosed.get() || !dataQueue.isEmpty()) {
                try {
                    if (dataQueue.size() > batchSize || isClosed.get()) {
                        int brows = 0;
                        int size = Math.min(dataQueue.size(), this.batchSize);
                        for (int i = 0; i < size; i++) {
                            Map<Integer, Column> map = dataQueue.take();
                            processOneRow(map);
                            totalRow++;
                            brows++;
                        }

                        if (!isTableInfoEmpty()) {
                            tableInfoList.add(tableInfo);
                        }

                        byte[] rawBlock = SerializeBlock.getStmt2BindBlock(
                                reqId,
                                stmtId,
                                tableInfoList,
                                toBeBindTableNameIndex,
                                toBeBindTagCount,
                                toBeBindColCount,
                                precision);

                        // 执行写入
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

                        tableInfo = TableInfo.getEmptyTableInfo();
                        tableInfoList.clear();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (SQLException e) {
                    tableInfo = TableInfo.getEmptyTableInfo();
                    tableInfoList.clear();
                    // record error
                    throw new RuntimeException(e);
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
                bindAllToTableInfo(colOrderedMap);
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
                    bindAllToTableInfo(colOrderedMap);
                }
            }
        }

        public void bindAllToTableInfo(Map<Integer, Column> colOrderedMap) {
            for (int index = 0; index < fields.size(); index++) {
                if (fields.get(index).getBindType() == FeildBindType.TAOS_FIELD_TBNAME.getValue()) {
                    if (colOrderedMap.get(index + 1).getData() instanceof byte[]) {
                        tableInfo.setTableName(new String((byte[]) colOrderedMap.get(index + 1).getData(), StandardCharsets.UTF_8));
                    }
                    if (colOrderedMap.get(index + 1).getData() instanceof String) {
                        tableInfo.setTableName((String) colOrderedMap.get(index + 1).getData());
                    }
                } else if (fields.get(index).getBindType() == FeildBindType.TAOS_FIELD_TAG.getValue()) {
                    LinkedList<Object> list = new LinkedList<>();
                    list.add(colOrderedMap.get(index + 1).getData());
                    tableInfo.getTagInfo().add(new ColumnInfo(index + 1, list, fields.get(index).getFieldType()));
                } else if (fields.get(index).getBindType() == FeildBindType.TAOS_FIELD_COL.getValue()) {
                    LinkedList<Object> list = new LinkedList<>();
                    list.add(colOrderedMap.get(index + 1).getData());
                    tableInfo.getDataList().add(new ColumnInfo(index + 1, list, fields.get(index).getFieldType()));
                }
            }
        }
    }
}
