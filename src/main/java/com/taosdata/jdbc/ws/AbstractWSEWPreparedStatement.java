package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.common.DataLengthCfg;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.utils.SyncObj;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.stmt2.entity.EWBackendThreadInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWRawBlock;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.PstmtConInfo;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractWSEWPreparedStatement extends AbsWSPreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(AbstractWSEWPreparedStatement.class);
    private static final ForkJoinPool SERIALIZE_EXECUTOR = Utils.getForkJoinPool();
    private static final long WRITE_QUEUE_SKEW_CHECK_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(5);

    private final boolean copyData;
    private final int writeThreadNum;
    private final ThreadPoolExecutor writerThreads;
    private final ArrayList<EWBackendThreadInfo> backendThreadInfoList;
    private final AtomicInteger remainingUnprocessedRows = new AtomicInteger(0);
    private final AtomicInteger batchInsertedRows = new AtomicInteger(0);
    private final AtomicInteger flushIn = new AtomicInteger(0);
    private final List<WorkerThread> workerThreadList;
    private final SyncObj syncObj = new SyncObj();
    private int addBatchCounts = 0;
    private long lastWriteQueueSkewCheckNanos = System.nanoTime();

    protected AbstractWSEWPreparedStatement(Transport transport,
                                            ConnectionParam param,
                                            String database,
                                            AbstractConnection connection,
                                            String sql,
                                            Long instanceId,
                                            Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp);
        this.copyData = param.isCopyData();
        this.writeThreadNum = param.getBackendWriteThreadNum();
        this.writerThreads = (ThreadPoolExecutor) Executors.newFixedThreadPool(writeThreadNum);
        this.backendThreadInfoList = new ArrayList<>(writeThreadNum);
        for (int i = 0; i < writeThreadNum; i++) {
            backendThreadInfoList.add(new EWBackendThreadInfo(
                    param.getCacheSizeByRow(),
                    param.getCacheSizeByRow() / param.getBatchSizeByRow()));
        }

        this.workerThreadList = new ArrayList<>(writeThreadNum);
        for (int i = 0; i < writeThreadNum; i++) {
            workerThreadList.add(new WorkerThread(
                    backendThreadInfoList.get(i),
                    new StmtInfo(prepareResp, sql),
                    new PstmtConInfo(transport, param, database, connection, instanceId),
                    closed,
                    remainingUnprocessedRows,
                    batchInsertedRows,
                    flushIn,
                    syncObj));
        }

        try {
            for (WorkerThread workerThread : workerThreadList) {
                workerThread.initStmt(param.getRetryTimes());
            }
        } catch (SQLException e) { //NOSONAR
            for (WorkerThread workerThread : workerThreadList) {
                workerThread.releaseStmt();
            }
            log.error("Failed to initialize prepared statement, sql: {}", sql, e);
            throw TSDBError.createSQLException(
                    TSDBErrorNumbers.ERROR_UNKNOWN,
                    "Failed to initialize prepared statement: " + e.getMessage());
        }

        for (WorkerThread workerThread : workerThreadList) {
            writerThreads.submit(workerThread);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }

        if (stmtInfo.isInsert()) {
            executeUpdate();
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support insert.");
        }

        return !stmtInfo.isInsert();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support insert.");
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }

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
            throw new SQLException(
                    "InsertedRows: " + totalRowsInserted + ", ErrorInfo: " + lastError.getMessage(),
                    "",
                    TSDBErrorNumbers.ERROR_FW_WRITE_ERROR);
        }

        return totalRowsInserted;
    }

    @Override
    public void addBatch() throws SQLException {
        if (colOrderedMap.size() == stmtInfo.getFields().size()) {
            Map<Integer, Column> map = copyMap(colOrderedMap);

            if (param.isStrictCheck()) {
                checkDataLength(map);
            }

            int hashCode;
            Object o = map.get(stmtInfo.getToBeBindTableNameIndex() + 1).getData();
            if (o instanceof String) {
                hashCode = o.hashCode();
            } else if (o instanceof byte[]) {
                hashCode = Arrays.hashCode((byte[]) o);
            } else {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "error type tbname.");
            }

            if (hashCode < 0) {
                hashCode = -hashCode;
            }

            EWBackendThreadInfo backendThreadInfo = backendThreadInfoList.get(hashCode % writeThreadNum);
            try {
                backendThreadInfo.getWriteQueue().put(map);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while adding batch", e);
            }

            warnIfWriteQueuesSkewed();
            triggerSerializeIfNeeded(backendThreadInfo);
            remainingUnprocessedRows.incrementAndGet();
            addBatchCounts++;
        } else {
            throw TSDBError.createSQLException(
                    TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                    "Only support standard jdbc bind api.");
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int[] ints = new int[addBatchCounts];
        for (int i = 0, len = ints.length; i < len; i++) {
            ints[i] = SUCCESS_NO_INFO;
        }
        addBatchCounts = 0;
        return ints;
    }

    @Override
    public void close() throws SQLException {
        waitWriteCompleted();
        if (isClosed()) {
            return;
        }

        super.close();

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

        for (WorkerThread workerThread : workerThreadList) {
            workerThread.releaseStmt();
        }
        for (EWBackendThreadInfo backendThreadInfo : backendThreadInfoList) {
            backendThreadInfo.releaseReusableColumnBuffers();
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw TSDBError.createSQLException(
                TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD,
                "Fast write mode only support insert.");
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

    protected abstract RecursiveAction newSerializationTask(EWBackendThreadInfo backendThreadInfo,
                                                            int batchSize,
                                                            boolean isProgressive);

    protected void dispatchSerializationTask(RecursiveAction task) {
        SERIALIZE_EXECUTOR.submit(task);
    }

    protected final void triggerSerializeProgressive(EWBackendThreadInfo backendThreadInfo) {
        if (!backendThreadInfo.getWriteQueue().isEmpty()
                && backendThreadInfo.getSerialQueue().remainingCapacity() > 0
                && backendThreadInfo.getSerializeRunning().compareAndSet(false, true)) {
            dispatchSerializationTask(newSerializationTask(
                    backendThreadInfo,
                    Math.min(backendThreadInfo.getWriteQueue().size(), param.getBatchSizeByRow()),
                    true));
        }
    }

    protected final void triggerSerializeIfNeeded(EWBackendThreadInfo backendThreadInfo) {
        if (backendThreadInfo.getWriteQueue().size() >= param.getBatchSizeByRow()
                && backendThreadInfo.getSerialQueue().remainingCapacity() > 0
                && backendThreadInfo.getSerializeRunning().compareAndSet(false, true)) {
            dispatchSerializationTask(newSerializationTask(
                    backendThreadInfo,
                    param.getBatchSizeByRow(),
                    false));
        }
    }

    private void waitWriteCompleted() {
        flushIn.incrementAndGet();
        while (remainingUnprocessedRows.get() != 0) {
            try {
                syncObj.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
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

    private void checkDataLength(Map<Integer, Column> map) throws SQLException {
        for (int i = 0; i < stmtInfo.getFields().size(); i++) {
            Field field = stmtInfo.getFields().get(i);
            Column column = map.get(i + 1);
            if (DataLengthCfg.getDataLength(column.getType()) == null
                    && field.getBindType() != FieldBindType.TAOS_FIELD_TBNAME.getValue()) {
                if (column.getData() instanceof byte[]) {
                    byte[] data = (byte[]) column.getData();
                    if (data.length > field.getBytes()) {
                        throw TSDBError.createSQLException(
                                TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                                "data length is too long, column index " + i);
                    }
                }
                if (column.getData() instanceof String) {
                    String data = (String) column.getData();
                    if (data.getBytes().length > field.getBytes()) {
                        throw TSDBError.createSQLException(
                                TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                                "data length is too long, column index " + i);
                    }
                }
            }
        }
    }

    private void warnIfWriteQueuesSkewed() {
        if (writeThreadNum <= 1) {
            return;
        }

        long now = System.nanoTime();
        if (now - lastWriteQueueSkewCheckNanos < WRITE_QUEUE_SKEW_CHECK_INTERVAL_NANOS) {
            return;
        }
        lastWriteQueueSkewCheckNanos = now;

        int batchSize = param.getBatchSizeByRow();
        QueueSkewStats stats = analyzeWriteQueueSkew(getWriteQueueSizes(), batchSize);
        if (!stats.skewed) {
            return;
        }

        log.warn("EW write queue distribution is skewed, stmt id: {}, max queue index: {}, max queue size: {}, other average queue size: {}, batch size: {}, queue sizes: {}.",
                stmtInfo.getStmtId(),
                stats.maxQueueIndex,
                stats.maxQueueSize,
                stats.otherAverageQueueSize,
                batchSize,
                Arrays.toString(stats.queueSizes));
    }

    private int[] getWriteQueueSizes() {
        int[] queueSizes = new int[backendThreadInfoList.size()];
        for (int i = 0; i < backendThreadInfoList.size(); i++) {
            queueSizes[i] = backendThreadInfoList.get(i).getWriteQueue().size();
        }
        return queueSizes;
    }

    static QueueSkewStats analyzeWriteQueueSkew(int[] queueSizes, int batchSize) {
        if (queueSizes == null || queueSizes.length <= 1 || batchSize <= 0) {
            return new QueueSkewStats(queueSizes, -1, 0, 0, false);
        }

        int maxQueueIndex = 0;
        int maxQueueSize = queueSizes[0];
        long totalQueueSize = queueSizes[0];
        for (int i = 1; i < queueSizes.length; i++) {
            int queueSize = queueSizes[i];
            totalQueueSize += queueSize;
            if (queueSize > maxQueueSize) {
                maxQueueSize = queueSize;
                maxQueueIndex = i;
            }
        }

        double otherAverageQueueSize = (totalQueueSize - maxQueueSize) / (double) (queueSizes.length - 1);
        boolean skewed = maxQueueSize >= batchSize
                && otherAverageQueueSize < batchSize
                && maxQueueSize > 3.0d * Math.max(otherAverageQueueSize, 1.0d);
        return new QueueSkewStats(queueSizes, maxQueueIndex, maxQueueSize, otherAverageQueueSize, skewed);
    }

    static final class QueueSkewStats {
        final int[] queueSizes;
        final int maxQueueIndex;
        final int maxQueueSize;
        final double otherAverageQueueSize;
        final boolean skewed;

        QueueSkewStats(int[] queueSizes,
                       int maxQueueIndex,
                       int maxQueueSize,
                       double otherAverageQueueSize,
                       boolean skewed) {
            this.queueSizes = queueSizes == null ? new int[0] : Arrays.copyOf(queueSizes, queueSizes.length);
            this.maxQueueIndex = maxQueueIndex;
            this.maxQueueSize = maxQueueSize;
            this.otherAverageQueueSize = otherAverageQueueSize;
            this.skewed = skewed;
        }
    }

    class WorkerThread extends WSRetryableStmt implements Runnable {
        private final Logger log = LoggerFactory.getLogger(WorkerThread.class);
        private final EWBackendThreadInfo backendThreadInfo;
        private final AtomicBoolean isClosed;
        private final AtomicInteger remainingUnprocessedRows;
        private final AtomicInteger flushIn;
        private final SyncObj syncObj;

        WorkerThread(EWBackendThreadInfo backendThreadInfo,
                     StmtInfo stmtInfo,
                     PstmtConInfo conInfo,
                     AtomicBoolean isClosed,
                     AtomicInteger remainingUnprocessedRows,
                     AtomicInteger batchInsertedRows,
                     AtomicInteger flushIn,
                     SyncObj syncObj) {
            super(conInfo.getConnection(),
                    conInfo.getParam(),
                    conInfo.getDatabase(),
                    conInfo.getTransport(),
                    conInfo.getInstanceId(),
                    stmtInfo,
                    batchInsertedRows);
            this.backendThreadInfo = backendThreadInfo;
            this.stmtInfo = stmtInfo;
            this.isClosed = isClosed;
            this.remainingUnprocessedRows = remainingUnprocessedRows;
            this.flushIn = flushIn;
            this.syncObj = syncObj;
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
                        triggerSerializeProgressive(backendThreadInfo);
                        continue;
                    }

                    rowCount = ewRawBlock.getRowCount();
                    lastError.set(ewRawBlock.getLastError());
                    if (lastError.get() == null) {
                        writeBlockWithRetry(ewRawBlock.getByteBuf());
                        triggerSerializeIfNeeded(backendThreadInfo);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (SQLException e) {
                    lastError.set(e);
                    log.error("Error in write data to server, stmt id: {}" +
                                    "rows: {}, code: {}, msg: {}",
                            stmtInfo.getStmtId(),
                            rowCount, e.getErrorCode(), e.getMessage());
                } catch (Exception e) {
                    log.error("unknown error:" ,e);
                }

                finally {
                    if (rowCount > 0) {
                        remainingUnprocessedRows.addAndGet(-rowCount);
                    }
                }
            }

            syncObj.signal();
        }

        public Exception getAndClearLastError() {
            Exception tmp = lastError.get();
            lastError.set(null);
            return tmp;
        }
    }
}
