package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ColumnInfo;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.common.SerializeBlock;
import com.taosdata.jdbc.common.TableInfo;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.stmt2.entity.EWBackendThreadInfo;
import com.taosdata.jdbc.ws.stmt2.entity.EWRawBlock;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;

// Efficient Writing Mode PreparedStatement
public class WSEWPreparedStatement extends AbstractWSEWPreparedStatement {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(WSEWPreparedStatement.class);

    public WSEWPreparedStatement(Transport transport,
                                 ConnectionParam param,
                                 String database,
                                 AbstractConnection connection,
                                 String sql,
                                 Long instanceId,
                                 Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp, false);
    }

    @Override
    protected RecursiveAction newSerializationTask(EWBackendThreadInfo backendThreadInfo,
                                                   int batchSize,
                                                   boolean isProgressive) {
        return new LegacyWSEWSerializationTask(backendThreadInfo, batchSize, stmtInfo, isProgressive);
    }

    static class LegacyWSEWSerializationTask extends RecursiveAction {
        final transient ArrayBlockingQueue<Map<Integer, Column>> writeQueue;
        final transient ArrayBlockingQueue<EWRawBlock> serialQueue;
        final int batchSize;

        private transient TableInfo tableInfo = TableInfo.getEmptyTableInfo();
        private final HashMap<ByteBuffer, TableInfo> tableInfoMap = new HashMap<>();
        private final transient StmtInfo stmtInfo;
        private final AtomicBoolean running;
        private final boolean isProgressive;

        public LegacyWSEWSerializationTask(EWBackendThreadInfo ewBackendThreadInfo,
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
                bindAllToTableInfo(stmtInfo.getFields(), colOrderedMap, tableInfo);
            } else {
                Object tbname = colOrderedMap.get(stmtInfo.getToBeBindTableNameIndex() + 1).getData();
                ByteBuffer tempTableName;
                if (tbname instanceof String) {
                    tempTableName = ByteBuffer.wrap(((String) tbname).getBytes(StandardCharsets.UTF_8));
                } else if (tbname instanceof byte[]) {
                    tempTableName = ByteBuffer.wrap((byte[]) tbname);
                } else {
                    throw TSDBError.createSQLException(
                            TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                            "table name must be string or binary");
                }

                if (tableInfo.getTableName().equals(tempTableName)) {
                    bindColToTableInfo(tableInfo, colOrderedMap);
                } else if (tableInfoMap.containsKey(tempTableName)) {
                    TableInfo tbInfo = tableInfoMap.get(tempTableName);
                    bindColToTableInfo(tbInfo, colOrderedMap);
                } else {
                    tableInfoMap.put(tableInfo.getTableName(), tableInfo);
                    tableInfo = TableInfo.getEmptyTableInfo();
                    bindAllToTableInfo(stmtInfo.getFields(), colOrderedMap, tableInfo);
                }
            }
        }

        private boolean isTableInfoEmpty() {
            return tableInfo.getTableName().capacity() == 0
                    && tableInfo.getTagInfo().isEmpty()
                    && tableInfo.getDataList().isEmpty();
        }

        private void bindColToTableInfo(TableInfo tableInfo, Map<Integer, Column> colOrderedMap) {
            for (ColumnInfo columnInfo : tableInfo.getDataList()) {
                columnInfo.add(colOrderedMap.get(columnInfo.getIndex()).getData());
            }
        }

        private void putEWRawBlock(EWRawBlock ewRawBlock) {
            try {
                serialQueue.put(ewRawBlock);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        protected void compute() {
            SQLException lastError = null;
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
                    long reqId = ReqId.getReqID();
                    ByteBuf rawBlock = SerializeBlock.getStmt2BindBlock(tableInfoMap, stmtInfo, reqId);
                    putEWRawBlock(new EWRawBlock(rawBlock, batchSize, lastError));
                    log.trace("buffer allocated: {}", Integer.toHexString(System.identityHashCode(rawBlock)));
                } catch (SQLException e) {
                    lastError = e;
                    putEWRawBlock(new EWRawBlock(null, batchSize, lastError));
                    log.error("Error in serialize data to block, stmt id: {}", stmtInfo.getStmtId(), e);
                    break;
                } catch (Exception e) {
                    lastError = new SQLException("Error in serialize data to block, stmt id: " + stmtInfo.getStmtId(), e);
                    putEWRawBlock(new EWRawBlock(null, batchSize, lastError));
                    log.error("Error in serialize data to block, stmt id: {}", stmtInfo.getStmtId(), e);
                    break;
                } finally {
                    tableInfo = TableInfo.getEmptyTableInfo();
                    tableInfoMap.clear();
                }
                if (isProgressive) {
                    break;
                }
            }
            running.set(false);
        }
    }
}
