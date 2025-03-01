package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Column;
import com.taosdata.jdbc.common.ColumnInfo;
import com.taosdata.jdbc.common.SerializeBlock;
import com.taosdata.jdbc.common.TableInfo;
import com.taosdata.jdbc.enums.FeildBindType;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.stmt2.entity.Field;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class SerializeBatchData {
    private List<Map<Integer, Column>> batchList;
    private LinkedBlockingQueue<byte[]> blockingQueueOut;

    private final List<Field> fields;
    private final List<TableInfo> tableInfoList = new ArrayList<>();
    private TableInfo tableInfo = TableInfo.getEmptyTableInfo();

    private final long reqId;
    private final long stmtId;
    private final int toBeBindTableNameIndex;
    private final int toBeBindTagCount;
    private final int toBeBindColCount;
    private final int precision;
    private SQLException e = null;

    public SerializeBatchData(List<Map<Integer, Column>> batchList,
                              LinkedBlockingQueue<byte[]> blockingQueueOut,
                              List<Field> fields,
                              long reqId,
                              long stmtId,
                              int toBeBindTableNameIndex,
                              int toBebindTagCount,
                              int toBebindColCount,
                              int precision,
                              SQLException lastException) {

        this.batchList = batchList;
        this.blockingQueueOut = blockingQueueOut;
        this.fields = fields;
        this.reqId = reqId;
        this.stmtId = stmtId;
        this.toBeBindTableNameIndex = toBeBindTableNameIndex;
        this.toBeBindTagCount = toBebindTagCount;
        this.toBeBindColCount = toBebindColCount;
        this.precision = precision;
        this.e = lastException;
    }

    public void clac() {
        try {
            for (Map<Integer, Column> map : batchList) {
                processOneRow(map);
            }

            byte[] outData = SerializeBlock.getStmt2BindBlock(
                    reqId,
                    stmtId,
                    tableInfoList,
                    toBeBindTableNameIndex,
                    toBeBindTagCount,
                    toBeBindColCount,
                    precision);
            blockingQueueOut.add(outData);
        } catch (SQLException ex) {

            e = ex;
        }
    }

    public void processOneRow(Map<Integer, Column> colOrderedMap) {
        if (StringUtils.isEmpty(tableInfo.getTableName())
                && tableInfo.getTagInfo().isEmpty()
                && tableInfo.getDataList().isEmpty()) {
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

    public Exception getE() {
        return e;
    }

}
