package com.taosdata.jdbc.common;

import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    private List<Object> dataList = new ArrayList<>();

    private final String tableName = null;
    private final List<ColumnInfo> tagInfo = null;
    // taos data type
    private final int type;
    private final int index;

    public TableInfo(int columnIndex, Object data, int type) {
        this.index = columnIndex;
        this.dataList.add(data);
        this.type = type;
    }

    public TableInfo(int columnIndex, List<Object> dataList, int type, Integer flag) {
        this.index = columnIndex;
        this.dataList = dataList;
        this.type = type;
    }

    public void add(Object data) {
        this.dataList.add(data);
    }

    public List<Object> getDataList() {
        return dataList;
    }

    public int getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

}
