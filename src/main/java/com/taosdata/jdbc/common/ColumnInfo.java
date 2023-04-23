package com.taosdata.jdbc.common;

import java.util.ArrayList;
import java.util.List;

public class ColumnInfo {
    private final ArrayList<Object> dataList = new ArrayList<>();
    // taos data type
    private final int type;
    private final int index;

    public ColumnInfo(int columnIndex, Object data, int type) {
        this.index = columnIndex;
        this.dataList.add(data);
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
