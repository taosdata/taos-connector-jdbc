package com.taosdata.jdbc.common;

import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    private  List<ColumnInfo> dataList;
    private  String tableName;
    private  List<ColumnInfo> tagInfo;

    public TableInfo(List<ColumnInfo> dataList, String tableName, List<ColumnInfo> tagInfo) {
        this.dataList = dataList;
        this.tableName = tableName;
        this.tagInfo = tagInfo;
    }

    public static TableInfo getEmptyTableInfo() {
        return new TableInfo(new ArrayList<>(), "", new ArrayList<>());
    }
    public List<ColumnInfo> getDataList() {
        return dataList;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnInfo> getTagInfo() {
        return tagInfo;
    }

    public void setDataList(List<ColumnInfo> dataList) {
        this.dataList = dataList;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTagInfo(List<ColumnInfo> tagInfo) {
        this.tagInfo = tagInfo;
    }


}
