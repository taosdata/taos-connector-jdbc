package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;

public abstract class MetaDropNormalTable extends Meta {
    private List<String> tableNameList;

    public List<String> getTableNameList() {
        return tableNameList;
    }

    public void setTableNameList(List<String> tableNameList) {
        this.tableNameList = tableNameList;
    }
}
